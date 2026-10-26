package market

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// BlockProcessor turns blocks into market output. The handler calls it in
// height order, one chain per processor, and calls Revert before replacement
// blocks after a reorg.
type BlockProcessor interface {
	Apply(ctx context.Context, block *streamv1.BlockEvent) (storage.BlockOutput, error)
	// Revert discards in-memory state derived from blocks above rollbackTo.
	Revert(ctx context.Context, rollbackTo int64) error
}

// Stateful is a BlockProcessor whose state the handler persists through a
// storage.StateStore, so a restart resumes where it stopped.
type Stateful interface {
	BlockProcessor
	// Restore loads persisted state into a processor that has not applied
	// any block.
	Restore(state storage.State) error
	// TakeDelta returns the state changed since the last call.
	TakeDelta() (storage.StateDelta, error)
}

// CandleReverter is a BlockProcessor that can say which candle series a revert
// will change, so live clients can be corrected.
type CandleReverter interface {
	BlockProcessor
	// RevertedCandles is called before Revert with the same height.
	RevertedCandles(rollbackTo int64) []storage.CandleReset
}

// Notifier pushes what each record changed to live clients. It is called
// after the output store has the change and before the checkpoint is
// committed, so a failure stops the engine and the record is applied and
// notified again after a restart. Notifications are therefore at least once.
type Notifier interface {
	NotifyBlock(ctx context.Context, output storage.BlockOutput, finalizedHeight int64) error
	NotifyRevert(ctx context.Context, revert storage.Revert) error
}

// NewBlockOutput returns an empty output carrying block's header.
func NewBlockOutput(block *streamv1.BlockEvent) storage.BlockOutput {
	finalized := block.GetFinalizedHeight()
	return storage.BlockOutput{
		ChainID:   block.GetChainId(),
		Height:    block.GetBlockHeight(),
		Hash:      block.GetBlockHash(),
		BlockTime: time.Unix(block.GetBlockTime(), 0).UTC(),
		Finalized: finalized > 0 && block.GetBlockHeight() <= finalized,
	}
}

// HandlerOptions configure the chainsync.Handler that runs a BlockProcessor.
type HandlerOptions struct {
	ChainID   uint64
	Processor BlockProcessor
	Output    storage.Output
	// Checkpoints persists the checkpoint alone. It is used when State is
	// nil, and the processor then rebuilds its state by replaying the topic.
	Checkpoints storage.CheckpointStore
	// State persists the processor's state with each checkpoint. The
	// processor must implement Stateful.
	State storage.StateStore
	// Notifier, when set, receives each block's output and each revert after
	// they are stored.
	Notifier Notifier
	Logger   *slog.Logger
}

// processorHandler runs a BlockProcessor and writes its output to a store.
type processorHandler struct {
	processor   BlockProcessor
	output      storage.Output
	checkpoints storage.CheckpointStore
	state       storage.StateStore
	stateful    Stateful
	notifier    Notifier
	logger      *slog.Logger
	chainID     uint64
}

// NewHandler returns the chainsync.Handler that runs options.Processor and
// writes its output.
func NewHandler(options HandlerOptions) (chainsync.Handler, error) {
	if options.Processor == nil || options.Output == nil || (options.Checkpoints == nil && options.State == nil) {
		return nil, errors.New("engine: a processor, output and checkpoints or state are required")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	handler := &processorHandler{
		processor: options.Processor, output: options.Output, checkpoints: options.Checkpoints,
		state: options.State, notifier: options.Notifier, logger: logger, chainID: options.ChainID,
	}
	if options.State != nil {
		var ok bool
		if handler.stateful, ok = options.Processor.(Stateful); !ok {
			return nil, fmt.Errorf("engine: processor %T cannot persist state", options.Processor)
		}
	}
	return handler, nil
}

func (handler *processorHandler) Restore(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	if handler.state == nil {
		return handler.checkpoints.Load(ctx, chainID)
	}
	state, ok, err := handler.state.LoadState(ctx, chainID)
	if err != nil || !ok {
		return chainsync.Checkpoint{}, false, err
	}
	if err := handler.stateful.Restore(state); err != nil {
		return chainsync.Checkpoint{}, false, fmt.Errorf("restore state: %w", err)
	}
	handler.logger.Info("state restored", "entries", len(state.Entries), "undo", len(state.Undo))
	return state.Checkpoint, true, nil
}

func (handler *processorHandler) Apply(ctx context.Context, block *streamv1.BlockEvent) error {
	output, err := handler.processor.Apply(ctx, block)
	if err != nil {
		return fmt.Errorf("process: %w", err)
	}
	if err := handler.output.WriteBlock(ctx, output); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if handler.notifier != nil {
		if err := handler.notifier.NotifyBlock(ctx, output, block.GetFinalizedHeight()); err != nil {
			return fmt.Errorf("notify block: %w", err)
		}
	}
	return nil
}

func (handler *processorHandler) Revert(ctx context.Context, rollbackTo int64) error {
	revert := storage.Revert{ChainID: handler.chainID, RollbackTo: rollbackTo}
	if reverter, ok := handler.processor.(CandleReverter); ok && handler.notifier != nil {
		revert.Candles = reverter.RevertedCandles(rollbackTo)
	}
	if err := handler.processor.Revert(ctx, rollbackTo); err != nil {
		return fmt.Errorf("revert processor: %w", err)
	}
	if err := handler.output.Rewind(ctx, handler.chainID, rollbackTo); err != nil {
		return fmt.Errorf("rewind output: %w", err)
	}
	if handler.notifier != nil {
		if err := handler.notifier.NotifyRevert(ctx, revert); err != nil {
			return fmt.Errorf("notify revert: %w", err)
		}
	}
	return nil
}

// Commit saves the checkpoint, with the processor's state changes when state
// is persisted.
func (handler *processorHandler) Commit(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	if handler.state == nil {
		return handler.checkpoints.Save(ctx, checkpoint)
	}
	delta, err := handler.stateful.TakeDelta()
	if err != nil {
		return fmt.Errorf("take state delta: %w", err)
	}
	return handler.state.Commit(ctx, checkpoint, delta)
}
