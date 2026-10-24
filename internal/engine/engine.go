// Package engine applies one chain topic, in order, to market storage.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrChainMismatch means a record belongs to a different chain than the
	// engine was configured for, usually a misconfigured topic.
	ErrChainMismatch = errors.New("engine: record chain does not match")
	// ErrHeightGap means a block skipped heights without a revert.
	ErrHeightGap = errors.New("engine: block height gap")
	// ErrParentMismatch means a block does not extend the last applied block
	// and no revert preceded it.
	ErrParentMismatch = errors.New("engine: block parent does not match")
)

// SourceFactory opens the chain topic at a start offset.
type SourceFactory func(start int64) (source.Source, error)

type Options struct {
	ChainID   uint64
	Chain     string
	Open      SourceFactory
	Processor Processor
	Output    storage.Output
	// Checkpoints persists the checkpoint alone. It is used when State is
	// nil, and the processor then rebuilds its state by replaying the topic.
	Checkpoints storage.CheckpointStore
	// State persists the processor's state with each checkpoint. The
	// processor must implement Stateful.
	State  storage.StateStore
	Logger *slog.Logger
	Now    func() time.Time
}

// Engine consumes one chain topic. Each record is applied and checkpointed
// before the next, so a restart resumes at the first unapplied record.
type Engine struct {
	chainID     uint64
	open        SourceFactory
	processor   Processor
	output      storage.Output
	checkpoints storage.CheckpointStore
	state       storage.StateStore
	stateful    Stateful
	logger      *slog.Logger
	now         func() time.Time
}

func New(options Options) (*Engine, error) {
	if options.ChainID == 0 || options.Open == nil || options.Processor == nil || options.Output == nil ||
		(options.Checkpoints == nil && options.State == nil) {
		return nil, errors.New("engine: chain id, source, processor, output and checkpoints or state are required")
	}
	var stateful Stateful
	if options.State != nil {
		var ok bool
		if stateful, ok = options.Processor.(Stateful); !ok {
			return nil, fmt.Errorf("engine: processor %T cannot persist state", options.Processor)
		}
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Engine{
		chainID: options.ChainID, open: options.Open, processor: options.Processor,
		output: options.Output, checkpoints: options.Checkpoints, state: options.State, stateful: stateful,
		logger: logger.With("chain", options.Chain, "chain_id", options.ChainID), now: now,
	}, nil
}

// Run consumes until ctx is done, returning nil, or a record fails to apply.
func (engine *Engine) Run(ctx context.Context) error {
	checkpoint, err := engine.load(ctx)
	if err != nil {
		return err
	}
	stream, err := engine.open(checkpoint.Offset + 1)
	if err != nil {
		return fmt.Errorf("engine: open source: %w", err)
	}
	defer stream.Close()
	engine.logger.Info("engine started", "offset", checkpoint.Offset+1, "height", checkpoint.Height)

	for {
		messages, fetchErr := stream.Fetch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if fetchErr != nil {
			return fmt.Errorf("engine: fetch: %w", fetchErr)
		}
		for _, message := range messages {
			checkpoint, err = engine.handle(ctx, checkpoint, message)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("engine: offset %d: %w", message.Offset, err)
			}
		}
	}
}

func (engine *Engine) handle(ctx context.Context, checkpoint storage.Checkpoint, message source.Message) (storage.Checkpoint, error) {
	if message.Offset <= checkpoint.Offset {
		return checkpoint, nil
	}
	var record streamv1.StreamRecord
	if err := proto.Unmarshal(message.Value, &record); err != nil {
		return checkpoint, fmt.Errorf("decode record: %w", err)
	}
	next := checkpoint
	switch payload := record.GetPayload().(type) {
	case *streamv1.StreamRecord_Revert:
		if err := engine.revert(ctx, checkpoint, payload.Revert, &next); err != nil {
			return checkpoint, err
		}
	case *streamv1.StreamRecord_Block:
		if err := engine.apply(ctx, checkpoint, payload.Block, &next); err != nil {
			return checkpoint, err
		}
	default:
		return checkpoint, fmt.Errorf("unknown record payload %T", payload)
	}
	next.Offset = message.Offset
	next.UpdatedAt = engine.now()
	if err := engine.save(ctx, next); err != nil {
		return checkpoint, err
	}
	return next, nil
}

// load returns the checkpoint to resume from, restoring processor state
// when it is persisted.
func (engine *Engine) load(ctx context.Context) (storage.Checkpoint, error) {
	start := storage.Checkpoint{ChainID: engine.chainID, Offset: -1}
	if engine.state == nil {
		checkpoint, ok, err := engine.checkpoints.Load(ctx, engine.chainID)
		if err != nil {
			return start, fmt.Errorf("engine: load checkpoint: %w", err)
		}
		if !ok {
			return start, nil
		}
		return checkpoint, nil
	}
	state, ok, err := engine.state.LoadState(ctx, engine.chainID)
	if err != nil {
		return start, fmt.Errorf("engine: load state: %w", err)
	}
	if !ok {
		return start, nil
	}
	if err := engine.stateful.Restore(state); err != nil {
		return start, fmt.Errorf("engine: restore state: %w", err)
	}
	engine.logger.Info("state restored", "entries", len(state.Entries), "undo", len(state.Undo))
	return state.Checkpoint, nil
}

// save persists the checkpoint, with the processor's state changes when
// state is persisted.
func (engine *Engine) save(ctx context.Context, checkpoint storage.Checkpoint) error {
	if engine.state == nil {
		if err := engine.checkpoints.Save(ctx, checkpoint); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
		return nil
	}
	delta, err := engine.stateful.TakeDelta()
	if err != nil {
		return fmt.Errorf("take state delta: %w", err)
	}
	if err := engine.state.Commit(ctx, checkpoint, delta); err != nil {
		return fmt.Errorf("commit state: %w", err)
	}
	return nil
}

func (engine *Engine) revert(ctx context.Context, checkpoint storage.Checkpoint, revert *streamv1.BlockRevert, next *storage.Checkpoint) error {
	if revert.GetChainId() != engine.chainID {
		return fmt.Errorf("%w: revert chain %d", ErrChainMismatch, revert.GetChainId())
	}
	rollbackTo := revert.GetRollbackTo()
	// Reverts may be redelivered; one at or above the applied height has
	// nothing left to undo.
	if !applied(checkpoint) || rollbackTo >= checkpoint.Height {
		return nil
	}
	if err := engine.processor.Revert(ctx, rollbackTo); err != nil {
		return fmt.Errorf("revert processor to %d: %w", rollbackTo, err)
	}
	if err := engine.output.Rewind(ctx, engine.chainID, rollbackTo); err != nil {
		return fmt.Errorf("rewind output to %d: %w", rollbackTo, err)
	}
	engine.logger.Warn("reverted", "from", checkpoint.Height, "to", rollbackTo)
	next.Height, next.Hash = rollbackTo, ""
	return nil
}

func (engine *Engine) apply(ctx context.Context, checkpoint storage.Checkpoint, block *streamv1.BlockEvent, next *storage.Checkpoint) error {
	if block.GetChainId() != engine.chainID {
		return fmt.Errorf("%w: block chain %d", ErrChainMismatch, block.GetChainId())
	}
	height := block.GetBlockHeight()
	if applied(checkpoint) {
		// A retried publish can redeliver blocks that were already applied.
		if height <= checkpoint.Height {
			engine.logger.Debug("skipping redelivered block", "height", height, "applied", checkpoint.Height)
			return nil
		}
		if height != checkpoint.Height+1 {
			return fmt.Errorf("%w: got %d after %d", ErrHeightGap, height, checkpoint.Height)
		}
		if checkpoint.Hash != "" && block.GetParentHash() != checkpoint.Hash {
			return fmt.Errorf("%w: block %d parent %s, applied %s", ErrParentMismatch, height, block.GetParentHash(), checkpoint.Hash)
		}
	}
	output, err := engine.processor.Apply(ctx, block)
	if err != nil {
		return fmt.Errorf("process block %d: %w", height, err)
	}
	if err := engine.output.WriteBlock(ctx, output); err != nil {
		return fmt.Errorf("write block %d: %w", height, err)
	}
	next.Height, next.Hash = height, block.GetBlockHash()
	return nil
}

// applied reports whether the checkpoint has applied at least one block.
func applied(checkpoint storage.Checkpoint) bool {
	return checkpoint.Height > 0 || checkpoint.Hash != ""
}
