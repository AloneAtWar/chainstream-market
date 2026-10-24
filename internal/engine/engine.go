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
	ChainID uint64
	Chain   string
	Open    SourceFactory
	// Handler receives the records. When it is nil, the engine builds one
	// from Processor, Output and Checkpoints or State.
	Handler   Handler
	Processor Processor
	Output    storage.Output
	// Checkpoints persists the checkpoint alone. It is used when State is
	// nil, and the processor then rebuilds its state by replaying the topic.
	Checkpoints storage.CheckpointStore
	// State persists the processor's state with each checkpoint. The
	// processor must implement Stateful.
	State storage.StateStore
	// Notifier, when set, receives each block's output and each revert after
	// they are stored. It is only used with Processor.
	Notifier Notifier
	Logger   *slog.Logger
	Now      func() time.Time
}

// Engine consumes one chain topic. Each record is applied and committed
// before the next, so a restart resumes at the first uncommitted record.
type Engine struct {
	chainID uint64
	open    SourceFactory
	handler Handler
	logger  *slog.Logger
	now     func() time.Time
}

func New(options Options) (*Engine, error) {
	if options.ChainID == 0 || options.Open == nil {
		return nil, errors.New("engine: chain id and source are required")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("chain", options.Chain, "chain_id", options.ChainID)
	handler := options.Handler
	if handler == nil {
		var err error
		if handler, err = newProcessorHandler(options, logger); err != nil {
			return nil, err
		}
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Engine{chainID: options.ChainID, open: options.Open, handler: handler, logger: logger, now: now}, nil
}

// Run consumes until ctx is done, returning nil, or a record fails to apply.
func (engine *Engine) Run(ctx context.Context) error {
	checkpoint, ok, err := engine.handler.Restore(ctx, engine.chainID)
	if err != nil {
		return fmt.Errorf("engine: restore: %w", err)
	}
	if !ok {
		checkpoint = storage.Checkpoint{ChainID: engine.chainID, Offset: -1}
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
	if err := engine.handler.Commit(ctx, next); err != nil {
		return checkpoint, fmt.Errorf("commit: %w", err)
	}
	return next, nil
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
	if err := engine.handler.Revert(ctx, rollbackTo); err != nil {
		return fmt.Errorf("revert to %d: %w", rollbackTo, err)
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
	if err := engine.handler.Apply(ctx, block); err != nil {
		return fmt.Errorf("apply block %d: %w", height, err)
	}
	next.Height, next.Hash = height, block.GetBlockHash()
	return nil
}

// applied reports whether the checkpoint has applied at least one block.
func applied(checkpoint storage.Checkpoint) bool {
	return checkpoint.Height > 0 || checkpoint.Hash != ""
}
