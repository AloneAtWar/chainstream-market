package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// Handler is what the engine drives. The engine checks continuity and calls
// Apply or Revert for each record that changes anything, then Commit for
// every record, so Commit must persist the checkpoint together with
// everything applied since the previous Commit.
type Handler interface {
	// Restore returns the last committed checkpoint and loads any state
	// that belongs to it.
	Restore(ctx context.Context, chainID uint64) (storage.Checkpoint, bool, error)
	Apply(ctx context.Context, block *streamv1.BlockEvent) error
	// Revert discards everything derived from blocks above rollbackTo.
	Revert(ctx context.Context, rollbackTo int64) error
	Commit(ctx context.Context, checkpoint storage.Checkpoint) error
}

// processorHandler runs a Processor and writes its output to a store.
type processorHandler struct {
	processor   Processor
	output      storage.Output
	checkpoints storage.CheckpointStore
	state       storage.StateStore
	stateful    Stateful
	logger      *slog.Logger
	chainID     uint64
}

func newProcessorHandler(options Options, logger *slog.Logger) (*processorHandler, error) {
	if options.Processor == nil || options.Output == nil || (options.Checkpoints == nil && options.State == nil) {
		return nil, errors.New("engine: a handler, or a processor, output and checkpoints or state, are required")
	}
	handler := &processorHandler{
		processor: options.Processor, output: options.Output, checkpoints: options.Checkpoints,
		state: options.State, logger: logger, chainID: options.ChainID,
	}
	if options.State != nil {
		var ok bool
		if handler.stateful, ok = options.Processor.(Stateful); !ok {
			return nil, fmt.Errorf("engine: processor %T cannot persist state", options.Processor)
		}
	}
	return handler, nil
}

func (handler *processorHandler) Restore(ctx context.Context, chainID uint64) (storage.Checkpoint, bool, error) {
	if handler.state == nil {
		return handler.checkpoints.Load(ctx, chainID)
	}
	state, ok, err := handler.state.LoadState(ctx, chainID)
	if err != nil || !ok {
		return storage.Checkpoint{}, false, err
	}
	if err := handler.stateful.Restore(state); err != nil {
		return storage.Checkpoint{}, false, fmt.Errorf("restore state: %w", err)
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
	return nil
}

func (handler *processorHandler) Revert(ctx context.Context, rollbackTo int64) error {
	if err := handler.processor.Revert(ctx, rollbackTo); err != nil {
		return fmt.Errorf("revert processor: %w", err)
	}
	if err := handler.output.Rewind(ctx, handler.chainID, rollbackTo); err != nil {
		return fmt.Errorf("rewind output: %w", err)
	}
	return nil
}

// Commit saves the checkpoint, with the processor's state changes when state
// is persisted.
func (handler *processorHandler) Commit(ctx context.Context, checkpoint storage.Checkpoint) error {
	if handler.state == nil {
		return handler.checkpoints.Save(ctx, checkpoint)
	}
	delta, err := handler.stateful.TakeDelta()
	if err != nil {
		return fmt.Errorf("take state delta: %w", err)
	}
	return handler.state.Commit(ctx, checkpoint, delta)
}
