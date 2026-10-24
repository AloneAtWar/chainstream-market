package engine

import (
	"context"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// Processor turns blocks into market output. The engine calls it in height
// order, one chain per processor, and calls Revert before replacement blocks
// after a reorg.
type Processor interface {
	Apply(ctx context.Context, block *streamv1.BlockEvent) (storage.BlockOutput, error)
	// Revert discards in-memory state derived from blocks above rollbackTo.
	Revert(ctx context.Context, rollbackTo int64) error
}

// Stateful is a Processor whose state the engine persists through a
// storage.StateStore, so a restart resumes where it stopped.
type Stateful interface {
	Processor
	// Restore loads persisted state into a processor that has not applied
	// any block.
	Restore(state storage.State) error
	// TakeDelta returns the state changed since the last call.
	TakeDelta() (storage.StateDelta, error)
}

// HeaderProcessor emits only block headers. It stands in until orientation,
// pricing and candle aggregation are added.
type HeaderProcessor struct{}

func (HeaderProcessor) Apply(_ context.Context, block *streamv1.BlockEvent) (storage.BlockOutput, error) {
	return NewBlockOutput(block), nil
}

func (HeaderProcessor) Revert(context.Context, int64) error { return nil }

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

var _ Processor = HeaderProcessor{}
