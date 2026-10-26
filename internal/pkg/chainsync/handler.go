package chainsync

import (
	"context"
	"time"

	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// Handler is what the engine drives. The engine checks continuity and calls
// Apply or Revert for each record that changes anything, then Commit for
// every record, so Commit must persist the checkpoint together with
// everything applied since the previous Commit.
type Handler interface {
	// Restore returns the last committed checkpoint and loads any state
	// that belongs to it.
	Restore(ctx context.Context, chainID uint64) (Checkpoint, bool, error)
	Apply(ctx context.Context, block *streamv1.BlockEvent) error
	// Revert discards everything derived from blocks above rollbackTo.
	Revert(ctx context.Context, rollbackTo int64) error
	Commit(ctx context.Context, checkpoint Checkpoint) error
}

// Checkpoint is the last chain-topic record the engine fully applied.
type Checkpoint struct {
	ChainID uint64
	// Offset is the Kafka offset of the last applied record; consumption
	// resumes at Offset+1.
	Offset int64
	// Height and Hash identify the last applied block. Hash is empty right
	// after a revert, when the block at Height is no longer known.
	Height    int64
	Hash      string
	UpdatedAt time.Time
}
