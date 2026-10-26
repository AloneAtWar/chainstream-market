package storage

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
)

// State is a processor's persisted state and the checkpoint it belongs to.
// The engine restores it into a fresh processor on start, so a restart
// resumes at Checkpoint.Offset+1 without replaying the topic.
type State struct {
	Checkpoint chainsync.Checkpoint
	// PrunedThrough is the highest height whose undo history was discarded.
	PrunedThrough int64
	Entries       []StateEntry
	// Undo is the retained reorg history in ascending height order.
	Undo []StateUndo
}

// StateEntry is one key of processor state. Kind names the map the key
// belongs to, such as pools or prices. Value is opaque to storage; a nil
// Value in a StateDelta deletes the key.
type StateEntry struct {
	Kind  string
	Key   string
	Value []byte
}

// StateUndo is the value a key held before the first write at Height. A nil
// Prev means the key did not exist.
type StateUndo struct {
	Kind   string
	Height int64
	Key    string
	Prev   []byte
}

// StateDelta is what one record changed in a processor's state.
type StateDelta struct {
	// Reverted reports a reorg: undo history above RevertTo is discarded
	// before Undo is added.
	Reverted bool
	RevertTo int64
	Entries  []StateEntry
	Undo     []StateUndo
	// PrunedThrough discards undo history at or below it.
	PrunedThrough int64
}

// StateStore persists processor state. Commit applies a delta and saves the
// checkpoint atomically, so state never runs ahead of or behind the record
// it was derived from.
type StateStore interface {
	LoadState(ctx context.Context, chainID uint64) (State, bool, error)
	Commit(ctx context.Context, checkpoint chainsync.Checkpoint, delta StateDelta) error
}
