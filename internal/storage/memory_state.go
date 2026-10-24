package storage

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"slices"
)

type memoryState struct {
	prunedThrough int64
	entries       map[stateID][]byte
	undo          map[undoID][]byte
}

type stateID struct{ kind, key string }

type undoID struct {
	kind   string
	height int64
	key    string
}

func (memory *Memory) LoadState(_ context.Context, chainID uint64) (State, bool, error) {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	checkpoint, ok := memory.checkpoints[chainID]
	if !ok {
		return State{}, false, nil
	}
	state := State{Checkpoint: checkpoint}
	stored := memory.states[chainID]
	if stored == nil {
		return state, true, nil
	}
	state.PrunedThrough = stored.prunedThrough
	for id, value := range stored.entries {
		state.Entries = append(state.Entries, StateEntry{Kind: id.kind, Key: id.key, Value: bytes.Clone(value)})
	}
	slices.SortFunc(state.Entries, func(left, right StateEntry) int {
		return cmp.Or(cmp.Compare(left.Kind, right.Kind), cmp.Compare(left.Key, right.Key))
	})
	for id, prev := range stored.undo {
		state.Undo = append(state.Undo, StateUndo{Kind: id.kind, Height: id.height, Key: id.key, Prev: bytes.Clone(prev)})
	}
	slices.SortFunc(state.Undo, func(left, right StateUndo) int {
		return cmp.Or(cmp.Compare(left.Height, right.Height), cmp.Compare(left.Kind, right.Kind), cmp.Compare(left.Key, right.Key))
	})
	return state, true, nil
}

func (memory *Memory) Commit(_ context.Context, checkpoint Checkpoint, delta StateDelta) error {
	if checkpoint.ChainID == 0 {
		return errors.New("memory storage: checkpoint chain id is required")
	}
	memory.mu.Lock()
	defer memory.mu.Unlock()
	stored := memory.states[checkpoint.ChainID]
	if stored == nil {
		stored = &memoryState{entries: make(map[stateID][]byte), undo: make(map[undoID][]byte)}
		memory.states[checkpoint.ChainID] = stored
	}
	if delta.Reverted {
		for id := range stored.undo {
			if id.height > delta.RevertTo {
				delete(stored.undo, id)
			}
		}
	}
	for _, undo := range delta.Undo {
		stored.undo[undoID{kind: undo.Kind, height: undo.Height, key: undo.Key}] = bytes.Clone(undo.Prev)
	}
	for id := range stored.undo {
		if id.height <= delta.PrunedThrough {
			delete(stored.undo, id)
		}
	}
	for _, entry := range delta.Entries {
		id := stateID{kind: entry.Kind, key: entry.Key}
		if entry.Value == nil {
			delete(stored.entries, id)
		} else {
			stored.entries[id] = bytes.Clone(entry.Value)
		}
	}
	stored.prunedThrough = max(stored.prunedThrough, delta.PrunedThrough)
	memory.checkpoints[checkpoint.ChainID] = checkpoint
	return nil
}
