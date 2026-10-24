// Package storagetest checks storage backends against the behavior the
// engine relies on.
package storagetest

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

// StateStore runs a commit, revert and prune sequence against store, which
// must hold no state for chains 1 and 56.
func StateStore(t *testing.T, store storage.StateStore) {
	t.Helper()
	ctx := context.Background()
	if _, ok, err := store.LoadState(ctx, 1); err != nil || ok {
		t.Fatalf("LoadState() on empty store = %v, %v", ok, err)
	}
	commit := func(checkpoint storage.Checkpoint, delta storage.StateDelta) {
		t.Helper()
		if err := store.Commit(ctx, checkpoint, delta); err != nil {
			t.Fatalf("Commit(%+v) error = %v", checkpoint, err)
		}
	}
	load := func(chainID uint64) storage.State {
		t.Helper()
		state, ok, err := store.LoadState(ctx, chainID)
		if err != nil || !ok {
			t.Fatalf("LoadState(%d) = %v, %v", chainID, ok, err)
		}
		state.Checkpoint.UpdatedAt = state.Checkpoint.UpdatedAt.UTC()
		return state
	}
	entry := func(kind, key, value string) storage.StateEntry {
		return storage.StateEntry{Kind: kind, Key: key, Value: bytesOf(value)}
	}
	undo := func(height int64, kind, key, prev string) storage.StateUndo {
		return storage.StateUndo{Kind: kind, Height: height, Key: key, Prev: bytesOf(prev)}
	}

	commit(storage.Checkpoint{ChainID: 1, Offset: 0, Height: 100, Hash: "a100"}, storage.StateDelta{
		Entries: []storage.StateEntry{entry("pools", "p", `{"a":1}`), entry("prices", "t", `"1.5"`)},
		Undo:    []storage.StateUndo{undo(100, "pools", "p", ""), undo(100, "prices", "t", "")},
	})
	commit(storage.Checkpoint{ChainID: 56, Offset: 7, Height: 9, Hash: "bsc"}, storage.StateDelta{
		Entries: []storage.StateEntry{entry("pools", "p", `{"bsc":true}`)},
	})
	commit(storage.Checkpoint{ChainID: 1, Offset: 1, Height: 101, Hash: "a101"}, storage.StateDelta{
		Entries: []storage.StateEntry{entry("pools", "p", `{"a":2}`), entry("prices", "t", "")},
		Undo:    []storage.StateUndo{undo(101, "pools", "p", `{"a":1}`), undo(101, "prices", "t", `"1.5"`)},
	})
	state := load(1)
	if state.Checkpoint.Offset != 1 || state.Checkpoint.Height != 101 || state.Checkpoint.Hash != "a101" {
		t.Fatalf("checkpoint = %+v", state.Checkpoint)
	}
	assertEntries(t, state.Entries, []storage.StateEntry{entry("pools", "p", `{"a":2}`)})
	assertUndo(t, state.Undo, []storage.StateUndo{
		undo(100, "pools", "p", ""), undo(100, "prices", "t", ""),
		undo(101, "pools", "p", `{"a":1}`), undo(101, "prices", "t", `"1.5"`),
	})

	// A revert to 100 restores the values and drops the undo history above it.
	commit(storage.Checkpoint{ChainID: 1, Offset: 2, Height: 100}, storage.StateDelta{
		Reverted: true, RevertTo: 100,
		Entries: []storage.StateEntry{entry("pools", "p", `{"a":1}`), entry("prices", "t", `"1.5"`)},
	})
	state = load(1)
	assertEntries(t, state.Entries, []storage.StateEntry{entry("pools", "p", `{"a":1}`), entry("prices", "t", `"1.5"`)})
	assertUndo(t, state.Undo, []storage.StateUndo{undo(100, "pools", "p", ""), undo(100, "prices", "t", "")})

	commit(storage.Checkpoint{ChainID: 1, Offset: 3, Height: 101, Hash: "b101"}, storage.StateDelta{
		Undo:          []storage.StateUndo{undo(101, "pools", "p", `{"a":1}`)},
		Entries:       []storage.StateEntry{entry("pools", "p", `{"a":3}`)},
		PrunedThrough: 100,
	})
	state = load(1)
	if state.PrunedThrough != 100 || state.Checkpoint.Hash != "b101" {
		t.Fatalf("state = %+v", state)
	}
	assertUndo(t, state.Undo, []storage.StateUndo{undo(101, "pools", "p", `{"a":1}`)})

	other := load(56)
	if other.Checkpoint.Offset != 7 || len(other.Undo) != 0 {
		t.Fatalf("chain 56 state = %+v", other)
	}
	assertEntries(t, other.Entries, []storage.StateEntry{entry("pools", "p", `{"bsc":true}`)})
}

func bytesOf(value string) []byte {
	if value == "" {
		return nil
	}
	return []byte(value)
}

// assertEntries compares entries ignoring JSON formatting.
func assertEntries(t *testing.T, got, want []storage.StateEntry) {
	t.Helper()
	normalize := func(entries []storage.StateEntry) []storage.StateEntry {
		out := make([]storage.StateEntry, len(entries))
		for index, entry := range entries {
			entry.Value = compact(t, entry.Value)
			out[index] = entry
		}
		return out
	}
	if !reflect.DeepEqual(normalize(got), normalize(want)) {
		t.Fatalf("entries = %+v, want %+v", got, want)
	}
}

func assertUndo(t *testing.T, got, want []storage.StateUndo) {
	t.Helper()
	normalize := func(undo []storage.StateUndo) []storage.StateUndo {
		out := make([]storage.StateUndo, len(undo))
		for index, entry := range undo {
			entry.Prev = compact(t, entry.Prev)
			out[index] = entry
		}
		return out
	}
	if !reflect.DeepEqual(normalize(got), normalize(want)) {
		t.Fatalf("undo = %+v, want %+v", got, want)
	}
}

func compact(t *testing.T, raw []byte) []byte {
	t.Helper()
	if raw == nil {
		return nil
	}
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, raw); err != nil {
		t.Fatalf("invalid JSON %q: %v", raw, err)
	}
	return buffer.Bytes()
}
