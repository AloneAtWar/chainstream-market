package market

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

// journal is a map that records the previous value of every write by block
// height, so state can be rewound after a reorg. Values must not share
// mutable memory with later writes.
//
// A journal also tracks what changed since the last take, so the engine can
// persist each record's changes instead of the whole state.
type journal[K comparable, V any] struct {
	codec  keyCodec[K]
	values map[K]V
	undo   []undoEntry[K, V]
	// dirty holds keys written or reverted since the last take.
	dirty map[K]struct{}
	// pending is the undo history added since the last take.
	pending []undoEntry[K, V]
}

type undoEntry[K comparable, V any] struct {
	Height  int64
	Key     K
	Prev    V
	Existed bool
}

// keyCodec names a journal's kind in storage and converts its keys to and
// from strings.
type keyCodec[K comparable] struct {
	kind   string
	encode func(K) string
	decode func(string) (K, error)
}

func stringKeys(kind string) keyCodec[string] {
	return keyCodec[string]{
		kind:   kind,
		encode: func(key string) string { return key },
		decode: func(key string) (string, error) { return key, nil },
	}
}

func newJournal[K comparable, V any](codec keyCodec[K]) *journal[K, V] {
	return &journal[K, V]{codec: codec, values: make(map[K]V), dirty: make(map[K]struct{})}
}

func (j *journal[K, V]) get(key K) (V, bool) {
	value, ok := j.values[key]
	return value, ok
}

func (j *journal[K, V]) set(height int64, key K, value V) {
	prev, existed := j.values[key]
	entry := undoEntry[K, V]{Height: height, Key: key, Prev: prev, Existed: existed}
	j.undo = append(j.undo, entry)
	j.pending = append(j.pending, entry)
	j.values[key] = value
	j.dirty[key] = struct{}{}
}

// remove deletes key at height, recording its value so a revert restores it.
func (j *journal[K, V]) remove(height int64, key K) {
	prev, existed := j.values[key]
	if !existed {
		return
	}
	entry := undoEntry[K, V]{Height: height, Key: key, Prev: prev, Existed: true}
	j.undo = append(j.undo, entry)
	j.pending = append(j.pending, entry)
	delete(j.values, key)
	j.dirty[key] = struct{}{}
}

// revert undoes every write made above height.
func (j *journal[K, V]) revert(height int64) {
	index := len(j.undo) - 1
	for ; index >= 0 && j.undo[index].Height > height; index-- {
		entry := j.undo[index]
		if entry.Existed {
			j.values[entry.Key] = entry.Prev
		} else {
			delete(j.values, entry.Key)
		}
		j.dirty[entry.Key] = struct{}{}
	}
	clear(j.undo[index+1:])
	j.undo = j.undo[:index+1]
	keep := sort.Search(len(j.pending), func(index int) bool { return j.pending[index].Height > height })
	clear(j.pending[keep:])
	j.pending = j.pending[:keep]
}

// restored returns, for every key written above height, the undo entry that
// holds its value as of height.
func (j *journal[K, V]) restored(height int64) map[K]undoEntry[K, V] {
	restored := make(map[K]undoEntry[K, V])
	for index := len(j.undo) - 1; index >= 0 && j.undo[index].Height > height; index-- {
		restored[j.undo[index].Key] = j.undo[index]
	}
	return restored
}

// prune forgets undo entries at or below height; those writes become
// permanent.
func (j *journal[K, V]) prune(height int64) {
	keep := sort.Search(len(j.undo), func(index int) bool { return j.undo[index].Height > height })
	if keep == 0 {
		return
	}
	j.undo = append([]undoEntry[K, V](nil), j.undo[keep:]...)
}

// take returns the changes since the last take and starts a new change set.
// Only the first undo entry per key and height is kept: reverting to a block
// boundary restores the value from before that block's first write.
func (j *journal[K, V]) take(delta *storage.StateDelta) error {
	for key := range j.dirty {
		entry := storage.StateEntry{Kind: j.codec.kind, Key: j.codec.encode(key)}
		if value, ok := j.values[key]; ok {
			raw, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("market: encode %s %s: %w", j.codec.kind, entry.Key, err)
			}
			entry.Value = raw
		}
		delta.Entries = append(delta.Entries, entry)
	}
	type seenKey struct {
		height int64
		key    K
	}
	seen := make(map[seenKey]struct{}, len(j.pending))
	for _, pending := range j.pending {
		if _, ok := seen[seenKey{pending.Height, pending.Key}]; ok {
			continue
		}
		seen[seenKey{pending.Height, pending.Key}] = struct{}{}
		undo := storage.StateUndo{Kind: j.codec.kind, Height: pending.Height, Key: j.codec.encode(pending.Key)}
		if pending.Existed {
			raw, err := json.Marshal(pending.Prev)
			if err != nil {
				return fmt.Errorf("market: encode %s undo %s: %w", j.codec.kind, undo.Key, err)
			}
			undo.Prev = raw
		}
		delta.Undo = append(delta.Undo, undo)
	}
	clear(j.dirty)
	j.pending = nil
	return nil
}

// load sets a persisted value without recording undo history.
func (j *journal[K, V]) load(entry storage.StateEntry) error {
	key, err := j.codec.decode(entry.Key)
	if err != nil {
		return fmt.Errorf("market: decode %s key %q: %w", j.codec.kind, entry.Key, err)
	}
	var value V
	if err := json.Unmarshal(entry.Value, &value); err != nil {
		return fmt.Errorf("market: decode %s %s: %w", j.codec.kind, entry.Key, err)
	}
	j.values[key] = value
	return nil
}

// loadUndo appends persisted undo history. Entries must arrive in ascending
// height order.
func (j *journal[K, V]) loadUndo(undo storage.StateUndo) error {
	key, err := j.codec.decode(undo.Key)
	if err != nil {
		return fmt.Errorf("market: decode %s undo key %q: %w", j.codec.kind, undo.Key, err)
	}
	if last := len(j.undo) - 1; last >= 0 && j.undo[last].Height > undo.Height {
		return fmt.Errorf("market: %s undo at %d after %d", j.codec.kind, undo.Height, j.undo[last].Height)
	}
	entry := undoEntry[K, V]{Height: undo.Height, Key: key, Existed: undo.Prev != nil}
	if entry.Existed {
		if err := json.Unmarshal(undo.Prev, &entry.Prev); err != nil {
			return fmt.Errorf("market: decode %s undo %s: %w", j.codec.kind, undo.Key, err)
		}
	}
	j.undo = append(j.undo, entry)
	return nil
}
