package market

import "sort"

// journal is a map that records the previous value of every write by block
// height, so state can be rewound after a reorg. Values must not share
// mutable memory with later writes.
type journal[K comparable, V any] struct {
	values map[K]V
	undo   []undoEntry[K, V]
}

type undoEntry[K comparable, V any] struct {
	height  int64
	key     K
	prev    V
	existed bool
}

func newJournal[K comparable, V any]() *journal[K, V] {
	return &journal[K, V]{values: make(map[K]V)}
}

func (j *journal[K, V]) get(key K) (V, bool) {
	value, ok := j.values[key]
	return value, ok
}

func (j *journal[K, V]) set(height int64, key K, value V) {
	prev, existed := j.values[key]
	j.undo = append(j.undo, undoEntry[K, V]{height: height, key: key, prev: prev, existed: existed})
	j.values[key] = value
}

// revert undoes every write made above height.
func (j *journal[K, V]) revert(height int64) {
	index := len(j.undo) - 1
	for ; index >= 0 && j.undo[index].height > height; index-- {
		entry := j.undo[index]
		if entry.existed {
			j.values[entry.key] = entry.prev
		} else {
			delete(j.values, entry.key)
		}
	}
	clear(j.undo[index+1:])
	j.undo = j.undo[:index+1]
}

// prune forgets undo entries at or below height; those writes become
// permanent.
func (j *journal[K, V]) prune(height int64) {
	keep := sort.Search(len(j.undo), func(index int) bool { return j.undo[index].height > height })
	if keep == 0 {
		return
	}
	j.undo = append([]undoEntry[K, V](nil), j.undo[keep:]...)
}
