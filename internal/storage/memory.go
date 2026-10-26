package storage

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
)

// Memory keeps checkpoints, processor state and outputs in process memory.
// It is the local and test backend; everything is lost on restart.
type Memory struct {
	mu          sync.RWMutex
	checkpoints map[uint64]chainsync.Checkpoint
	blocks      map[uint64]map[int64]BlockOutput
	states      map[uint64]*memoryState
}

func NewMemory() *Memory {
	return &Memory{
		checkpoints: make(map[uint64]chainsync.Checkpoint),
		blocks:      make(map[uint64]map[int64]BlockOutput),
		states:      make(map[uint64]*memoryState),
	}
}

func (memory *Memory) Load(_ context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	checkpoint, ok := memory.checkpoints[chainID]
	return checkpoint, ok, nil
}

func (memory *Memory) Save(_ context.Context, checkpoint chainsync.Checkpoint) error {
	if checkpoint.ChainID == 0 {
		return errors.New("memory storage: checkpoint chain id is required")
	}
	memory.mu.Lock()
	defer memory.mu.Unlock()
	memory.checkpoints[checkpoint.ChainID] = checkpoint
	return nil
}

func (memory *Memory) WriteBlock(_ context.Context, block BlockOutput) error {
	if block.ChainID == 0 {
		return errors.New("memory storage: block chain id is required")
	}
	memory.mu.Lock()
	defer memory.mu.Unlock()
	chain := memory.blocks[block.ChainID]
	if chain == nil {
		chain = make(map[int64]BlockOutput)
		memory.blocks[block.ChainID] = chain
	}
	chain[block.Height] = block
	return nil
}

func (memory *Memory) Rewind(_ context.Context, chainID uint64, rollbackTo int64) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	for height := range memory.blocks[chainID] {
		if height > rollbackTo {
			delete(memory.blocks[chainID], height)
		}
	}
	return nil
}

// Blocks returns the stored outputs for chainID in height order.
func (memory *Memory) Blocks(chainID uint64) []BlockOutput {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	blocks := make([]BlockOutput, 0, len(memory.blocks[chainID]))
	for _, block := range memory.blocks[chainID] {
		blocks = append(blocks, block)
	}
	sort.Slice(blocks, func(left, right int) bool { return blocks[left].Height < blocks[right].Height })
	return blocks
}

var (
	_ CheckpointStore = (*Memory)(nil)
	_ Output          = (*Memory)(nil)
	_ StateStore      = (*Memory)(nil)
)
