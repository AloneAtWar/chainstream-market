package storage

import (
	"context"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

func TestMemoryWriteBlockReplacesHeight(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	for _, block := range []model.BlockOutput{
		{ChainID: 1, Height: 10, Hash: "a"},
		{ChainID: 1, Height: 11, Hash: "b"},
		{ChainID: 1, Height: 11, Hash: "b2"},
		{ChainID: 56, Height: 11, Hash: "c"},
	} {
		if err := memory.WriteBlock(ctx, block); err != nil {
			t.Fatalf("WriteBlock() error = %v", err)
		}
	}
	blocks := memory.Blocks(1)
	if len(blocks) != 2 || blocks[0].Hash != "a" || blocks[1].Hash != "b2" {
		t.Fatalf("Blocks(1) = %+v", blocks)
	}
}

func TestMemoryRewindOnlyTouchesChainAboveHeight(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	for _, block := range []model.BlockOutput{
		{ChainID: 1, Height: 10}, {ChainID: 1, Height: 11}, {ChainID: 1, Height: 12}, {ChainID: 56, Height: 12},
	} {
		if err := memory.WriteBlock(ctx, block); err != nil {
			t.Fatalf("WriteBlock() error = %v", err)
		}
	}
	if err := memory.Rewind(ctx, 1, 10); err != nil {
		t.Fatalf("Rewind() error = %v", err)
	}
	if got := memory.Blocks(1); len(got) != 1 || got[0].Height != 10 {
		t.Fatalf("Blocks(1) = %+v", got)
	}
	if got := memory.Blocks(56); len(got) != 1 {
		t.Fatalf("Blocks(56) = %+v", got)
	}
}

func TestMemoryCheckpoints(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	if _, ok, err := memory.Load(ctx, 1); err != nil || ok {
		t.Fatalf("Load() = %v, %v", ok, err)
	}
	if err := memory.Save(ctx, chainsync.Checkpoint{}); err == nil {
		t.Fatal("Save() without chain id error = nil")
	}
	want := chainsync.Checkpoint{ChainID: 1, Offset: 7, Height: 100, Hash: "h"}
	if err := memory.Save(ctx, want); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	got, ok, err := memory.Load(ctx, 1)
	if err != nil || !ok || got != want {
		t.Fatalf("Load() = %+v, %v, %v", got, ok, err)
	}
}
