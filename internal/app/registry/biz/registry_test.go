package biz

import (
	"context"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

type nopRepo struct{}

func (nopRepo) LoadRegistryCheckpoint(context.Context, uint64) (chainsync.Checkpoint, int64, bool, error) {
	return chainsync.Checkpoint{}, 0, false, nil
}

func (nopRepo) CommitRegistry(context.Context, chainsync.Checkpoint, model.RegistryChange) error {
	return nil
}

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(Options{Repo: nil, ChainID: 1, HistoryDepth: 1}); err == nil {
		t.Fatal("New() without repo error = nil")
	}
	if _, err := New(Options{Repo: nopRepo{}, ChainID: 1}); err == nil {
		t.Fatal("New() without history depth error = nil")
	}
}
