package storage

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// RegistryStore persists token and pool metadata. CommitRegistry applies a
// change and saves the checkpoint atomically.
type RegistryStore interface {
	// LoadRegistryCheckpoint returns the last committed checkpoint and the
	// highest height whose undo history was pruned.
	LoadRegistryCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, int64, bool, error)
	CommitRegistry(ctx context.Context, checkpoint chainsync.Checkpoint, change model.RegistryChange) error
}

// RegistryReader answers token and pool queries for the API.
type RegistryReader interface {
	Token(ctx context.Context, chainID uint64, address string) (model.Token, bool, error)
	Pool(ctx context.Context, chainID uint64, poolID string) (model.Pool, bool, error)
	// TokenPools returns pools that trade token, most recently observed
	// first.
	TokenPools(ctx context.Context, chainID uint64, token string, limit int) ([]model.Pool, error)
}
