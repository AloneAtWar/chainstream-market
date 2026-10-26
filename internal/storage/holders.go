package storage

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// HolderStore persists holder balances. CommitHolders applies a change and
// saves the checkpoint atomically.
type HolderStore interface {
	// LoadHolderCheckpoint returns the last committed checkpoint and the
	// highest height whose undo history was pruned.
	LoadHolderCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, int64, bool, error)
	// Holdings returns the stored holdings among keys.
	Holdings(ctx context.Context, chainID uint64, keys []model.HolderKey) ([]model.Holding, error)
	// StaleHoldings returns up to limit holdings, least recently verified
	// first.
	StaleHoldings(ctx context.Context, chainID uint64, limit int) ([]model.Holding, error)
	CommitHolders(ctx context.Context, checkpoint chainsync.Checkpoint, change model.HolderChange) error
}

// HolderReader answers holder queries for the API.
type HolderReader interface {
	// TopHolders returns the largest positive holdings of token.
	TopHolders(ctx context.Context, chainID uint64, token string, limit int) ([]model.Holding, error)
	// HolderCount returns the number of holders of token with a positive
	// balance, kept by CommitHolders rather than counted on each call.
	HolderCount(ctx context.Context, chainID uint64, token string) (int64, error)
}
