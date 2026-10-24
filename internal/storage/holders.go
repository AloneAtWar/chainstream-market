package storage

import (
	"context"
	"math/big"
)

// HolderKey is one holder of one token. Both addresses are lowercase.
type HolderKey struct {
	Token  string
	Holder string
}

// Holding is a holder's raw token balance.
type Holding struct {
	HolderKey
	Balance *big.Int
}

// HolderSeed creates a holding first seen in a block. Verified means Balance
// came from balanceOf rather than from summing transfers.
type HolderSeed struct {
	Holding
	Verified bool
}

// HolderDelta adds Delta to an existing holding. Verified marks the result
// as checked against balanceOf at the change's height.
type HolderDelta struct {
	HolderKey
	Delta    *big.Int
	Verified bool
}

// HolderChange is what one record changed in the holder balances.
type HolderChange struct {
	// Reverted undoes every change above RevertTo before anything else.
	Reverted bool
	RevertTo int64
	// Height is the block that Created and Deltas belong to.
	Height  int64
	Created []HolderSeed
	Deltas  []HolderDelta
	// PrunedThrough discards undo history at or below it.
	PrunedThrough int64
}

// HolderStore persists holder balances. CommitHolders applies a change and
// saves the checkpoint atomically.
type HolderStore interface {
	// LoadHolderCheckpoint returns the last committed checkpoint and the
	// highest height whose undo history was pruned.
	LoadHolderCheckpoint(ctx context.Context, chainID uint64) (Checkpoint, int64, bool, error)
	// Holdings returns the stored holdings among keys.
	Holdings(ctx context.Context, chainID uint64, keys []HolderKey) ([]Holding, error)
	// StaleHoldings returns up to limit holdings, least recently verified
	// first.
	StaleHoldings(ctx context.Context, chainID uint64, limit int) ([]Holding, error)
	CommitHolders(ctx context.Context, checkpoint Checkpoint, change HolderChange) error
}

// HolderReader answers holder queries for the API.
type HolderReader interface {
	// TopHolders returns the largest positive holdings of token.
	TopHolders(ctx context.Context, chainID uint64, token string, limit int) ([]Holding, error)
	// HolderCount counts holders of token with a positive balance.
	HolderCount(ctx context.Context, chainID uint64, token string) (int64, error)
}
