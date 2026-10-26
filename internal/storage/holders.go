package storage

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// HolderReader answers holder queries for the API.
type HolderReader interface {
	// TopHolders returns the largest positive holdings of token.
	TopHolders(ctx context.Context, chainID uint64, token string, limit int) ([]model.Holding, error)
	// HolderCount returns the number of holders of token with a positive
	// balance, kept by CommitHolders rather than counted on each call.
	HolderCount(ctx context.Context, chainID uint64, token string) (int64, error)
}
