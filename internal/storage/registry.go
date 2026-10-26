package storage

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// RegistryReader answers token and pool queries for the API.
type RegistryReader interface {
	Token(ctx context.Context, chainID uint64, address string) (model.Token, bool, error)
	Pool(ctx context.Context, chainID uint64, poolID string) (model.Pool, bool, error)
	// TokenPools returns pools that trade token, most recently observed
	// first.
	TokenPools(ctx context.Context, chainID uint64, token string, limit int) ([]model.Pool, error)
}
