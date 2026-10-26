package storage

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// RankingReader answers token list queries for the API.
type RankingReader interface {
	TokenList(ctx context.Context, query model.TokenListQuery) ([]model.RankedToken, error)
}
