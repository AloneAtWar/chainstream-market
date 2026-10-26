package pgstore

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

// TokenList reads a page of the rankings.
func (store *Store) TokenList(ctx context.Context, query model.TokenListQuery) ([]model.RankedToken, error) {
	return pgdb.TokenList(ctx, store.db, query)
}

var _ storage.RankingReader = (*Store)(nil)
