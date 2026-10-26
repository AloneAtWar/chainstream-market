package data

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/service"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"gorm.io/gorm"
)

// rankingReader reads market-rankings' token lists.
type rankingReader struct {
	db *gorm.DB
}

// NewRankingReader reads market-rankings' token lists.
func NewRankingReader(data *Data) service.RankingReader { return &rankingReader{db: data.state} }

// TokenList reads a page of the rankings.
func (reader *rankingReader) TokenList(ctx context.Context, query model.TokenListQuery) ([]model.RankedToken, error) {
	return pgdb.TokenList(ctx, reader.db, query)
}
