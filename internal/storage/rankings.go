package storage

import (
	"context"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// RankingSource reads what the rankings are built from out of the
// market-engine output.
type RankingSource interface {
	// ChainHead returns the time of the highest stored block.
	ChainHead(ctx context.Context, chainID uint64) (time.Time, bool, error)
	// TokenActivity aggregates the current version of every token candle of
	// interval that opens at or after since.
	TokenActivity(ctx context.Context, chainID uint64, interval time.Duration, since time.Time) ([]model.TokenActivity, error)
	// LatestQuotes returns the latest price of every token priced at or
	// after since and of each of tokens. A zero since returns every token.
	LatestQuotes(ctx context.Context, chainID uint64, since time.Time, tokens []string) ([]model.TokenQuote, error)
}

// RankingStore keeps the token rankings. CommitRankings applies a refresh
// and recounts holders and creation heights from the other PostgreSQL
// tables in one transaction.
type RankingStore interface {
	// LoadRankingCursor returns the chain head time of the last refresh.
	LoadRankingCursor(ctx context.Context, chainID uint64) (time.Time, bool, error)
	// RankedSince returns ranked tokens whose price is at or after since.
	RankedSince(ctx context.Context, chainID uint64, since time.Time) ([]string, error)
	CommitRankings(ctx context.Context, refresh model.RankingRefresh) error
}

// RankingReader answers token list queries for the API.
type RankingReader interface {
	TokenList(ctx context.Context, query model.TokenListQuery) ([]model.RankedToken, error)
}
