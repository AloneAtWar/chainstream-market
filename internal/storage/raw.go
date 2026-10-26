package storage

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// History reads what earlier blocks stored: ch-loader's raw trades and
// market-engine's own token prices. market-engine uses it to recompute the
// trades of a token whose decimals arrived late, beyond the ones it held.
type History interface {
	// RawTrades returns, in chain order, the trades at heights from..through
	// with a leg in one of tokens. ok is false when the raw store does not
	// hold every height through yet.
	RawTrades(ctx context.Context, chainID uint64, tokens []string, from, through int64) (trades []model.RawTradeAt, ok bool, err error)
	// PriceHistory returns each token's stored prices at heights
	// from..through, oldest first, led by its latest price below from.
	PriceHistory(ctx context.Context, chainID uint64, tokens []string, from, through int64) (map[string][]model.PricePoint, error)
}

// TransferReader serves raw transfers to the API.
type TransferReader interface {
	Transfers(ctx context.Context, query model.TransferQuery) ([]model.Transfer, error)
}
