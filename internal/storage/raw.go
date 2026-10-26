package storage

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// RawStore keeps raw facts by (chain, height). It is not transactional, so
// a writer stores facts first and the checkpoint after them; a checkpoint
// can lag the facts but never lead them.
type RawStore interface {
	LoadRawCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error)
	// WriteRaw inserts blocks, which must not already be stored.
	WriteRaw(ctx context.Context, blocks []model.RawBlock) error
	// DeleteRawAbove removes every fact above height.
	DeleteRawAbove(ctx context.Context, chainID uint64, height int64) error
	SaveRawCheckpoint(ctx context.Context, checkpoint chainsync.Checkpoint) error
}

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
