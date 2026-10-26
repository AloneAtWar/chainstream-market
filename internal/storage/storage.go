// Package storage defines what market-engine persists and the interfaces its
// ClickHouse, Redis and memory backends implement.
//
// Every write is keyed by (chain, block height) so that replaying a block
// after a crash, or rewinding after a reorg, leaves exactly one copy of each
// fact. The engine saves its checkpoint only after Output.WriteBlock returns,
// so a store must tolerate the same block being written twice.
package storage

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// CheckpointStore persists one chainsync.Checkpoint per chain.
type CheckpointStore interface {
	Load(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error)
	Save(ctx context.Context, checkpoint chainsync.Checkpoint) error
}

// Output stores what the engine derives from each block.
type Output interface {
	// WriteBlock replaces everything previously written for block.Height.
	WriteBlock(ctx context.Context, block model.BlockOutput) error
	// Rewind removes everything written above rollbackTo and restores
	// running aggregates, such as open candles, to their state at that height.
	Rewind(ctx context.Context, chainID uint64, rollbackTo int64) error
}

// Reader serves stored market output to the API.
type Reader interface {
	// LatestPrice returns token's most recent USD price.
	LatestPrice(ctx context.Context, chainID uint64, token string) (model.TokenPrice, bool, error)
	// Candles returns the current version of each bucket in range, oldest
	// first.
	Candles(ctx context.Context, query model.CandleQuery) ([]model.Candle, error)
	// Trades returns the most recent trades, newest first.
	Trades(ctx context.Context, query model.TradeQuery) ([]model.Trade, error)
}
