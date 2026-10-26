// Package chstore stores market-engine output in ClickHouse.
package chstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"gorm.io/gorm"
)

// tables lists every output table with the column Rewind deletes by. A trade
// or price recomputed after its token's decimals arrived keeps its own
// block_height and is deleted by the block that recomputed it.
var tables = []struct{ name, height string }{
	{"blocks", "block_height"},
	{"trades", "revision_height"},
	{"token_prices", "revision_height"},
	{"candles", "update_height"},
}

// Store writes each block's output as plain inserts through GORM. Rewriting
// a height that may already exist, after a restart or a reorg, deletes it
// first, so every height holds exactly one copy of its output.
type Store struct {
	db *gorm.DB
	mu sync.Mutex
	// written is the highest height each chain may have rows at; blocks
	// above it need no delete before insert.
	written map[uint64]int64
}

// Open connects, creates the database and tables if needed, and loads the
// highest stored height per chain.
func Open(ctx context.Context, options chdb.Options) (*Store, error) {
	db, err := chdb.Connect(ctx, options)
	if err != nil {
		return nil, err
	}
	store := &Store{db: db, written: make(map[uint64]int64)}
	if err := store.migrate(ctx); err != nil {
		return nil, errors.Join(err, chdb.Close(db))
	}
	if err := store.loadWritten(ctx); err != nil {
		return nil, errors.Join(err, chdb.Close(db))
	}
	return store, nil
}

func (store *Store) migrate(ctx context.Context) error {
	if err := chdb.Migrate(ctx, store.db, chdb.MarketSchema); err != nil {
		return fmt.Errorf("clickhouse store: migrate: %w", err)
	}
	return nil
}

func (store *Store) loadWritten(ctx context.Context) error {
	var heights []struct {
		ChainID uint64
		Height  int64
	}
	if err := store.db.WithContext(ctx).Model(&blockRow{}).Select("chain_id, max(block_height) AS height").
		Group("chain_id").Find(&heights).Error; err != nil {
		return fmt.Errorf("clickhouse store: load heights: %w", err)
	}
	for _, row := range heights {
		store.written[row.ChainID] = row.Height
	}
	return nil
}

func (store *Store) Close() error {
	return chdb.Close(store.db)
}

func (store *Store) WriteBlock(ctx context.Context, block model.BlockOutput) error {
	if block.ChainID == 0 {
		return errors.New("clickhouse store: block chain id is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if written, ok := store.written[block.ChainID]; ok && block.Height <= written {
		if err := store.deleteFrom(ctx, block.ChainID, "=", block.Height); err != nil {
			return err
		}
	}
	if err := store.insertTrades(ctx, block); err != nil {
		return err
	}
	if err := store.insertPrices(ctx, block); err != nil {
		return err
	}
	if err := store.insertCandles(ctx, block); err != nil {
		return err
	}
	// The block row goes last: its presence means the height is complete.
	if err := store.insert(ctx, "blocks", &[]blockRow{{
		ChainID: block.ChainID, BlockHeight: block.Height, BlockHash: block.Hash,
		BlockTime: block.BlockTime, Finalized: block.Finalized,
	}}); err != nil {
		return err
	}
	if block.Height > store.written[block.ChainID] {
		store.written[block.ChainID] = block.Height
	}
	return nil
}

func (store *Store) Rewind(ctx context.Context, chainID uint64, rollbackTo int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.deleteFrom(ctx, chainID, ">", rollbackTo); err != nil {
		return err
	}
	if written, ok := store.written[chainID]; ok && written > rollbackTo {
		store.written[chainID] = rollbackTo
	}
	return nil
}

func (store *Store) deleteFrom(ctx context.Context, chainID uint64, operator string, height int64) error {
	for _, table := range tables {
		query := fmt.Sprintf("DELETE FROM %s WHERE chain_id = ? AND %s %s ?", table.name, table.height, operator)
		if err := store.db.WithContext(ctx).Exec(query, chainID, height).Error; err != nil {
			return fmt.Errorf("clickhouse store: delete %s %s %d: %w", table.name, operator, height, err)
		}
	}
	return nil
}

func (store *Store) insertTrades(ctx context.Context, block model.BlockOutput) error {
	if len(block.Trades) == 0 {
		return nil
	}
	rows := make([]tradeRow, len(block.Trades))
	for index, trade := range block.Trades {
		rows[index] = tradeRow{
			ChainID: trade.ChainID, BlockHeight: trade.BlockHeight, BlockTime: trade.BlockTime,
			TransactionHash: trade.TransactionHash, TransactionIndex: trade.TransactionIndex,
			LogIndex: trade.LogIndex, OperationIndex: trade.OperationIndex, Protocol: trade.Protocol,
			PoolID: trade.PoolID, Trader: trade.Trader, Side: trade.Side.String(),
			BaseToken: trade.BaseToken, QuoteToken: trade.QuoteToken,
			BaseAmount: trade.BaseAmount, QuoteAmount: trade.QuoteAmount, PriceQuote: trade.PriceQuote,
			PriceUSD: trade.PriceUSD, ValueUSD: trade.ValueUSD, Finalized: block.Finalized,
			RevisionHeight: block.Height, Outlier: trade.Outlier,
		}
	}
	return store.insert(ctx, "trades", &rows)
}

func (store *Store) insertPrices(ctx context.Context, block model.BlockOutput) error {
	if len(block.Prices) == 0 {
		return nil
	}
	rows := make([]priceRow, len(block.Prices))
	for index, price := range block.Prices {
		rows[index] = priceRow{
			ChainID: price.ChainID, Token: price.Token, BlockHeight: price.BlockHeight, BlockTime: price.BlockTime,
			PriceUSD: price.PriceUSD, Source: price.Source.String(), Hops: price.Hops,
			LiquidityUSD: price.LiquidityUSD, RevisionHeight: block.Height,
		}
	}
	return store.insert(ctx, "token_prices", &rows)
}

func (store *Store) insertCandles(ctx context.Context, block model.BlockOutput) error {
	if len(block.Candles) == 0 {
		return nil
	}
	rows := make([]candleRow, len(block.Candles))
	for index, candle := range block.Candles {
		rows[index] = candleRow{
			ChainID: candle.ChainID, Subject: candle.Subject.String(), Key: candle.Key,
			IntervalSeconds: uint32(candle.Interval / time.Second), OpenTime: candle.OpenTime,
			Open: candle.Open, High: candle.High, Low: candle.Low, Close: candle.Close,
			VolumeBase: candle.VolumeBase, VolumeUSD: candle.VolumeUSD, Trades: candle.Trades,
			UpdateHeight: candle.UpdateHeight,
		}
	}
	return store.insert(ctx, "candles", &rows)
}

func (store *Store) insert(ctx context.Context, table string, rows any) error {
	if err := chdb.Insert(ctx, store.db, rows); err != nil {
		return fmt.Errorf("clickhouse store: insert %s: %w", table, err)
	}
	return nil
}

var _ storage.Output = (*Store)(nil)

// ApplyRetention sets the TTL of market-engine's tables. Only market-engine
// calls it, so readers that open the store leave the TTL alone.
func (store *Store) ApplyRetention(ctx context.Context, retention chdb.Retention) error {
	return chdb.ApplyTTL(ctx, store.db, retention, retention.EngineTTL())
}
