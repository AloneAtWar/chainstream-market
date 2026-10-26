// Package chstore stores market-engine output in ClickHouse.
package chstore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Options struct {
	Addr     []string
	Database string
	Username string
	Password string
	// DialTimeout bounds connecting; zero uses the driver default.
	DialTimeout time.Duration
}

// tables lists every output table with the column Rewind deletes by. A trade
// or price recomputed after its token's decimals arrived keeps its own
// block_height and is deleted by the block that recomputed it.
var tables = []struct{ name, height string }{
	{"blocks", "block_height"},
	{"trades", "revision_height"},
	{"token_prices", "revision_height"},
	{"candles", "update_height"},
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Store writes each block's output as plain inserts. Rewriting a height that
// may already exist, after a restart or a reorg, deletes it first, so every
// height holds exactly one copy of its output.
type Store struct {
	conn driver.Conn
	mu   sync.Mutex
	// written is the highest height each chain may have rows at; blocks
	// above it need no delete before insert.
	written map[uint64]int64
}

// Open connects, creates the database and tables if needed, and loads the
// highest stored height per chain.
func Open(ctx context.Context, options Options) (*Store, error) {
	conn, err := Connect(ctx, options)
	if err != nil {
		return nil, err
	}
	store := &Store{conn: conn, written: make(map[uint64]int64)}
	if err := store.migrate(ctx); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	if err := store.loadWritten(ctx); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	return store, nil
}

// Connect creates options.Database if needed and connects to it. Deletes
// are synchronous on the returned connection.
func Connect(ctx context.Context, options Options) (driver.Conn, error) {
	if len(options.Addr) == 0 {
		return nil, errors.New("clickhouse store: at least one address is required")
	}
	if !identifier.MatchString(options.Database) {
		return nil, fmt.Errorf("clickhouse store: invalid database name %q", options.Database)
	}
	admin, err := connect(options, "default")
	if err != nil {
		return nil, err
	}
	createErr := admin.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+options.Database)
	closeErr := admin.Close()
	if joined := errors.Join(createErr, closeErr); joined != nil {
		return nil, fmt.Errorf("clickhouse store: create database: %w", joined)
	}
	return connect(options, options.Database)
}

func connect(options Options, database string) (driver.Conn, error) {
	conn, err := chgo.Open(&chgo.Options{
		Addr:        options.Addr,
		Auth:        chgo.Auth{Database: database, Username: options.Username, Password: options.Password},
		DialTimeout: options.DialTimeout,
		Settings: chgo.Settings{
			// Make deletes visible to the inserts and reads that follow.
			"lightweight_deletes_sync": 2,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: connect: %w", err)
	}
	return conn, nil
}

func (store *Store) migrate(ctx context.Context) error {
	for _, statement := range schema {
		if err := store.conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("clickhouse store: migrate: %w", err)
		}
	}
	return nil
}

func (store *Store) loadWritten(ctx context.Context) (err error) {
	rows, err := store.conn.Query(ctx, "SELECT chain_id, max(block_height) FROM blocks GROUP BY chain_id")
	if err != nil {
		return fmt.Errorf("clickhouse store: load heights: %w", err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var chainID uint64
		var height int64
		if scanErr := rows.Scan(&chainID, &height); scanErr != nil {
			return fmt.Errorf("clickhouse store: scan heights: %w", scanErr)
		}
		store.written[chainID] = height
	}
	return rows.Err()
}

func (store *Store) Close() error {
	return store.conn.Close()
}

func (store *Store) WriteBlock(ctx context.Context, block storage.BlockOutput) error {
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
	if err := store.insert(ctx, "blocks", func(batch driver.Batch) error {
		return batch.Append(block.ChainID, block.Height, block.Hash, block.BlockTime, block.Finalized)
	}); err != nil {
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
		if err := store.conn.Exec(ctx, query, chainID, height); err != nil {
			return fmt.Errorf("clickhouse store: delete %s %s %d: %w", table.name, operator, height, err)
		}
	}
	return nil
}

func (store *Store) insertTrades(ctx context.Context, block storage.BlockOutput) error {
	if len(block.Trades) == 0 {
		return nil
	}
	return store.insert(ctx, "trades", func(batch driver.Batch) error {
		for _, trade := range block.Trades {
			if err := batch.Append(
				trade.ChainID, trade.BlockHeight, trade.BlockTime, trade.TransactionHash, trade.TransactionIndex,
				trade.LogIndex, trade.OperationIndex, trade.Protocol, trade.PoolID, trade.Trader,
				trade.Side.String(), trade.BaseToken, trade.QuoteToken, trade.BaseAmount, trade.QuoteAmount,
				trade.PriceQuote, trade.PriceUSD, trade.ValueUSD, block.Finalized, block.Height, trade.Outlier,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *Store) insertPrices(ctx context.Context, block storage.BlockOutput) error {
	if len(block.Prices) == 0 {
		return nil
	}
	return store.insert(ctx, "token_prices", func(batch driver.Batch) error {
		for _, price := range block.Prices {
			if err := batch.Append(
				price.ChainID, price.Token, price.BlockHeight, price.BlockTime, price.PriceUSD,
				price.Source.String(), price.Hops, price.LiquidityUSD, block.Height,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *Store) insertCandles(ctx context.Context, block storage.BlockOutput) error {
	if len(block.Candles) == 0 {
		return nil
	}
	return store.insert(ctx, "candles", func(batch driver.Batch) error {
		for _, candle := range block.Candles {
			if err := batch.Append(
				candle.ChainID, candle.Subject.String(), candle.Key, uint32(candle.Interval/time.Second),
				candle.OpenTime, candle.Open, candle.High, candle.Low, candle.Close,
				candle.VolumeBase, candle.VolumeUSD, candle.Trades, candle.UpdateHeight,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *Store) insert(ctx context.Context, table string, fill func(driver.Batch) error) error {
	batch, err := store.conn.PrepareBatch(ctx, "INSERT INTO "+table)
	if err != nil {
		return fmt.Errorf("clickhouse store: prepare %s: %w", table, err)
	}
	if err := fill(batch); err != nil {
		return errors.Join(fmt.Errorf("clickhouse store: append %s: %w", table, err), batch.Abort())
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse store: send %s: %w", table, err)
	}
	return nil
}

var _ storage.Output = (*Store)(nil)
