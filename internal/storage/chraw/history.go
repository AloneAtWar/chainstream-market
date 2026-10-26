package chraw

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

// historyPoll is how often History rereads ch-loader's checkpoint while it
// waits for a height.
const historyPoll = 200 * time.Millisecond

// History reads ch-loader's raw trades and market-engine's token prices,
// which share one database. ch-loader consumes the chain topics on its own
// checkpoint, so RawTrades waits up to wait for it to reach the heights
// asked for.
type History struct {
	store *Store
	wait  time.Duration
}

// NewHistory reads history through store.
func NewHistory(store *Store, wait time.Duration) *History {
	return &History{store: store, wait: wait}
}

// RawTrades reports ok false, without waiting, when ch-loader has never
// stored the chain, and after wait when it is still behind through.
func (history *History) RawTrades(ctx context.Context, chainID uint64, tokens []string, from, through int64) ([]storage.RawTradeAt, bool, error) {
	if len(tokens) == 0 || through < from {
		return nil, true, nil
	}
	deadline := time.Now().Add(history.wait)
	for {
		checkpoint, found, err := history.store.LoadRawCheckpoint(ctx, chainID)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
		if checkpoint.Height >= through {
			break
		}
		if !time.Now().Before(deadline) {
			return nil, false, nil
		}
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(historyPoll):
		}
	}
	trades, err := history.store.rawTrades(ctx, chainID, tokens, from, through)
	return trades, err == nil, err
}

func (store *Store) rawTrades(ctx context.Context, chainID uint64, tokens []string, from, through int64) ([]storage.RawTradeAt, error) {
	rows, err := store.conn.Query(ctx, `
		SELECT block_height, block_time, transaction_hash, transaction_index, log_index, operation_index,
			protocol, dex_address, pool_id, action, sender, trader, recipient,
			amount_assets, amount_raw, amount_values, amount_decimals, amount_decimals_known,
			reserves_after, sqrt_price_x96, liquidity, tick, active_bin_id
		FROM raw_trades
		WHERE chain_id = ? AND block_height >= ? AND block_height <= ? AND hasAny(amount_assets, ?)
		ORDER BY block_height, log_index, operation_index`, chainID, from, through, tokens)
	if err != nil {
		return nil, fmt.Errorf("clickhouse raw store: query trades: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	var trades []storage.RawTradeAt
	for rows.Next() {
		var trade storage.RawTradeAt
		var assets, raws, values []string
		var decimals []uint32
		var known []bool
		if err := rows.Scan(&trade.Height, &trade.BlockTime, &trade.TransactionHash, &trade.TransactionIndex,
			&trade.LogIndex, &trade.OperationIndex, &trade.Protocol, &trade.DexAddress, &trade.PoolID,
			&trade.Action, &trade.Sender, &trade.Trader, &trade.Recipient,
			&assets, &raws, &values, &decimals, &known,
			&trade.ReservesAfter, &trade.SqrtPriceX96, &trade.Liquidity, &trade.Tick, &trade.ActiveBinID); err != nil {
			return nil, fmt.Errorf("clickhouse raw store: scan trade: %w", err)
		}
		if len(raws) != len(assets) || len(values) != len(assets) || len(decimals) != len(assets) || len(known) != len(assets) {
			return nil, errors.New("clickhouse raw store: trade amount arrays differ in length")
		}
		trade.BlockTime = trade.BlockTime.UTC()
		trade.Amounts = make([]storage.RawAmount, len(assets))
		for index := range assets {
			trade.Amounts[index] = storage.RawAmount{
				Asset: assets[index], Raw: raws[index], Value: values[index],
				Decimals: decimals[index], DecimalsKnown: known[index],
			}
		}
		trades = append(trades, trade)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse raw store: read trades: %w", err)
	}
	return trades, nil
}

// PriceHistory reads market-engine's token_prices: every row in range, and
// the latest row below from through the (chain_id, token, block_height)
// sort key.
func (history *History) PriceHistory(ctx context.Context, chainID uint64, tokens []string, from, through int64) (map[string][]storage.PricePoint, error) {
	if len(tokens) == 0 {
		return nil, nil
	}
	rows, err := history.store.conn.Query(ctx, `
		SELECT token, block_height, price_usd, hops
		FROM token_prices
		WHERE chain_id = ? AND token IN ? AND block_height >= ? AND block_height <= ?
		UNION ALL
		SELECT token, max(block_height), argMax(price_usd, block_height), argMax(hops, block_height)
		FROM token_prices
		WHERE chain_id = ? AND token IN ? AND block_height < ?
		GROUP BY token`, chainID, tokens, from, through, chainID, tokens, from)
	if err != nil {
		return nil, fmt.Errorf("clickhouse raw store: query prices: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	prices := make(map[string][]storage.PricePoint)
	for rows.Next() {
		var token string
		var point storage.PricePoint
		if err := rows.Scan(&token, &point.Height, &point.USD, &point.Hops); err != nil {
			return nil, fmt.Errorf("clickhouse raw store: scan price: %w", err)
		}
		prices[token] = append(prices[token], point)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse raw store: read prices: %w", err)
	}
	for token, points := range prices {
		slices.SortStableFunc(points, func(left, right storage.PricePoint) int {
			return cmp.Compare(left.Height, right.Height)
		})
		prices[token] = points
	}
	return prices, nil
}

var _ storage.History = (*History)(nil)
