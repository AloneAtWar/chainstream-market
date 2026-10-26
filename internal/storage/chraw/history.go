package chraw

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/shopspring/decimal"
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

// History reads market-engine's backfills through store, waiting up to wait
// for ch-loader to catch up.
func (store *Store) History(wait time.Duration) storage.History {
	return NewHistory(store, wait)
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
	var rows []tradeRow
	err := store.db.WithContext(ctx).Select(`block_height, block_time, transaction_hash, transaction_index,
		log_index, operation_index, protocol, dex_address, pool_id, action, sender, trader, recipient,
		amount_assets, amount_raw, amount_values, amount_decimals, amount_decimals_known,
		reserves_after, sqrt_price_x96, liquidity, tick, active_bin_id`).
		Where("chain_id = ? AND block_height >= ? AND block_height <= ? AND hasAny(amount_assets, ?)",
			chainID, from, through, chstore.ArrayParam[string](tokens)).
		Order("block_height, log_index, operation_index").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse raw store: query trades: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	trades := make([]storage.RawTradeAt, len(rows))
	for index, row := range rows {
		count := len(row.AmountAssets)
		if len(row.AmountRaw) != count || len(row.AmountValues) != count || len(row.AmountDecimals) != count ||
			len(row.AmountDecimalsKnown) != count {
			return nil, errors.New("clickhouse raw store: trade amount arrays differ in length")
		}
		trade := storage.RawTradeAt{Height: row.BlockHeight, BlockTime: row.BlockTime.UTC()}
		trade.TransactionHash, trade.TransactionIndex = row.TransactionHash, row.TransactionIndex
		trade.LogIndex, trade.OperationIndex = row.LogIndex, row.OperationIndex
		trade.Protocol, trade.DexAddress, trade.PoolID, trade.Action = row.Protocol, row.DexAddress, row.PoolID, row.Action
		trade.Sender, trade.Trader, trade.Recipient = row.Sender, row.Trader, row.Recipient
		trade.ReservesAfter, trade.SqrtPriceX96, trade.Liquidity = row.ReservesAfter, row.SqrtPriceX96, row.Liquidity
		trade.Tick, trade.ActiveBinID = row.Tick, row.ActiveBinID
		trade.Amounts = make([]storage.RawAmount, count)
		for amount := range count {
			trade.Amounts[amount] = storage.RawAmount{
				Asset: row.AmountAssets[amount], Raw: row.AmountRaw[amount], Value: row.AmountValues[amount],
				Decimals: row.AmountDecimals[amount], DecimalsKnown: row.AmountDecimalsKnown[amount],
			}
		}
		trades[index] = trade
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
	var rows []struct {
		Token       string
		BlockHeight int64
		PriceUSD    decimal.Decimal `gorm:"column:price_usd"`
		Hops        uint8
	}
	err := history.store.db.WithContext(ctx).Raw(`
		SELECT token, block_height, price_usd, hops
		FROM token_prices
		WHERE chain_id = ? AND token IN ? AND block_height >= ? AND block_height <= ?
		UNION ALL
		SELECT token, max(block_height) AS block_height, argMax(price_usd, block_height) AS price_usd,
			argMax(hops, block_height) AS hops
		FROM token_prices
		WHERE chain_id = ? AND token IN ? AND block_height < ?
		GROUP BY token
		SETTINGS prefer_column_name_to_alias = 1`, chainID, tokens, from, through, chainID, tokens, from).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse raw store: query prices: %w", err)
	}
	prices := make(map[string][]storage.PricePoint)
	for _, row := range rows {
		prices[row.Token] = append(prices[row.Token], storage.PricePoint{Height: row.BlockHeight, USD: row.PriceUSD, Hops: row.Hops})
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
