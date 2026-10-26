package data

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// historyPoll is how often the history rereads ch-loader's checkpoint while
// it waits for a height.
const historyPoll = 200 * time.Millisecond

// historyRepo reads ch-loader's raw trades and market-engine's token prices,
// which share one database. ch-loader consumes the chain topics on its own
// checkpoint, so RawTrades waits up to wait for it to reach the heights
// asked for.
type historyRepo struct {
	db   *gorm.DB
	wait time.Duration
}

// NewHistoryRepo reads history from db, which has the market and raw
// tables, waiting up to wait for ch-loader to catch up.
func NewHistoryRepo(db *gorm.DB, wait time.Duration) biz.History {
	return &historyRepo{db: db, wait: wait}
}

// RawTrades reports ok false, without waiting, when ch-loader has never
// stored the chain, and after wait when it is still behind through.
func (history *historyRepo) RawTrades(ctx context.Context, chainID uint64, tokens []string, from, through int64) ([]model.RawTradeAt, bool, error) {
	if len(tokens) == 0 || through < from {
		return nil, true, nil
	}
	deadline := time.Now().Add(history.wait)
	for {
		checkpoint, found, err := chdb.LoadRawCheckpoint(ctx, history.db, chainID)
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
	trades, err := history.rawTrades(ctx, chainID, tokens, from, through)
	return trades, err == nil, err
}

func (history *historyRepo) rawTrades(ctx context.Context, chainID uint64, tokens []string, from, through int64) ([]model.RawTradeAt, error) {
	var rows []chdb.RawTradeRow
	err := history.db.WithContext(ctx).Select(`block_height, block_time, transaction_hash, transaction_index,
		log_index, operation_index, protocol, dex_address, pool_id, action, sender, trader, recipient,
		amount_assets, amount_raw, amount_values, amount_decimals, amount_decimals_known,
		reserves_after, sqrt_price_x96, liquidity, tick, active_bin_id`).
		Where("chain_id = ? AND block_height >= ? AND block_height <= ? AND hasAny(amount_assets, ?)",
			chainID, from, through, chdb.ArrayParam[string](tokens)).
		Order("block_height, log_index, operation_index").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse history: query trades: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	trades := make([]model.RawTradeAt, len(rows))
	for index, row := range rows {
		count := len(row.AmountAssets)
		if len(row.AmountRaw) != count || len(row.AmountValues) != count || len(row.AmountDecimals) != count ||
			len(row.AmountDecimalsKnown) != count {
			return nil, errors.New("clickhouse history: trade amount arrays differ in length")
		}
		trade := model.RawTradeAt{Height: row.BlockHeight, BlockTime: row.BlockTime.UTC()}
		trade.TransactionHash, trade.TransactionIndex = row.TransactionHash, row.TransactionIndex
		trade.LogIndex, trade.OperationIndex = row.LogIndex, row.OperationIndex
		trade.Protocol, trade.DexAddress, trade.PoolID, trade.Action = row.Protocol, row.DexAddress, row.PoolID, row.Action
		trade.Sender, trade.Trader, trade.Recipient = row.Sender, row.Trader, row.Recipient
		trade.ReservesAfter, trade.SqrtPriceX96, trade.Liquidity = row.ReservesAfter, row.SqrtPriceX96, row.Liquidity
		trade.Tick, trade.ActiveBinID = row.Tick, row.ActiveBinID
		trade.Amounts = make([]model.RawAmount, count)
		for amount := range count {
			trade.Amounts[amount] = model.RawAmount{
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
func (history *historyRepo) PriceHistory(ctx context.Context, chainID uint64, tokens []string, from, through int64) (map[string][]model.PricePoint, error) {
	if len(tokens) == 0 {
		return nil, nil
	}
	var rows []struct {
		Token       string
		BlockHeight int64
		PriceUSD    decimal.Decimal `gorm:"column:price_usd"`
		Hops        uint8
	}
	err := history.db.WithContext(ctx).Raw(`
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
		return nil, fmt.Errorf("clickhouse history: query prices: %w", err)
	}
	prices := make(map[string][]model.PricePoint)
	for _, row := range rows {
		prices[row.Token] = append(prices[row.Token], model.PricePoint{Height: row.BlockHeight, USD: row.PriceUSD, Hops: row.Hops})
	}
	for token, points := range prices {
		slices.SortStableFunc(points, func(left, right model.PricePoint) int {
			return cmp.Compare(left.Height, right.Height)
		})
		prices[token] = points
	}
	return prices, nil
}
