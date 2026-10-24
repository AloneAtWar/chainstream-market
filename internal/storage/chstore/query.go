package chstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/shopspring/decimal"
)

func (store *Store) LatestPrice(ctx context.Context, chainID uint64, token string) (storage.TokenPrice, bool, error) {
	rows, err := store.conn.Query(ctx, `
		SELECT block_height, block_time, price_usd, toString(source), hops, liquidity_usd
		FROM token_prices
		WHERE chain_id = ? AND token = ?
		ORDER BY block_height DESC
		LIMIT 1`, chainID, token)
	if err != nil {
		return storage.TokenPrice{}, false, fmt.Errorf("clickhouse store: query price: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	if !rows.Next() {
		return storage.TokenPrice{}, false, rows.Err()
	}
	price := storage.TokenPrice{ChainID: chainID, Token: token}
	var source string
	if err := rows.Scan(&price.BlockHeight, &price.BlockTime, &price.PriceUSD, &source, &price.Hops, &price.LiquidityUSD); err != nil {
		return storage.TokenPrice{}, false, fmt.Errorf("clickhouse store: scan price: %w", err)
	}
	price.Source = storage.ParsePriceSource(source)
	return price, true, nil
}

func (store *Store) Candles(ctx context.Context, query storage.CandleQuery) ([]storage.Candle, error) {
	if query.Limit <= 0 || query.Interval < time.Second {
		return nil, errors.New("clickhouse store: candle query needs a limit and an interval")
	}
	rows, err := store.conn.Query(ctx, `
		SELECT open_time,
			argMax(open, update_height), argMax(high, update_height),
			argMax(low, update_height), argMax(close, update_height),
			argMax(volume_base, update_height), argMax(volume_usd, update_height),
			argMax(trades, update_height), max(update_height)
		FROM candles
		WHERE chain_id = ? AND subject = ? AND key = ? AND interval_seconds = ?
			AND open_time >= ? AND open_time < ?
		GROUP BY open_time
		ORDER BY open_time DESC
		LIMIT ?`,
		query.ChainID, query.Subject.String(), query.Key, uint32(query.Interval/time.Second),
		query.From, query.To, uint64(query.Limit))
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: query candles: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	var candles []storage.Candle
	for rows.Next() {
		candle := storage.Candle{
			ChainID: query.ChainID, Subject: query.Subject, Key: query.Key, Interval: query.Interval,
		}
		if err := rows.Scan(&candle.OpenTime, &candle.Open, &candle.High, &candle.Low, &candle.Close,
			&candle.VolumeBase, &candle.VolumeUSD, &candle.Trades, &candle.UpdateHeight); err != nil {
			return nil, fmt.Errorf("clickhouse store: scan candle: %w", err)
		}
		candle.OpenTime = candle.OpenTime.UTC()
		candles = append(candles, candle)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse store: read candles: %w", err)
	}
	slices.Reverse(candles)
	return candles, nil
}

func (store *Store) Trades(ctx context.Context, query storage.TradeQuery) ([]storage.Trade, error) {
	if query.Limit <= 0 || (query.BaseToken == "") == (query.PoolID == "") {
		return nil, errors.New("clickhouse store: trade query needs a limit and exactly one of base token or pool")
	}
	filter, value := "base_token = ?", query.BaseToken
	if query.PoolID != "" {
		filter, value = "pool_id = ?", query.PoolID
	}
	args := []any{query.ChainID, value}
	if query.BeforeHeight > 0 {
		filter += " AND block_height < ?"
		args = append(args, query.BeforeHeight)
	}
	args = append(args, uint64(query.Limit))
	statement := strings.Replace(`
		SELECT block_height, block_time, transaction_hash, transaction_index, log_index, operation_index,
			protocol, pool_id, trader, toString(side), base_token, quote_token,
			base_amount, quote_amount, price_quote, price_usd, value_usd
		FROM trades
		WHERE chain_id = ? AND FILTER
		ORDER BY block_height DESC, log_index DESC, operation_index DESC
		LIMIT ?`, "FILTER", filter, 1)
	rows, err := store.conn.Query(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: query trades: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	var trades []storage.Trade
	for rows.Next() {
		trade := storage.Trade{ChainID: query.ChainID}
		var side string
		var amounts [5]decimal.Decimal
		if err := rows.Scan(&trade.BlockHeight, &trade.BlockTime, &trade.TransactionHash, &trade.TransactionIndex,
			&trade.LogIndex, &trade.OperationIndex, &trade.Protocol, &trade.PoolID, &trade.Trader, &side,
			&trade.BaseToken, &trade.QuoteToken, &amounts[0], &amounts[1], &amounts[2], &amounts[3], &amounts[4]); err != nil {
			return nil, fmt.Errorf("clickhouse store: scan trade: %w", err)
		}
		trade.BlockTime = trade.BlockTime.UTC()
		trade.Side = storage.ParseSide(side)
		trade.BaseAmount, trade.QuoteAmount, trade.PriceQuote = amounts[0], amounts[1], amounts[2]
		trade.PriceUSD, trade.ValueUSD = amounts[3], amounts[4]
		trades = append(trades, trade)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse store: read trades: %w", err)
	}
	return trades, nil
}

var _ storage.Reader = (*Store)(nil)
