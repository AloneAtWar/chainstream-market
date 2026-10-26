package chstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

func (store *Store) LatestPrice(ctx context.Context, chainID uint64, token string) (storage.TokenPrice, bool, error) {
	var rows []priceRow
	err := store.db.WithContext(ctx).
		Select("block_height, block_time, price_usd, toString(source) AS source, hops, liquidity_usd").
		Where("chain_id = ? AND token = ?", chainID, token).
		Order("block_height DESC").Limit(1).Find(&rows).Error
	if err != nil {
		return storage.TokenPrice{}, false, fmt.Errorf("clickhouse store: query price: %w", err)
	}
	if len(rows) == 0 {
		return storage.TokenPrice{}, false, nil
	}
	row := rows[0]
	return storage.TokenPrice{
		ChainID: chainID, Token: token, BlockHeight: row.BlockHeight, BlockTime: row.BlockTime,
		PriceUSD: row.PriceUSD, Source: storage.ParsePriceSource(row.Source), Hops: row.Hops,
		LiquidityUSD: row.LiquidityUSD,
	}, true, nil
}

func (store *Store) Candles(ctx context.Context, query storage.CandleQuery) ([]storage.Candle, error) {
	if query.Limit <= 0 || query.Interval < time.Second {
		return nil, errors.New("clickhouse store: candle query needs a limit and an interval")
	}
	// The aliases name the model's columns; prefer_column_name_to_alias
	// keeps them from shadowing the columns the other aggregates read.
	var rows []candleRow
	err := store.db.WithContext(ctx).Raw(`
		SELECT open_time,
			argMax(open, update_height) AS open, argMax(high, update_height) AS high,
			argMax(low, update_height) AS low, argMax(close, update_height) AS close,
			argMax(volume_base, update_height) AS volume_base, argMax(volume_usd, update_height) AS volume_usd,
			argMax(trades, update_height) AS trades, max(update_height) AS update_height
		FROM candles
		WHERE chain_id = ? AND subject = ? AND key = ? AND interval_seconds = ?
			AND open_time >= ? AND open_time < ?
		GROUP BY open_time
		ORDER BY open_time DESC
		LIMIT ?
		SETTINGS prefer_column_name_to_alias = 1`,
		query.ChainID, query.Subject.String(), query.Key, uint32(query.Interval/time.Second),
		query.From, query.To, uint64(query.Limit)).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: query candles: %w", err)
	}
	candles := make([]storage.Candle, len(rows))
	for index, row := range rows {
		candles[index] = storage.Candle{
			ChainID: query.ChainID, Subject: query.Subject, Key: query.Key, Interval: query.Interval,
			OpenTime: row.OpenTime.UTC(), Open: row.Open, High: row.High, Low: row.Low, Close: row.Close,
			VolumeBase: row.VolumeBase, VolumeUSD: row.VolumeUSD, Trades: row.Trades, UpdateHeight: row.UpdateHeight,
		}
	}
	slices.Reverse(candles)
	if len(candles) == 0 {
		return nil, nil
	}
	return candles, nil
}

func (store *Store) Trades(ctx context.Context, query storage.TradeQuery) ([]storage.Trade, error) {
	if query.Limit <= 0 || (query.BaseToken == "") == (query.PoolID == "") {
		return nil, errors.New("clickhouse store: trade query needs a limit and exactly one of base token or pool")
	}
	db := store.db.WithContext(ctx).Model(&tradeRow{}).Select(`block_height, block_time, transaction_hash,
		transaction_index, log_index, operation_index, protocol, pool_id, trader, toString(side) AS side,
		base_token, quote_token, base_amount, quote_amount, price_quote, price_usd, value_usd, outlier`).
		Where("chain_id = ?", query.ChainID)
	if query.PoolID != "" {
		db = db.Where("pool_id = ?", query.PoolID)
	} else {
		db = db.Where("base_token = ?", query.BaseToken)
	}
	if query.BeforeHeight > 0 {
		db = db.Where("block_height < ?", query.BeforeHeight)
	}
	var rows []tradeRow
	if err := db.Order("block_height DESC, log_index DESC, operation_index DESC").Limit(query.Limit).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("clickhouse store: query trades: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	trades := make([]storage.Trade, len(rows))
	for index, row := range rows {
		trades[index] = storage.Trade{
			ChainID: query.ChainID, BlockHeight: row.BlockHeight, BlockTime: row.BlockTime.UTC(),
			TransactionHash: row.TransactionHash, TransactionIndex: row.TransactionIndex,
			LogIndex: row.LogIndex, OperationIndex: row.OperationIndex, Protocol: row.Protocol,
			PoolID: row.PoolID, Trader: row.Trader, Side: storage.ParseSide(row.Side),
			BaseToken: row.BaseToken, QuoteToken: row.QuoteToken, BaseAmount: row.BaseAmount,
			QuoteAmount: row.QuoteAmount, PriceQuote: row.PriceQuote, PriceUSD: row.PriceUSD,
			ValueUSD: row.ValueUSD, Outlier: row.Outlier,
		}
	}
	return trades, nil
}

var _ storage.Reader = (*Store)(nil)
