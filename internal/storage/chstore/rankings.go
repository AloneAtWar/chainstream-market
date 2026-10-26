package chstore

import (
	"context"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/shopspring/decimal"
)

func (store *Store) ChainHead(ctx context.Context, chainID uint64) (time.Time, bool, error) {
	var rows []blockRow
	err := store.db.WithContext(ctx).Select("block_time").Where("chain_id = ?", chainID).
		Order("block_height DESC").Limit(1).Find(&rows).Error
	if err != nil {
		return time.Time{}, false, fmt.Errorf("clickhouse store: query chain head: %w", err)
	}
	if len(rows) == 0 {
		return time.Time{}, false, nil
	}
	return rows[0].BlockTime.UTC(), true, nil
}

// TokenActivity reads each bucket's current version first, then sums the
// buckets per token. Buckets chain open to previous close, so the earliest
// bucket's open is the price the window started at.
func (store *Store) TokenActivity(ctx context.Context, chainID uint64, interval time.Duration, since time.Time) ([]storage.TokenActivity, error) {
	if interval < time.Second {
		return nil, fmt.Errorf("clickhouse store: activity interval %s is below one second", interval)
	}
	var rows []struct {
		Key       string
		VolumeUSD decimal.Decimal `gorm:"column:volume_usd"`
		Trades    uint64
		Open      decimal.Decimal
		Close     decimal.Decimal
	}
	err := store.db.WithContext(ctx).Raw(`
		SELECT key, sum(bucket_volume) AS volume_usd, sum(bucket_trades) AS trades,
			argMin(bucket_open, open_time) AS open, argMax(bucket_close, open_time) AS close
		FROM (
			SELECT key, open_time,
				argMax(volume_usd, update_height) AS bucket_volume,
				argMax(trades, update_height) AS bucket_trades,
				argMax(open, update_height) AS bucket_open,
				argMax(close, update_height) AS bucket_close
			FROM candles
			WHERE chain_id = ? AND subject = 'token' AND interval_seconds = ? AND open_time >= ?
			GROUP BY key, open_time
		)
		GROUP BY key`,
		chainID, uint32(interval/time.Second), since).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: query activity: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	activity := make([]storage.TokenActivity, len(rows))
	for index, row := range rows {
		activity[index] = storage.TokenActivity{
			Token: row.Key, VolumeUSD: row.VolumeUSD, Trades: int64(row.Trades), Open: row.Open, Close: row.Close,
		}
	}
	return activity, nil
}

// LatestQuotes finds recently priced tokens by block_time, which the monthly
// partitions and their min/max index narrow to recent parts, then reads each
// one's latest row through the (chain_id, token) sort key.
func (store *Store) LatestQuotes(ctx context.Context, chainID uint64, since time.Time, tokens []string) ([]storage.TokenQuote, error) {
	filter, args := "", []any{chainID}
	if !since.IsZero() {
		filter = ` AND (token IN (SELECT token FROM token_prices WHERE chain_id = ? AND block_time >= ?)`
		args = append(args, chainID, since)
		if len(tokens) > 0 {
			filter += ` OR token IN ?`
			args = append(args, tokens)
		}
		filter += `)`
	}
	// The aliases name the model's columns; prefer_column_name_to_alias
	// keeps them from shadowing the columns the other aggregates read.
	var rows []priceRow
	err := store.db.WithContext(ctx).Raw(`
		SELECT token, argMax(price_usd, block_height) AS price_usd, argMax(liquidity_usd, block_height) AS liquidity_usd,
			max(block_height) AS block_height, argMax(block_time, block_height) AS block_time
		FROM token_prices
		WHERE chain_id = ?`+filter+`
		GROUP BY token
		SETTINGS prefer_column_name_to_alias = 1`, args...).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: query quotes: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	quotes := make([]storage.TokenQuote, len(rows))
	for index, row := range rows {
		quotes[index] = storage.TokenQuote{
			Token: row.Token, PriceUSD: row.PriceUSD, LiquidityUSD: row.LiquidityUSD,
			BlockHeight: row.BlockHeight, BlockTime: row.BlockTime.UTC(),
		}
	}
	return quotes, nil
}

var _ storage.RankingSource = (*Store)(nil)
