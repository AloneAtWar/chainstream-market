package chstore

import (
	"context"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

func (store *Store) ChainHead(ctx context.Context, chainID uint64) (time.Time, bool, error) {
	rows, err := store.conn.Query(ctx,
		`SELECT block_time FROM blocks WHERE chain_id = ? ORDER BY block_height DESC LIMIT 1`, chainID)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("clickhouse store: query chain head: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	if !rows.Next() {
		return time.Time{}, false, rows.Err()
	}
	var head time.Time
	if err := rows.Scan(&head); err != nil {
		return time.Time{}, false, fmt.Errorf("clickhouse store: scan chain head: %w", err)
	}
	return head.UTC(), true, nil
}

// TokenActivity reads each bucket's current version first, then sums the
// buckets per token. Buckets chain open to previous close, so the earliest
// bucket's open is the price the window started at.
func (store *Store) TokenActivity(ctx context.Context, chainID uint64, interval time.Duration, since time.Time) ([]storage.TokenActivity, error) {
	if interval < time.Second {
		return nil, fmt.Errorf("clickhouse store: activity interval %s is below one second", interval)
	}
	rows, err := store.conn.Query(ctx, `
		SELECT key, sum(bucket_volume), sum(bucket_trades),
			argMin(bucket_open, open_time), argMax(bucket_close, open_time)
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
		chainID, uint32(interval/time.Second), since)
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: query activity: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	var activity []storage.TokenActivity
	for rows.Next() {
		var row storage.TokenActivity
		var trades uint64
		if err := rows.Scan(&row.Token, &row.VolumeUSD, &trades, &row.Open, &row.Close); err != nil {
			return nil, fmt.Errorf("clickhouse store: scan activity: %w", err)
		}
		row.Trades = int64(trades)
		activity = append(activity, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse store: read activity: %w", err)
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
	rows, err := store.conn.Query(ctx, `
		SELECT token, argMax(price_usd, block_height), argMax(liquidity_usd, block_height),
			max(block_height), argMax(block_time, block_height)
		FROM token_prices
		WHERE chain_id = ?`+filter+`
		GROUP BY token`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: query quotes: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	var quotes []storage.TokenQuote
	for rows.Next() {
		var quote storage.TokenQuote
		if err := rows.Scan(&quote.Token, &quote.PriceUSD, &quote.LiquidityUSD, &quote.BlockHeight, &quote.BlockTime); err != nil {
			return nil, fmt.Errorf("clickhouse store: scan quote: %w", err)
		}
		quote.BlockTime = quote.BlockTime.UTC()
		quotes = append(quotes, quote)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse store: read quotes: %w", err)
	}
	return quotes, nil
}

var _ storage.RankingSource = (*Store)(nil)
