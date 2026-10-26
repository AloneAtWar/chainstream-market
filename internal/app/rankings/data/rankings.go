package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/rankings/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// rankingRepo keeps the token_rankings table and its checkpoint in
// PostgreSQL.
type rankingRepo struct {
	db *gorm.DB
}

// NewRankingRepo returns market-rankings' repository.
func NewRankingRepo(data *Data) biz.RankingRepo { return &rankingRepo{db: data.state} }

// LoadRankingCursor returns the chain head time of the last refresh.
func (repo *rankingRepo) LoadRankingCursor(ctx context.Context, chainID uint64) (time.Time, bool, error) {
	var row pgdb.RankingCheckpointRow
	err := repo.db.WithContext(ctx).Where("chain_id = ?", int64(chainID)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("postgres store: load ranking cursor: %w", err)
	}
	return row.HeadTime.UTC(), true, nil
}

// RankedSince returns ranked tokens whose price is at or after since.
func (repo *rankingRepo) RankedSince(ctx context.Context, chainID uint64, since time.Time) ([]string, error) {
	tokens := []string{}
	err := repo.db.WithContext(ctx).Table("token_rankings").
		Where("chain_id = ? AND price_time >= ?", int64(chainID), since).Pluck("token", &tokens).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: ranked tokens: %w", err)
	}
	return tokens, nil
}

// CommitRankings applies refresh in one transaction. Updates skip rows
// whose values did not change, so a refresh rewrites only what moved.
func (repo *rankingRepo) CommitRankings(ctx context.Context, refresh model.RankingRefresh) error {
	if refresh.ChainID == 0 {
		return errors.New("postgres store: ranking chain id is required")
	}
	chainID := int64(refresh.ChainID)
	err := repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := &pgdb.Runner{Tx: tx}
		queueQuotes(run, chainID, refresh)
		queueActivity(run, chainID, refresh.Activity)
		run.Exec(`DELETE FROM token_rankings WHERE chain_id = ? AND price_height IS NULL AND trades_24h = 0`, chainID)
		// holder-indexer keeps holder_counts current, so this is one lookup per
		// ranked token. A token missing there has no holders.
		run.Exec(`UPDATE token_rankings AS r SET holders = c.holders
			FROM (
				SELECT k.token, COALESCE(h.holders, 0) AS holders
				FROM token_rankings AS k
				LEFT JOIN holder_counts AS h ON h.chain_id = k.chain_id AND h.token = k.token
				WHERE k.chain_id = @chain
			) AS c
			WHERE r.chain_id = @chain AND r.token = c.token AND r.holders <> c.holders`, map[string]any{"chain": chainID})
		run.Exec(`UPDATE token_rankings AS r SET created_height = d.height, created_time = d.created_at
			FROM (
				SELECT k.token, (t.deployment->>'block_height')::bigint AS height, (t.deployment->>'block_time')::bigint AS created_at
				FROM token_rankings AS k
				LEFT JOIN registry_tokens AS t ON t.chain_id = k.chain_id AND t.address = k.token
				WHERE k.chain_id = @chain
			) AS d
			WHERE r.chain_id = @chain AND r.token = d.token
			  AND (r.created_height, r.created_time) IS DISTINCT FROM (d.height, d.created_at)`, map[string]any{"chain": chainID})
		if run.Err != nil {
			return run.Err
		}
		refreshedAt := refresh.RefreshedAt
		if refreshedAt.IsZero() {
			refreshedAt = time.Now()
		}
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"head_time", "refreshed_at"}),
		}).Create(&pgdb.RankingCheckpointRow{ChainID: chainID, HeadTime: refresh.Head, RefreshedAt: refreshedAt}).Error
	})
	if err != nil {
		return fmt.Errorf("postgres store: commit rankings chain %d: %w", refresh.ChainID, err)
	}
	return nil
}

func queueQuotes(run *pgdb.Runner, chainID int64, refresh model.RankingRefresh) {
	count := len(refresh.Quotes)
	tokens, prices, liquidity := make([]string, count), make([]string, count), make([]string, count)
	heights, times := make([]int64, count), make([]time.Time, count)
	for index, quote := range refresh.Quotes {
		tokens[index], prices[index], liquidity[index] = quote.Token, quote.PriceUSD.String(), quote.LiquidityUSD.String()
		heights[index], times[index] = quote.BlockHeight, quote.BlockTime
	}
	run.Exec(`INSERT INTO token_rankings AS r (chain_id, token, price_usd, liquidity_usd, price_height, price_time)
		SELECT @chain, token, price::numeric, liquidity::numeric, height, priced_at
		FROM unnest(CAST(@tokens AS text[]), CAST(@prices AS text[]), CAST(@liquidity AS text[]), CAST(@heights AS bigint[]), CAST(@times AS timestamptz[]))
			AS q(token, price, liquidity, height, priced_at)
		ON CONFLICT (chain_id, token) DO UPDATE SET price_usd = EXCLUDED.price_usd,
			liquidity_usd = EXCLUDED.liquidity_usd, price_height = EXCLUDED.price_height, price_time = EXCLUDED.price_time
		WHERE (r.price_usd, r.liquidity_usd, r.price_height, r.price_time)
			IS DISTINCT FROM (EXCLUDED.price_usd, EXCLUDED.liquidity_usd, EXCLUDED.price_height, EXCLUDED.price_time)`,
		map[string]any{
			"chain": chainID, "tokens": pgdb.Array(tokens), "prices": pgdb.Array(prices), "liquidity": pgdb.Array(liquidity),
			"heights": pgdb.Array(heights), "times": pgdb.Array(times),
		})
	// A token the scan covered but did not return has no price rows left.
	var since *time.Time
	if !refresh.Since.IsZero() {
		since = &refresh.Since
	}
	run.Exec(`UPDATE token_rankings
		SET price_usd = NULL, price_height = NULL, price_time = NULL, liquidity_usd = 0
		WHERE chain_id = @chain AND price_height IS NOT NULL
		  AND (CAST(@since AS timestamptz) IS NULL OR price_time >= @since) AND NOT (token = ANY(CAST(@tokens AS text[])))`,
		map[string]any{"chain": chainID, "since": since, "tokens": pgdb.Array(tokens)})
}

func queueActivity(run *pgdb.Runner, chainID int64, activity []model.TokenActivity) {
	count := len(activity)
	tokens, volumes, changes := make([]string, count), make([]string, count), make([]string, count)
	trades := make([]int64, count)
	for index, row := range activity {
		tokens[index], volumes[index], trades[index] = row.Token, row.VolumeUSD.String(), row.Trades
		changes[index] = priceChange(row.Open, row.Close).String()
	}
	run.Exec(`UPDATE token_rankings SET volume_24h_usd = 0, trades_24h = 0, price_change_24h = 0
		WHERE chain_id = @chain AND (volume_24h_usd <> 0 OR trades_24h <> 0 OR price_change_24h <> 0)
		  AND NOT (token = ANY(CAST(@tokens AS text[])))`, map[string]any{"chain": chainID, "tokens": pgdb.Array(tokens)})
	run.Exec(`INSERT INTO token_rankings AS r (chain_id, token, volume_24h_usd, trades_24h, price_change_24h)
		SELECT @chain, token, volume::numeric, trades, change::numeric
		FROM unnest(CAST(@tokens AS text[]), CAST(@volumes AS text[]), CAST(@trades AS bigint[]), CAST(@changes AS text[])) AS a(token, volume, trades, change)
		ON CONFLICT (chain_id, token) DO UPDATE SET volume_24h_usd = EXCLUDED.volume_24h_usd,
			trades_24h = EXCLUDED.trades_24h, price_change_24h = EXCLUDED.price_change_24h
		WHERE (r.volume_24h_usd, r.trades_24h, r.price_change_24h)
			IS DISTINCT FROM (EXCLUDED.volume_24h_usd, EXCLUDED.trades_24h, EXCLUDED.price_change_24h)`,
		map[string]any{
			"chain": chainID, "tokens": pgdb.Array(tokens), "volumes": pgdb.Array(volumes), "trades": pgdb.Array(trades),
			"changes": pgdb.Array(changes),
		})
}

// priceChange is close/open - 1, or zero without an opening price.
func priceChange(open, closePrice decimal.Decimal) decimal.Decimal {
	if !open.IsPositive() {
		return decimal.Zero
	}
	return closePrice.DivRound(open, 18).Sub(decimal.NewFromInt(1))
}
