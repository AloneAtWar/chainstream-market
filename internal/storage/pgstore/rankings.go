package pgstore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (store *Store) LoadRankingCursor(ctx context.Context, chainID uint64) (time.Time, bool, error) {
	var row rankingCheckpointRow
	err := store.db.WithContext(ctx).Where("chain_id = ?", int64(chainID)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("postgres store: load ranking cursor: %w", err)
	}
	return row.HeadTime.UTC(), true, nil
}

func (store *Store) RankedSince(ctx context.Context, chainID uint64, since time.Time) ([]string, error) {
	tokens := []string{}
	err := store.db.WithContext(ctx).Table("token_rankings").
		Where("chain_id = ? AND price_time >= ?", int64(chainID), since).Pluck("token", &tokens).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: ranked tokens: %w", err)
	}
	return tokens, nil
}

// CommitRankings applies refresh in one transaction. Updates skip rows
// whose values did not change, so a refresh rewrites only what moved.
func (store *Store) CommitRankings(ctx context.Context, refresh model.RankingRefresh) error {
	if refresh.ChainID == 0 {
		return errors.New("postgres store: ranking chain id is required")
	}
	chainID := int64(refresh.ChainID)
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := &runner{tx: tx}
		queueQuotes(run, chainID, refresh)
		queueActivity(run, chainID, refresh.Activity)
		run.exec(`DELETE FROM token_rankings WHERE chain_id = ? AND price_height IS NULL AND trades_24h = 0`, chainID)
		// holder-indexer keeps holder_counts current, so this is one lookup per
		// ranked token. A token missing there has no holders.
		run.exec(`UPDATE token_rankings AS r SET holders = c.holders
			FROM (
				SELECT k.token, COALESCE(h.holders, 0) AS holders
				FROM token_rankings AS k
				LEFT JOIN holder_counts AS h ON h.chain_id = k.chain_id AND h.token = k.token
				WHERE k.chain_id = @chain
			) AS c
			WHERE r.chain_id = @chain AND r.token = c.token AND r.holders <> c.holders`, map[string]any{"chain": chainID})
		run.exec(`UPDATE token_rankings AS r SET created_height = d.height, created_time = d.created_at
			FROM (
				SELECT k.token, (t.deployment->>'block_height')::bigint AS height, (t.deployment->>'block_time')::bigint AS created_at
				FROM token_rankings AS k
				LEFT JOIN registry_tokens AS t ON t.chain_id = k.chain_id AND t.address = k.token
				WHERE k.chain_id = @chain
			) AS d
			WHERE r.chain_id = @chain AND r.token = d.token
			  AND (r.created_height, r.created_time) IS DISTINCT FROM (d.height, d.created_at)`, map[string]any{"chain": chainID})
		if run.err != nil {
			return run.err
		}
		refreshedAt := refresh.RefreshedAt
		if refreshedAt.IsZero() {
			refreshedAt = time.Now()
		}
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"head_time", "refreshed_at"}),
		}).Create(&rankingCheckpointRow{ChainID: chainID, HeadTime: refresh.Head, RefreshedAt: refreshedAt}).Error
	})
	if err != nil {
		return fmt.Errorf("postgres store: commit rankings chain %d: %w", refresh.ChainID, err)
	}
	return nil
}

func queueQuotes(run *runner, chainID int64, refresh model.RankingRefresh) {
	count := len(refresh.Quotes)
	tokens, prices, liquidity := make([]string, count), make([]string, count), make([]string, count)
	heights, times := make([]int64, count), make([]time.Time, count)
	for index, quote := range refresh.Quotes {
		tokens[index], prices[index], liquidity[index] = quote.Token, quote.PriceUSD.String(), quote.LiquidityUSD.String()
		heights[index], times[index] = quote.BlockHeight, quote.BlockTime
	}
	run.exec(`INSERT INTO token_rankings AS r (chain_id, token, price_usd, liquidity_usd, price_height, price_time)
		SELECT @chain, token, price::numeric, liquidity::numeric, height, priced_at
		FROM unnest(CAST(@tokens AS text[]), CAST(@prices AS text[]), CAST(@liquidity AS text[]), CAST(@heights AS bigint[]), CAST(@times AS timestamptz[]))
			AS q(token, price, liquidity, height, priced_at)
		ON CONFLICT (chain_id, token) DO UPDATE SET price_usd = EXCLUDED.price_usd,
			liquidity_usd = EXCLUDED.liquidity_usd, price_height = EXCLUDED.price_height, price_time = EXCLUDED.price_time
		WHERE (r.price_usd, r.liquidity_usd, r.price_height, r.price_time)
			IS DISTINCT FROM (EXCLUDED.price_usd, EXCLUDED.liquidity_usd, EXCLUDED.price_height, EXCLUDED.price_time)`,
		map[string]any{
			"chain": chainID, "tokens": array(tokens), "prices": array(prices), "liquidity": array(liquidity),
			"heights": array(heights), "times": array(times),
		})
	// A token the scan covered but did not return has no price rows left.
	var since *time.Time
	if !refresh.Since.IsZero() {
		since = &refresh.Since
	}
	run.exec(`UPDATE token_rankings
		SET price_usd = NULL, price_height = NULL, price_time = NULL, liquidity_usd = 0
		WHERE chain_id = @chain AND price_height IS NOT NULL
		  AND (CAST(@since AS timestamptz) IS NULL OR price_time >= @since) AND NOT (token = ANY(CAST(@tokens AS text[])))`,
		map[string]any{"chain": chainID, "since": since, "tokens": array(tokens)})
}

func queueActivity(run *runner, chainID int64, activity []model.TokenActivity) {
	count := len(activity)
	tokens, volumes, changes := make([]string, count), make([]string, count), make([]string, count)
	trades := make([]int64, count)
	for index, row := range activity {
		tokens[index], volumes[index], trades[index] = row.Token, row.VolumeUSD.String(), row.Trades
		changes[index] = priceChange(row.Open, row.Close).String()
	}
	run.exec(`UPDATE token_rankings SET volume_24h_usd = 0, trades_24h = 0, price_change_24h = 0
		WHERE chain_id = @chain AND (volume_24h_usd <> 0 OR trades_24h <> 0 OR price_change_24h <> 0)
		  AND NOT (token = ANY(CAST(@tokens AS text[])))`, map[string]any{"chain": chainID, "tokens": array(tokens)})
	run.exec(`INSERT INTO token_rankings AS r (chain_id, token, volume_24h_usd, trades_24h, price_change_24h)
		SELECT @chain, token, volume::numeric, trades, change::numeric
		FROM unnest(CAST(@tokens AS text[]), CAST(@volumes AS text[]), CAST(@trades AS bigint[]), CAST(@changes AS text[])) AS a(token, volume, trades, change)
		ON CONFLICT (chain_id, token) DO UPDATE SET volume_24h_usd = EXCLUDED.volume_24h_usd,
			trades_24h = EXCLUDED.trades_24h, price_change_24h = EXCLUDED.price_change_24h
		WHERE (r.volume_24h_usd, r.trades_24h, r.price_change_24h)
			IS DISTINCT FROM (EXCLUDED.volume_24h_usd, EXCLUDED.trades_24h, EXCLUDED.price_change_24h)`,
		map[string]any{
			"chain": chainID, "tokens": array(tokens), "volumes": array(volumes), "trades": array(trades),
			"changes": array(changes),
		})
}

// priceChange is close/open - 1, or zero without an opening price.
func priceChange(open, closePrice decimal.Decimal) decimal.Decimal {
	if !open.IsPositive() {
		return decimal.Zero
	}
	return closePrice.DivRound(open, 18).Sub(decimal.NewFromInt(1))
}

var (
	rankingColumns = map[model.RankingSort]string{
		model.SortVolume24h:      "volume_24h_usd",
		model.SortPriceChange24h: "price_change_24h",
		model.SortLiquidity:      "liquidity_usd",
		model.SortHolders:        "holders",
		model.SortCreated:        "created_height",
	}
	addressPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)
	likeEscaper    = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
)

// TokenList reads a page of the rankings, joined with registry metadata. A
// search starts from registry_tokens instead, so tokens without market data
// are found too and list after the ranked ones.
func (store *Store) TokenList(ctx context.Context, query model.TokenListQuery) ([]model.RankedToken, error) {
	column, ok := rankingColumns[query.Sort]
	if !ok || query.Limit <= 0 || query.Offset < 0 {
		return nil, errors.New("postgres store: token list needs a known sort, a limit and a non-negative offset")
	}
	direction := "DESC"
	if query.Ascending {
		direction = "ASC"
	}
	values := map[string]any{
		"chain": int64(query.ChainID), "min_liquidity": query.MinLiquidityUSD.String(),
		"limit": query.Limit, "offset": query.Offset,
	}
	var from, where, order string
	search := strings.ToLower(strings.TrimSpace(query.Search))
	switch {
	case search == "":
		from = `token_rankings AS r LEFT JOIN registry_tokens AS t ON t.chain_id = r.chain_id AND t.address = r.token`
		where = `r.chain_id = @chain AND r.liquidity_usd >= CAST(@min_liquidity AS numeric)`
		if query.Sort == model.SortCreated {
			where += ` AND r.created_height IS NOT NULL`
		}
		order = fmt.Sprintf(`r.%s %s, r.token %s`, column, direction, direction)
	case addressPattern.MatchString(search):
		values["address"] = search
		from = `(SELECT CAST(@address AS text) AS address) AS k
			LEFT JOIN registry_tokens AS t ON t.chain_id = @chain AND t.address = k.address
			LEFT JOIN token_rankings AS r ON r.chain_id = @chain AND r.token = k.address`
		where = `(t.address IS NOT NULL OR r.token IS NOT NULL) AND COALESCE(r.liquidity_usd, 0) >= CAST(@min_liquidity AS numeric)`
		order = `1`
	default:
		values["prefix"], values["symbol"] = likeEscaper.Replace(search)+"%", search
		from = `registry_tokens AS t LEFT JOIN token_rankings AS r ON r.chain_id = t.chain_id AND r.token = t.address`
		where = `t.chain_id = @chain AND lower(t.symbol) LIKE @prefix AND COALESCE(r.liquidity_usd, 0) >= CAST(@min_liquidity AS numeric)`
		order = fmt.Sprintf(`lower(t.symbol) = @symbol DESC, r.token IS NOT NULL DESC, r.%s %s NULLS LAST, t.address %s`,
			column, direction, direction)
	}
	var rows []rankedTokenRow
	err := store.db.WithContext(ctx).Raw(`
		SELECT COALESCE(r.token, t.address) AS token, t.name, t.symbol, t.decimals, r.token IS NOT NULL AS ranked,
			r.price_usd::text AS price_usd, r.price_height, COALESCE(r.price_change_24h, 0)::text AS price_change_24h,
			COALESCE(r.volume_24h_usd, 0)::text AS volume_24h_usd, COALESCE(r.trades_24h, 0) AS trades_24h,
			COALESCE(r.liquidity_usd, 0)::text AS liquidity_usd, COALESCE(r.holders, 0) AS holders,
			r.created_height, r.created_time, c.refreshed_at
		FROM `+from+`
		LEFT JOIN ranking_checkpoints AS c ON c.chain_id = @chain
		WHERE `+where+`
		ORDER BY `+order+`
		LIMIT @limit OFFSET @offset`, values).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: token list: %w", err)
	}
	tokens := make([]model.RankedToken, len(rows))
	for index, row := range rows {
		if tokens[index], err = row.rankedToken(); err != nil {
			return nil, fmt.Errorf("postgres store: read token list: %w", err)
		}
	}
	return tokens, nil
}

// rankedTokenRow is one TokenList row with its numerics as text.
type rankedTokenRow struct {
	Token          string
	Name           *string
	Symbol         *string
	Decimals       *int16
	Ranked         bool
	PriceUSD       *string `gorm:"column:price_usd"`
	PriceHeight    *int64
	PriceChange24h string `gorm:"column:price_change_24h"`
	Volume24hUSD   string `gorm:"column:volume_24h_usd"`
	Trades24h      int64  `gorm:"column:trades_24h"`
	LiquidityUSD   string `gorm:"column:liquidity_usd"`
	Holders        int64
	CreatedHeight  *int64
	CreatedTime    *int64
	RefreshedAt    *time.Time
}

func (row rankedTokenRow) rankedToken() (model.RankedToken, error) {
	token := model.RankedToken{
		Token: row.Token, Name: row.Name, Symbol: row.Symbol, Ranked: row.Ranked, PriceHeight: row.PriceHeight,
		Trades24h: row.Trades24h, Holders: row.Holders, CreatedHeight: row.CreatedHeight, CreatedTime: row.CreatedTime,
	}
	if row.Decimals != nil {
		value := uint8(*row.Decimals)
		token.Decimals = &value
	}
	if row.PriceUSD != nil {
		value, err := decimal.NewFromString(*row.PriceUSD)
		if err != nil {
			return token, fmt.Errorf("price of %s: %w", token.Token, err)
		}
		token.PriceUSD = &value
	}
	var err error
	if token.PriceChange24h, err = decimal.NewFromString(row.PriceChange24h); err != nil {
		return token, fmt.Errorf("price change of %s: %w", token.Token, err)
	}
	if token.Volume24hUSD, err = decimal.NewFromString(row.Volume24hUSD); err != nil {
		return token, fmt.Errorf("volume of %s: %w", token.Token, err)
	}
	if token.LiquidityUSD, err = decimal.NewFromString(row.LiquidityUSD); err != nil {
		return token, fmt.Errorf("liquidity of %s: %w", token.Token, err)
	}
	if row.RefreshedAt != nil {
		token.RefreshedAt = row.RefreshedAt.UTC()
	}
	return token, nil
}

var (
	_ storage.RankingStore  = (*Store)(nil)
	_ storage.RankingReader = (*Store)(nil)
)
