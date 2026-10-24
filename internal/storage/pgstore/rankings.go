package pgstore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// rankingSchema creates the token rankings, a rollup that market-rankings
// rewrites from the ClickHouse output, holder_balances and registry_tokens.
// Each sort column has an index led by chain_id, so a page reads only the
// rows it returns.
var rankingSchema = []string{
	`CREATE TABLE IF NOT EXISTS ranking_checkpoints (
		chain_id     BIGINT PRIMARY KEY,
		head_time    TIMESTAMPTZ NOT NULL,
		refreshed_at TIMESTAMPTZ NOT NULL
	)`,
	// price_* are NULL for a token that traded in the window without a
	// price of its own.
	`CREATE TABLE IF NOT EXISTS token_rankings (
		chain_id         BIGINT NOT NULL,
		token            TEXT NOT NULL,
		price_usd        NUMERIC,
		price_height     BIGINT,
		price_time       TIMESTAMPTZ,
		liquidity_usd    NUMERIC NOT NULL DEFAULT 0,
		volume_24h_usd   NUMERIC NOT NULL DEFAULT 0,
		trades_24h       BIGINT NOT NULL DEFAULT 0,
		price_change_24h NUMERIC NOT NULL DEFAULT 0,
		holders          BIGINT NOT NULL DEFAULT 0,
		created_height   BIGINT,
		created_time     BIGINT,
		PRIMARY KEY (chain_id, token)
	)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_volume ON token_rankings (chain_id, volume_24h_usd, token)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_change ON token_rankings (chain_id, price_change_24h, token)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_liquidity ON token_rankings (chain_id, liquidity_usd, token)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_holders ON token_rankings (chain_id, holders, token)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_created ON token_rankings (chain_id, created_height, token)
		WHERE created_height IS NOT NULL`,
	`CREATE INDEX IF NOT EXISTS token_rankings_price_time ON token_rankings (chain_id, price_time)`,
	// Symbol search matches by prefix, which a btree serves only with
	// pattern ops under a non-C collation.
	`CREATE INDEX IF NOT EXISTS registry_tokens_symbol_prefix
		ON registry_tokens (chain_id, lower(symbol) text_pattern_ops)`,
}

func (store *Store) LoadRankingCursor(ctx context.Context, chainID uint64) (time.Time, bool, error) {
	var head time.Time
	err := store.pool.QueryRow(ctx,
		`SELECT head_time FROM ranking_checkpoints WHERE chain_id = $1`, int64(chainID)).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("postgres store: load ranking cursor: %w", err)
	}
	return head.UTC(), true, nil
}

func (store *Store) RankedSince(ctx context.Context, chainID uint64, since time.Time) ([]string, error) {
	rows, err := store.pool.Query(ctx,
		`SELECT token FROM token_rankings WHERE chain_id = $1 AND price_time >= $2`, int64(chainID), since)
	if err != nil {
		return nil, fmt.Errorf("postgres store: ranked tokens: %w", err)
	}
	tokens, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("postgres store: read ranked tokens: %w", err)
	}
	return tokens, nil
}

// CommitRankings applies refresh in one transaction. Updates skip rows
// whose values did not change, so a refresh rewrites only what moved.
func (store *Store) CommitRankings(ctx context.Context, refresh storage.RankingRefresh) (err error) {
	if refresh.ChainID == 0 {
		return errors.New("postgres store: ranking chain id is required")
	}
	chainID := int64(refresh.ChainID)
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres store: begin: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, rollback(ctx, tx))
		}
	}()

	batch := &pgx.Batch{}
	queueQuotes(batch, chainID, refresh)
	queueActivity(batch, chainID, refresh.Activity)
	batch.Queue(`DELETE FROM token_rankings WHERE chain_id = $1 AND price_height IS NULL AND trades_24h = 0`, chainID)
	// Holders are counted through holder_balances_top, one index range per
	// ranked token.
	batch.Queue(`UPDATE token_rankings AS r SET holders = c.holders
		FROM (
			SELECT k.token, (SELECT count(*) FROM holder_balances AS b
				WHERE b.chain_id = $1 AND b.token = k.token AND b.balance > 0) AS holders
			FROM token_rankings AS k WHERE k.chain_id = $1
		) AS c
		WHERE r.chain_id = $1 AND r.token = c.token AND r.holders <> c.holders`, chainID)
	batch.Queue(`UPDATE token_rankings AS r SET created_height = d.height, created_time = d.created_at
		FROM (
			SELECT k.token, (t.deployment->>'block_height')::bigint AS height, (t.deployment->>'block_time')::bigint AS created_at
			FROM token_rankings AS k
			LEFT JOIN registry_tokens AS t ON t.chain_id = k.chain_id AND t.address = k.token
			WHERE k.chain_id = $1
		) AS d
		WHERE r.chain_id = $1 AND r.token = d.token
		  AND (r.created_height, r.created_time) IS DISTINCT FROM (d.height, d.created_at)`, chainID)
	refreshedAt := refresh.RefreshedAt
	if refreshedAt.IsZero() {
		refreshedAt = time.Now()
	}
	batch.Queue(`INSERT INTO ranking_checkpoints (chain_id, head_time, refreshed_at) VALUES ($1, $2, $3)
		ON CONFLICT (chain_id) DO UPDATE SET head_time = EXCLUDED.head_time, refreshed_at = EXCLUDED.refreshed_at`,
		chainID, refresh.Head, refreshedAt)

	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("postgres store: commit rankings chain %d: %w", refresh.ChainID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres store: commit: %w", err)
	}
	return nil
}

func queueQuotes(batch *pgx.Batch, chainID int64, refresh storage.RankingRefresh) {
	count := len(refresh.Quotes)
	tokens, prices, liquidity := make([]string, count), make([]string, count), make([]string, count)
	heights, times := make([]int64, count), make([]time.Time, count)
	for index, quote := range refresh.Quotes {
		tokens[index], prices[index], liquidity[index] = quote.Token, quote.PriceUSD.String(), quote.LiquidityUSD.String()
		heights[index], times[index] = quote.BlockHeight, quote.BlockTime
	}
	batch.Queue(`INSERT INTO token_rankings AS r (chain_id, token, price_usd, liquidity_usd, price_height, price_time)
		SELECT $1, token, price::numeric, liquidity::numeric, height, priced_at
		FROM unnest($2::text[], $3::text[], $4::text[], $5::bigint[], $6::timestamptz[]) AS q(token, price, liquidity, height, priced_at)
		ON CONFLICT (chain_id, token) DO UPDATE SET price_usd = EXCLUDED.price_usd,
			liquidity_usd = EXCLUDED.liquidity_usd, price_height = EXCLUDED.price_height, price_time = EXCLUDED.price_time
		WHERE (r.price_usd, r.liquidity_usd, r.price_height, r.price_time)
			IS DISTINCT FROM (EXCLUDED.price_usd, EXCLUDED.liquidity_usd, EXCLUDED.price_height, EXCLUDED.price_time)`,
		chainID, tokens, prices, liquidity, heights, times)
	// A token the scan covered but did not return has no price rows left.
	var since *time.Time
	if !refresh.Since.IsZero() {
		since = &refresh.Since
	}
	batch.Queue(`UPDATE token_rankings
		SET price_usd = NULL, price_height = NULL, price_time = NULL, liquidity_usd = 0
		WHERE chain_id = $1 AND price_height IS NOT NULL
		  AND ($2::timestamptz IS NULL OR price_time >= $2) AND NOT (token = ANY($3::text[]))`,
		chainID, since, tokens)
}

func queueActivity(batch *pgx.Batch, chainID int64, activity []storage.TokenActivity) {
	count := len(activity)
	tokens, volumes, changes := make([]string, count), make([]string, count), make([]string, count)
	trades := make([]int64, count)
	for index, row := range activity {
		tokens[index], volumes[index], trades[index] = row.Token, row.VolumeUSD.String(), row.Trades
		changes[index] = priceChange(row.Open, row.Close).String()
	}
	batch.Queue(`UPDATE token_rankings SET volume_24h_usd = 0, trades_24h = 0, price_change_24h = 0
		WHERE chain_id = $1 AND (volume_24h_usd <> 0 OR trades_24h <> 0 OR price_change_24h <> 0)
		  AND NOT (token = ANY($2::text[]))`, chainID, tokens)
	batch.Queue(`INSERT INTO token_rankings AS r (chain_id, token, volume_24h_usd, trades_24h, price_change_24h)
		SELECT $1, token, volume::numeric, trades, change::numeric
		FROM unnest($2::text[], $3::text[], $4::bigint[], $5::text[]) AS a(token, volume, trades, change)
		ON CONFLICT (chain_id, token) DO UPDATE SET volume_24h_usd = EXCLUDED.volume_24h_usd,
			trades_24h = EXCLUDED.trades_24h, price_change_24h = EXCLUDED.price_change_24h
		WHERE (r.volume_24h_usd, r.trades_24h, r.price_change_24h)
			IS DISTINCT FROM (EXCLUDED.volume_24h_usd, EXCLUDED.trades_24h, EXCLUDED.price_change_24h)`,
		chainID, tokens, volumes, trades, changes)
}

// priceChange is close/open - 1, or zero without an opening price.
func priceChange(open, closePrice decimal.Decimal) decimal.Decimal {
	if !open.IsPositive() {
		return decimal.Zero
	}
	return closePrice.DivRound(open, 18).Sub(decimal.NewFromInt(1))
}

var (
	rankingColumns = map[storage.RankingSort]string{
		storage.SortVolume24h:      "volume_24h_usd",
		storage.SortPriceChange24h: "price_change_24h",
		storage.SortLiquidity:      "liquidity_usd",
		storage.SortHolders:        "holders",
		storage.SortCreated:        "created_height",
	}
	addressPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)
	likeEscaper    = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
)

// TokenList reads a page of the rankings, joined with registry metadata. A
// search starts from registry_tokens instead, so tokens without market data
// are found too and list after the ranked ones.
func (store *Store) TokenList(ctx context.Context, query storage.TokenListQuery) ([]storage.RankedToken, error) {
	column, ok := rankingColumns[query.Sort]
	if !ok || query.Limit <= 0 || query.Offset < 0 {
		return nil, errors.New("postgres store: token list needs a known sort, a limit and a non-negative offset")
	}
	direction := "DESC"
	if query.Ascending {
		direction = "ASC"
	}
	args := []any{int64(query.ChainID), query.MinLiquidityUSD.String(), query.Limit, query.Offset}
	var from, where, order string
	search := strings.ToLower(strings.TrimSpace(query.Search))
	switch {
	case search == "":
		from = `token_rankings AS r LEFT JOIN registry_tokens AS t ON t.chain_id = r.chain_id AND t.address = r.token`
		where = `r.chain_id = $1 AND r.liquidity_usd >= $2::numeric`
		if query.Sort == storage.SortCreated {
			where += ` AND r.created_height IS NOT NULL`
		}
		order = fmt.Sprintf(`r.%s %s, r.token %s`, column, direction, direction)
	case addressPattern.MatchString(search):
		args = append(args, search)
		from = `(SELECT $5::text AS address) AS k
			LEFT JOIN registry_tokens AS t ON t.chain_id = $1 AND t.address = k.address
			LEFT JOIN token_rankings AS r ON r.chain_id = $1 AND r.token = k.address`
		where = `(t.address IS NOT NULL OR r.token IS NOT NULL) AND COALESCE(r.liquidity_usd, 0) >= $2::numeric`
		order = `1`
	default:
		args = append(args, likeEscaper.Replace(search)+"%", search)
		from = `registry_tokens AS t LEFT JOIN token_rankings AS r ON r.chain_id = t.chain_id AND r.token = t.address`
		where = `t.chain_id = $1 AND lower(t.symbol) LIKE $5 AND COALESCE(r.liquidity_usd, 0) >= $2::numeric`
		order = fmt.Sprintf(`lower(t.symbol) = $6 DESC, r.token IS NOT NULL DESC, r.%s %s NULLS LAST, t.address %s`,
			column, direction, direction)
	}
	rows, err := store.pool.Query(ctx, `
		SELECT COALESCE(r.token, t.address), t.name, t.symbol, t.decimals, r.token IS NOT NULL,
			r.price_usd::text, r.price_height, COALESCE(r.price_change_24h, 0)::text, COALESCE(r.volume_24h_usd, 0)::text,
			COALESCE(r.trades_24h, 0), COALESCE(r.liquidity_usd, 0)::text, COALESCE(r.holders, 0),
			r.created_height, r.created_time, c.refreshed_at
		FROM `+from+`
		LEFT JOIN ranking_checkpoints AS c ON c.chain_id = $1
		WHERE `+where+`
		ORDER BY `+order+`
		LIMIT $3 OFFSET $4`, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres store: token list: %w", err)
	}
	tokens, err := pgx.CollectRows(rows, scanRankedToken)
	if err != nil {
		return nil, fmt.Errorf("postgres store: read token list: %w", err)
	}
	return tokens, nil
}

func scanRankedToken(row pgx.CollectableRow) (storage.RankedToken, error) {
	var token storage.RankedToken
	var decimals *int16
	var price *string
	var change, volume, liquidity string
	var refreshedAt *time.Time
	if err := row.Scan(&token.Token, &token.Name, &token.Symbol, &decimals, &token.Ranked,
		&price, &token.PriceHeight, &change, &volume, &token.Trades24h, &liquidity, &token.Holders,
		&token.CreatedHeight, &token.CreatedTime, &refreshedAt); err != nil {
		return token, err
	}
	if decimals != nil {
		value := uint8(*decimals)
		token.Decimals = &value
	}
	if price != nil {
		value, err := decimal.NewFromString(*price)
		if err != nil {
			return token, fmt.Errorf("price of %s: %w", token.Token, err)
		}
		token.PriceUSD = &value
	}
	var err error
	if token.PriceChange24h, err = decimal.NewFromString(change); err != nil {
		return token, fmt.Errorf("price change of %s: %w", token.Token, err)
	}
	if token.Volume24hUSD, err = decimal.NewFromString(volume); err != nil {
		return token, fmt.Errorf("volume of %s: %w", token.Token, err)
	}
	if token.LiquidityUSD, err = decimal.NewFromString(liquidity); err != nil {
		return token, fmt.Errorf("liquidity of %s: %w", token.Token, err)
	}
	if refreshedAt != nil {
		token.RefreshedAt = refreshedAt.UTC()
	}
	return token, nil
}

var (
	_ storage.RankingStore  = (*Store)(nil)
	_ storage.RankingReader = (*Store)(nil)
)
