package pgdb

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// RankingCheckpointRow is a ranking_checkpoints row.
type RankingCheckpointRow struct {
	ChainID     int64 `gorm:"primaryKey;autoIncrement:false"`
	HeadTime    time.Time
	RefreshedAt time.Time
}

func (RankingCheckpointRow) TableName() string { return "ranking_checkpoints" }

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

// TokenList reads a page of the rankings market-rankings keeps, joined with
// registry metadata. A search starts from registry_tokens instead, so tokens
// without market data are found too and list after the ranked ones.
// market-api serves it.
func TokenList(ctx context.Context, db *gorm.DB, query model.TokenListQuery) ([]model.RankedToken, error) {
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
	err := db.WithContext(ctx).Raw(`
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
