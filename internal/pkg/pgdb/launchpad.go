package pgdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"strconv"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"gorm.io/gorm"
)

// launchpadProgress is a token's progress as a wad: the curve's, or a direct
// launch's pseudo-progress. A direct launch with a launch window moves with
// the chain head through it; one without moves with its fully diluted value
// (price_usd times the launch pool's total supply) up to its protocol's
// target in @targets, a JSON object of USD values, or @default_target. It
// is NULL until the token has a price, decimals and a known supply (LEAST
// would skip the NULL, hence the CASE).
const launchpadProgress = `CASE WHEN l.direct THEN CASE
		WHEN l.launch_end > l.launch_start THEN floor(1e18 * LEAST(1, GREATEST(0,
			(h.head_time - l.launch_start)::numeric / (l.launch_end - l.launch_start))))
		ELSE floor(1e18 * (SELECT CASE WHEN f.value > 1 THEN 1 ELSE f.value END FROM (SELECT
			r.price_usd * s.supply / power(10::numeric, t.decimals)
			/ NULLIF(COALESCE((CAST(@targets AS jsonb) ->> l.protocol)::numeric, CAST(@default_target AS numeric)), 0)
			AS value) AS f))
		END
	ELSE l.progress END`

// launchpadSelect reads launchpad_tokens rows as LaunchpadListing, with
// registry names as a fallback, the quote's decimals, the token's ranking,
// the launch pool's total supply and the chain head time. @chain names the
// chain; launchpadProgress names the rest.
const launchpadSelect = `
	SELECT l.token, l.protocol, l.pool_id, l.manager, l.quote, l.creator,
		COALESCE(l.name, t.name) AS name, COALESCE(l.symbol, t.symbol) AS symbol, t.decimals,
		l.meta_uri, l.status, l.launch_height, l.launch_time, l.launch_seen,
		(` + launchpadProgress + `)::text AS progress, l.base_remaining::text AS base_remaining,
		l.quote_raised::text AS quote_raised, l.price::text AS price,
		l.trades, l.buys, l.sells, l.last_trade_time, l.completed_time, l.migrated_time, l.target_pool_id,
		q.symbol AS quote_symbol, q.decimals AS quote_decimals, r.price_usd::text AS price_usd,
		r.volume_24h_usd::text AS volume_24h_usd, r.holders, h.head_time,
		COALESCE(l.direct, false) AS direct, l.trading_pool_id, l.launch_start, l.launch_end
	FROM launchpad_tokens AS l
	CROSS JOIN (SELECT COALESCE(MAX(block_time), 0) AS head_time FROM launchpad_heads WHERE chain_id = @chain) AS h
	LEFT JOIN registry_tokens AS t ON t.chain_id = l.chain_id AND t.address = l.token
	LEFT JOIN registry_tokens AS q ON q.chain_id = l.chain_id AND q.address = l.quote
	LEFT JOIN token_rankings AS r ON r.chain_id = l.chain_id AND r.token = l.token
	LEFT JOIN LATERAL (
		SELECT CASE WHEN p.metadata ->> 'total_supply' ~ '^[0-9]{1,78}$' THEN (p.metadata ->> 'total_supply')::numeric END AS supply
		FROM registry_pools AS p WHERE l.direct AND p.chain_id = l.chain_id AND p.pool_id = l.pool_id
	) AS s ON true`

// launchpadTargets are the named values launchpadProgress reads.
func launchpadTargets(targets model.LaunchpadTargets) (map[string]any, error) {
	byProtocol := targets.ByProtocol
	if byProtocol == nil {
		byProtocol = map[string]float64{}
	}
	encoded, err := json.Marshal(byProtocol)
	if err != nil {
		return nil, fmt.Errorf("postgres store: launchpad targets: %w", err)
	}
	return map[string]any{"targets": string(encoded), "default_target": strconv.FormatFloat(targets.Default, 'f', -1, 64)}, nil
}

// LaunchpadList reads one launchpad list, see model.LaunchpadQuery. The
// windows count back from the newest block time market-launchpad applied.
// market-api serves it.
//
// LaunchpadList 读取一栏：新币按发射时间倒序；发射中按进度倒序（同进度按最后交易时间），只含
// 发射不超过 MaxAge 且 Stale 内仍有交易的代币；已迁移按迁移时间倒序。时间窗口从
// market-launchpad 应用到的最新区块时间往回算。
func LaunchpadList(ctx context.Context, db *gorm.DB, query model.LaunchpadQuery) ([]model.LaunchpadListing, error) {
	if query.Limit <= 0 {
		return nil, errors.New("postgres store: launchpad list needs a limit")
	}
	minProgress := "0"
	if query.MinProgress != nil {
		minProgress = query.MinProgress.String()
	}
	values, err := launchpadTargets(query.Targets)
	if err != nil {
		return nil, err
	}
	maps.Copy(values, map[string]any{
		"chain": int64(query.ChainID), "protocol": query.Protocol, "limit": query.Limit,
		"max_age": query.MaxAge, "stale": query.Stale, "migrated_age": query.MigratedAge, "min_progress": minProgress,
	})
	var where, order string
	switch query.Column {
	case model.LaunchpadColumnNew:
		where = `l.status = 'bonding' AND l.launch_seen AND l.launch_time >= h.head_time - @max_age`
		order = `l.launch_time DESC, l.token DESC`
	case model.LaunchpadColumnBonding:
		where = `l.status IN ('bonding', 'completed') AND l.launch_time >= h.head_time - @max_age
			AND l.last_trade_time >= h.head_time - @stale
			AND COALESCE(` + launchpadProgress + `, 0) >= CAST(@min_progress AS numeric)`
		order = `(` + launchpadProgress + `) DESC NULLS LAST, l.last_trade_time DESC, l.token DESC`
	case model.LaunchpadColumnMigrated:
		where = `l.status = 'migrated' AND l.migrated_time >= h.head_time - @migrated_age`
		order = `l.migrated_time DESC, l.token DESC`
	default:
		return nil, fmt.Errorf("postgres store: unknown launchpad column %q", query.Column)
	}
	var rows []launchpadRow
	err = db.WithContext(ctx).Raw(launchpadSelect+`
		WHERE l.chain_id = @chain AND (@protocol = '' OR l.protocol = @protocol) AND `+where+`
		ORDER BY `+order+`
		LIMIT @limit`, values).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: launchpad list: %w", err)
	}
	return launchpadListings(rows)
}

// LaunchpadToken reads one launchpad token, with targets for a direct
// launch's pseudo-progress; ok is false when market-launchpad has not seen
// it.
//
// LaunchpadToken 读取单个发射平台代币（targets 用于直发代币的伪进度）；market-launchpad
// 没见过时 ok 为 false。
func LaunchpadToken(
	ctx context.Context, db *gorm.DB, chainID uint64, token string, targets model.LaunchpadTargets,
) (model.LaunchpadListing, bool, error) {
	values, err := launchpadTargets(targets)
	if err != nil {
		return model.LaunchpadListing{}, false, err
	}
	values["chain"], values["token"] = int64(chainID), token
	var rows []launchpadRow
	err = db.WithContext(ctx).Raw(launchpadSelect+`
		WHERE l.chain_id = @chain AND l.token = @token`, values).Scan(&rows).Error
	if err != nil {
		return model.LaunchpadListing{}, false, fmt.Errorf("postgres store: launchpad token: %w", err)
	}
	listings, err := launchpadListings(rows)
	if err != nil || len(listings) == 0 {
		return model.LaunchpadListing{}, false, err
	}
	return listings[0], true, nil
}

// LaunchpadProtocols lists the launchpad platforms market-launchpad has seen
// on a chain, in name order.
//
// LaunchpadProtocols 返回一条链上 market-launchpad 见过的发射平台，按名称排序。
func LaunchpadProtocols(ctx context.Context, db *gorm.DB, chainID uint64) ([]string, error) {
	var protocols []string
	err := db.WithContext(ctx).Raw(`SELECT protocol FROM launchpad_protocols WHERE chain_id = ? ORDER BY protocol`,
		int64(chainID)).Scan(&protocols).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: launchpad protocols: %w", err)
	}
	return protocols, nil
}

// launchpadRow is one launchpadSelect row with its numerics as text.
type launchpadRow struct {
	Token         string
	Protocol      string
	PoolID        string
	Manager       string
	Quote         string
	Creator       *string
	Name          *string
	Symbol        *string
	Decimals      *int16
	MetaURI       *string `gorm:"column:meta_uri"`
	Status        string
	LaunchHeight  int64
	LaunchTime    int64
	LaunchSeen    bool
	Progress      *string
	BaseRemaining *string
	QuoteRaised   *string
	Price         *string
	Trades        int64
	Buys          int64
	Sells         int64
	LastTradeTime *int64
	CompletedTime *int64
	MigratedTime  *int64
	TargetPoolID  *string
	QuoteSymbol   *string
	QuoteDecimals *int16
	PriceUSD      *string `gorm:"column:price_usd"`
	Volume24hUSD  *string `gorm:"column:volume_24h_usd"`
	Holders       *int64
	HeadTime      int64
	Direct        bool
	TradingPoolID *string
	LaunchStart   *int64
	LaunchEnd     *int64
}

func launchpadListings(rows []launchpadRow) ([]model.LaunchpadListing, error) {
	result := make([]model.LaunchpadListing, len(rows))
	for index, row := range rows {
		listing := model.LaunchpadListing{
			Token: row.Token, Protocol: row.Protocol, PoolID: row.PoolID, Manager: row.Manager, Quote: row.Quote,
			Creator: row.Creator, Name: row.Name, Symbol: row.Symbol, MetaURI: row.MetaURI, Status: row.Status,
			LaunchHeight: row.LaunchHeight, LaunchTime: row.LaunchTime, LaunchSeen: row.LaunchSeen,
			Trades: row.Trades, Buys: row.Buys, Sells: row.Sells, LastTradeTime: row.LastTradeTime,
			CompletedTime: row.CompletedTime, MigratedTime: row.MigratedTime, TargetPoolID: row.TargetPoolID,
			PriceUSD: row.PriceUSD, Volume24hUSD: row.Volume24hUSD, Holders: row.Holders, HeadTime: row.HeadTime, QuoteSymbol: row.QuoteSymbol,
			Decimals: smallUint8(row.Decimals), QuoteDecimals: smallUint8(row.QuoteDecimals),
			Direct: row.Direct, TradingPoolID: row.TradingPoolID, LaunchStart: row.LaunchStart, LaunchEnd: row.LaunchEnd,
		}
		var err error
		for _, field := range []struct {
			raw    *string
			target **big.Int
		}{
			{row.Progress, &listing.Progress}, {row.BaseRemaining, &listing.BaseRemaining},
			{row.QuoteRaised, &listing.QuoteRaised}, {row.Price, &listing.Price},
		} {
			if *field.target, err = optionalBig(field.raw); err != nil {
				return nil, fmt.Errorf("postgres store: launchpad token %s: %w", row.Token, err)
			}
		}
		result[index] = listing
	}
	return result, nil
}

func optionalBig(raw *string) (*big.Int, error) {
	if raw == nil {
		return nil, nil
	}
	value, ok := new(big.Int).SetString(*raw, 10)
	if !ok {
		return nil, fmt.Errorf("invalid integer %q", *raw)
	}
	return value, nil
}

func smallUint8(value *int16) *uint8 {
	if value == nil || *value < 0 || *value > 255 {
		return nil
	}
	converted := uint8(*value)
	return &converted
}
