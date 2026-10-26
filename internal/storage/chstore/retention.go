package chstore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Retention ages rows out of the tables by their block time, or a candle's
// open time. Zero days keeps rows. Every fact table is partitioned by month,
// and the trade and raw tables drop a month's parts whole once its newest
// row has expired, so rows outlive their age by up to a month.
type Retention struct {
	// StoragePolicy is given to each table that ColdAfterDays moves. It must
	// contain a volume named "default", the volume of a table created
	// without a policy, and ColdVolume. ClickHouse cannot take it back.
	StoragePolicy string
	ColdVolume    string
	// ColdAfterDays moves parts whose newest row is this old to ColdVolume.
	ColdAfterDays int

	// TradesDays deletes market-engine's trades.
	TradesDays int
	// TokenPriceRollupDays collapses older token_prices rows into the
	// latest row per token in each part, which merges leave at about one per
	// token and month, so every token keeps its last price.
	TokenPriceRollupDays int
	// CandleCompactDays keeps only the last version of older candle
	// buckets. It must reach past the deepest reorg, since a rewind can only
	// restore versions that are still stored.
	CandleCompactDays int
	// CandleDays deletes candles of an interval; intervals left out stay.
	CandleDays map[time.Duration]int

	// RawTransfersDays, RawTradesDays and OracleUpdatesDays delete
	// ch-loader's raw facts. Asset and pool facts stay.
	RawTransfersDays  int
	RawTradesDays     int
	OracleUpdatesDays int
}

// Validate reports a negative age or a cold tier configured by halves.
func (retention Retention) Validate() error {
	days := []int{
		retention.ColdAfterDays, retention.TradesDays, retention.TokenPriceRollupDays, retention.CandleCompactDays,
		retention.RawTransfersDays, retention.RawTradesDays, retention.OracleUpdatesDays,
	}
	for interval, age := range retention.CandleDays {
		if interval < time.Second || interval%time.Second != 0 {
			return fmt.Errorf("clickhouse retention: candle interval %s must be whole seconds", interval)
		}
		days = append(days, age)
	}
	if slices.Min(days) < 0 {
		return errors.New("clickhouse retention: days must not be negative")
	}
	if (retention.StoragePolicy == "") != (retention.ColdAfterDays == 0) {
		return errors.New("clickhouse retention: storage policy and cold after days go together")
	}
	if retention.ColdAfterDays > 0 && (!identifier.MatchString(retention.StoragePolicy) || !identifier.MatchString(retention.ColdVolume)) {
		return fmt.Errorf("clickhouse retention: invalid storage policy %q or cold volume %q",
			retention.StoragePolicy, retention.ColdVolume)
	}
	return nil
}

// TableTTL is one table's TTL rules in ClickHouse syntax.
type TableTTL struct {
	Table string
	Rules []string
}

func days(column string, age int) string {
	return fmt.Sprintf("%s + toIntervalDay(%d)", column, age)
}

// deleteRules returns a DELETE rule after deleteDays and, when it comes
// sooner, a move to the cold volume.
func (retention Retention) deleteRules(column string, deleteDays int) []string {
	var rules []string
	if retention.ColdAfterDays > 0 && (deleteDays == 0 || retention.ColdAfterDays < deleteDays) {
		rules = append(rules, retention.coldRule(column))
	}
	if deleteDays > 0 {
		rules = append(rules, days(column, deleteDays))
	}
	return rules
}

func (retention Retention) coldRule(column string) string {
	return days(column, retention.ColdAfterDays) + " TO VOLUME '" + retention.ColdVolume + "'"
}

// latest sets each column to its value at the highest version.
func latest(version string, columns ...string) string {
	sets := make([]string, 0, len(columns)+1)
	for _, column := range columns {
		sets = append(sets, fmt.Sprintf("%s = argMax(%s, %s)", column, column, version))
	}
	return strings.Join(append(sets, fmt.Sprintf("%s = max(%s)", version, version)), ", ")
}

// EngineTTL returns the rules for market-engine's tables. blocks keeps
// every row: it is small and holds each chain's head.
func (retention Retention) EngineTTL() []TableTTL {
	prices := retention.deleteRules("block_time", 0)
	if retention.TokenPriceRollupDays > 0 {
		prices = append(prices, days("block_time", retention.TokenPriceRollupDays)+
			" GROUP BY chain_id, token SET block_time = max(block_time), "+
			latest("block_height", "price_usd", "source", "hops", "liquidity_usd", "revision_height"))
	}
	candles := retention.deleteRules("open_time", 0)
	if retention.CandleCompactDays > 0 {
		candles = append(candles, days("open_time", retention.CandleCompactDays)+
			" GROUP BY chain_id, subject, key, interval_seconds, open_time SET "+
			latest("update_height", "open", "high", "low", "close", "volume_base", "volume_usd", "trades"))
	}
	intervals := make([]time.Duration, 0, len(retention.CandleDays))
	for interval, age := range retention.CandleDays {
		if age > 0 {
			intervals = append(intervals, interval)
		}
	}
	slices.Sort(intervals)
	for _, interval := range intervals {
		candles = append(candles, fmt.Sprintf("%s WHERE interval_seconds = %d",
			days("open_time", retention.CandleDays[interval]), interval/time.Second))
	}
	return []TableTTL{
		{"trades", retention.deleteRules("block_time", retention.TradesDays)},
		{"token_prices", prices},
		{"candles", candles},
	}
}

// RawTTL returns the rules for ch-loader's tables.
func (retention Retention) RawTTL() []TableTTL {
	return []TableTTL{
		{"raw_transfers", retention.deleteRules("block_time", retention.RawTransfersDays)},
		{"raw_trades", retention.deleteRules("block_time", retention.RawTradesDays)},
		{"oracle_updates", retention.deleteRules("block_time", retention.OracleUpdatesDays)},
		{"asset_facts", retention.deleteRules("block_time", 0)},
		{"pool_facts", retention.deleteRules("block_time", 0)},
	}
}

// ApplyRetention sets the TTL of market-engine's tables. Only market-engine
// calls it, so readers that open the store leave the TTL alone.
func (store *Store) ApplyRetention(ctx context.Context, retention Retention) error {
	return ApplyTTL(ctx, store.conn, retention, retention.EngineTTL())
}

// tableTTL finds the TTL clause in a table's CREATE statement, which
// ClickHouse writes before SETTINGS.
var tableTTL = regexp.MustCompile(`\) TTL (.*?)(?: SETTINGS |$)`)

// ApplyTTL gives each table retention's storage policy and its TTL rules,
// or removes its TTL when it has no rules. A table whose TTL already matches
// is left alone: changing a TTL rewrites the table's parts in the background.
func ApplyTTL(ctx context.Context, conn driver.Conn, retention Retention, ttls []TableTTL) error {
	if err := retention.Validate(); err != nil {
		return err
	}
	for _, ttl := range ttls {
		var create, policy string
		if err := conn.QueryRow(ctx, `SELECT create_table_query, storage_policy FROM system.tables
			WHERE database = currentDatabase() AND name = ?`, ttl.Table).Scan(&create, &policy); err != nil {
			return fmt.Errorf("clickhouse retention: read %s: %w", ttl.Table, err)
		}
		if retention.StoragePolicy != "" && policy != retention.StoragePolicy {
			if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s MODIFY SETTING storage_policy = '%s'",
				ttl.Table, retention.StoragePolicy)); err != nil {
				return fmt.Errorf("clickhouse retention: set %s storage policy: %w", ttl.Table, err)
			}
		}
		current := ""
		if match := tableTTL.FindStringSubmatch(create); match != nil {
			current = match[1]
		}
		want := strings.Join(ttl.Rules, ", ")
		same, err := sameTTL(ctx, conn, current, want)
		if err != nil {
			return fmt.Errorf("clickhouse retention: compare %s TTL: %w", ttl.Table, err)
		}
		if same {
			continue
		}
		statement := "ALTER TABLE " + ttl.Table + " REMOVE TTL"
		if want != "" {
			statement = "ALTER TABLE " + ttl.Table + " MODIFY TTL " + want
		}
		if err := conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("clickhouse retention: set %s TTL: %w", ttl.Table, err)
		}
	}
	return nil
}

// sameTTL compares two TTL clauses as ClickHouse formats them, since a
// stored clause reads differently from the one that set it.
func sameTTL(ctx context.Context, conn driver.Conn, current, want string) (bool, error) {
	if current == "" || want == "" {
		return current == want, nil
	}
	var same bool
	err := conn.QueryRow(ctx, `SELECT formatQuerySingleLine(?) = formatQuerySingleLine(?)`,
		"ALTER TABLE t MODIFY TTL "+current, "ALTER TABLE t MODIFY TTL "+want).Scan(&same)
	return same, err
}
