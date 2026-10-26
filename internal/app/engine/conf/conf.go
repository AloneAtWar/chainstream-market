// Package conf is market-engine's configuration: the Bootstrap proto, its
// defaults and its validation.
package conf

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The drivers of storage and state.
const (
	StorageMemory     = "memory"
	StorageClickHouse = "clickhouse"

	StateMemory   = "memory"
	StatePostgres = "postgres"
)

const (
	defaultMinLiquidityUSD       = 1000
	defaultCandleMinLiquidityUSD = 10000
	defaultMaxHops               = 3
	defaultHistoryDepth          = 256
	defaultOracleDeviation       = 0.02
	defaultOutlierDeviation      = 0.5
	defaultLateTradeBlocks       = 7200
	defaultLateTradesPerToken    = 1000
	defaultBackfillBlocks        = 50400
	defaultBackfillWait          = 30 * time.Second
)

// DefaultIntervals are the candle sizes used when none are configured.
func DefaultIntervals() []time.Duration {
	return []time.Duration{
		time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 4 * time.Hour, 24 * time.Hour,
	}
}

// ProviderSet provides the sections of Bootstrap to the injector. State has
// a provider of its own: wire matches field names regardless of case, so
// wire.FieldsOf finds the protobuf message's internal state field instead.
var ProviderSet = wire.NewSet(
	wire.FieldsOf(new(*Bootstrap), "Source", "Kafka", "Storage", "Market", "Live", "Metrics", "Chains"),
	NewState,
)

// NewState provides the state section.
func NewState(bootstrap *Bootstrap) *State { return bootstrap.GetState() }

// Load reads, defaults and validates the configuration at path, a file or a
// directory of them. The returned func closes the config's sources.
func Load(path string) (*Bootstrap, func() error, error) {
	bootstrap := &Bootstrap{}
	closeConfig, err := config.Load(path, bootstrap)
	if err != nil {
		return nil, nil, err
	}
	return bootstrap, closeConfig, nil
}

// Parse decodes, defaults and validates raw YAML without the environment.
func Parse(raw []byte) (*Bootstrap, error) {
	bootstrap := &Bootstrap{}
	if err := config.Parse(raw, bootstrap); err != nil {
		return nil, err
	}
	return bootstrap, nil
}

// ApplyDefaults fills every section and replaces unset settings with their
// defaults.
func (bootstrap *Bootstrap) ApplyDefaults() {
	if bootstrap.Source == nil {
		bootstrap.Source = &config.Source{}
	}
	if bootstrap.Kafka == nil {
		bootstrap.Kafka = &config.Kafka{}
	}
	if bootstrap.Storage == nil {
		bootstrap.Storage = &Storage{}
	}
	if bootstrap.Storage.Clickhouse == nil {
		bootstrap.Storage.Clickhouse = &config.ClickHouse{}
	}
	if bootstrap.State == nil {
		bootstrap.State = &State{}
	}
	if bootstrap.State.Postgres == nil {
		bootstrap.State.Postgres = &config.Postgres{}
	}
	if bootstrap.Market == nil {
		bootstrap.Market = &Market{}
	}
	if bootstrap.Live == nil {
		bootstrap.Live = &config.Live{}
	}
	if bootstrap.Metrics == nil {
		bootstrap.Metrics = &config.Metrics{}
	}
	bootstrap.Source.ApplyDefaults()
	bootstrap.Kafka.ApplyDefaults()
	bootstrap.Storage.Clickhouse.ApplyDefaults()
	bootstrap.Live.ApplyDefaults()
	config.ApplyChainDefaults(bootstrap.Chains)
	if strings.TrimSpace(bootstrap.Storage.Driver) == "" {
		bootstrap.Storage.Driver = StorageMemory
	}
	if strings.TrimSpace(bootstrap.State.Driver) == "" {
		bootstrap.State.Driver = StateMemory
	}

	market := bootstrap.Market
	if market.MinLiquidityUsd == 0 {
		market.MinLiquidityUsd = defaultMinLiquidityUSD
	}
	if market.CandleMinLiquidityUsd == 0 {
		market.CandleMinLiquidityUsd = defaultCandleMinLiquidityUSD
	}
	if market.MaxHops == 0 {
		market.MaxHops = defaultMaxHops
	}
	if len(market.Intervals) == 0 {
		for _, interval := range DefaultIntervals() {
			market.Intervals = append(market.Intervals, durationpb.New(interval))
		}
	}
	if market.HistoryDepth == 0 {
		market.HistoryDepth = defaultHistoryDepth
	}
	if market.OracleDeviation == 0 {
		market.OracleDeviation = defaultOracleDeviation
	}
	if market.OutlierDeviation == 0 {
		market.OutlierDeviation = defaultOutlierDeviation
	}
	if market.LateTradeBlocks == 0 {
		market.LateTradeBlocks = defaultLateTradeBlocks
	}
	if market.LateTradesPerToken == 0 {
		market.LateTradesPerToken = defaultLateTradesPerToken
	}
	if market.BackfillBlocks == 0 {
		market.BackfillBlocks = defaultBackfillBlocks
	}
	config.DefaultDuration(&market.BackfillWait, defaultBackfillWait)
}

// Validate reports the first invalid setting of a defaulted Bootstrap.
func (bootstrap *Bootstrap) Validate() error {
	if err := bootstrap.GetSource().Validate(); err != nil {
		return err
	}
	live := bootstrap.GetLive()
	if err := live.Validate(); err != nil {
		return err
	}
	needsKafka := bootstrap.GetSource().NeedsKafka() || live.NeedsKafka()
	if err := bootstrap.GetKafka().Validate(needsKafka); err != nil {
		return err
	}
	if err := bootstrap.GetMarket().validate(); err != nil {
		return err
	}
	if err := bootstrap.GetStorage().validate(bootstrap.GetMarket().IntervalDurations()); err != nil {
		return err
	}
	if err := bootstrap.GetState().validate(); err != nil {
		return err
	}
	return config.ValidateChains(bootstrap.GetChains(), live)
}

func (market *Market) validate() error {
	if market.GetMinLiquidityUsd() < 0 || market.GetCandleMinLiquidityUsd() < 0 {
		return errors.New("conf: market liquidity thresholds must not be negative")
	}
	if market.GetMaxHops() > 255 {
		return errors.New("conf: market.max_hops must be at most 255")
	}
	if market.GetOracleDeviation() < 0 {
		return errors.New("conf: market.oracle_deviation must not be negative")
	}
	if market.GetOutlierDeviation() < 0 && market.GetOutlierDeviation() != -1 {
		return errors.New("conf: market.outlier_deviation must be positive or -1")
	}
	if market.GetHistoryDepth() < 0 {
		return errors.New("conf: market.history_depth must be positive")
	}
	if market.GetLateTradeBlocks() < -1 || market.GetLateTradesPerToken() < 0 {
		return errors.New("conf: market.late_trade_blocks must be positive or -1, and late_trades_per_token positive")
	}
	if market.GetBackfillBlocks() < -1 || market.GetBackfillWait().AsDuration() < 0 {
		return errors.New("conf: market.backfill_blocks must be positive or -1, and backfill_wait positive")
	}
	for _, interval := range market.IntervalDurations() {
		if interval < time.Second || interval%time.Second != 0 {
			return fmt.Errorf("conf: market interval %s must be whole seconds", interval)
		}
	}
	return nil
}

// validate checks storage.driver and the settings of the driver it names.
// Candle ages must name one of intervals.
func (storage *Storage) validate(intervals []time.Duration) error {
	switch storage.GetDriver() {
	case StorageMemory:
		return nil
	case StorageClickHouse:
		if len(storage.GetClickhouse().GetAddr()) == 0 {
			return errors.New("conf: storage.clickhouse.addr is required")
		}
		return storage.GetClickhouse().GetRetention().Validate(intervals)
	default:
		return fmt.Errorf("conf: unsupported storage.driver %q", storage.GetDriver())
	}
}

// validate checks state.driver and the settings of the driver it names.
func (state *State) validate() error {
	switch state.GetDriver() {
	case StateMemory:
	case StatePostgres:
		if strings.TrimSpace(state.GetPostgres().GetDsn()) == "" {
			return errors.New("conf: state.postgres.dsn is required")
		}
	default:
		return fmt.Errorf("conf: unsupported state.driver %q", state.GetDriver())
	}
	return nil
}

// IntervalDurations returns the candle intervals.
func (market *Market) IntervalDurations() []time.Duration {
	intervals := make([]time.Duration, len(market.GetIntervals()))
	for index, interval := range market.GetIntervals() {
		intervals[index] = interval.AsDuration()
	}
	return intervals
}

// LiveTopics maps chain IDs to their live topics, or returns nil when live
// push is disabled.
func (bootstrap *Bootstrap) LiveTopics() map[uint64]string {
	return config.LiveTopics(bootstrap.GetLive(), bootstrap.GetChains())
}
