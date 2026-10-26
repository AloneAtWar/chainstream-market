package conf

import (
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	StorageMemory     = "memory"
	StorageClickHouse = "clickhouse"

	StateMemory   = "memory"
	StatePostgres = "postgres"

	SourceKafka = "kafka"
	SourceFile  = "file"

	LiveKafka = "kafka"
	LiveRedis = "redis"

	defaultClickHouseDatabase = "market"
	defaultAPIAddr            = ":8080"
	defaultColdVolume         = "cold"

	defaultFetchMaxBytes = 104857600
	defaultFetchMaxWait  = 500 * time.Millisecond

	defaultSourcePollInterval = 200 * time.Millisecond
	defaultRedisDialTimeout   = 5 * time.Second

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

	defaultHoldersReconcileEvery = 100
	defaultHoldersReconcileBatch = 200
	defaultRPCBatchSize          = 100
	defaultRPCTimeout            = 30 * time.Second

	defaultRankingsRefreshInterval = time.Minute
	defaultRankingsCandleInterval  = 5 * time.Minute
	defaultRankingsReorgWindow     = time.Hour
	// minRankingCandleDays keeps the candles market-rankings sums over 24h.
	minRankingCandleDays = 2

	defaultLoaderBatchLag      = 5 * time.Minute
	defaultLoaderFlushRows     = 100000
	defaultLoaderFlushInterval = 2 * time.Second

	defaultLiveMaxLag           = 10 * time.Minute
	defaultLiveCandleThrottle   = 250 * time.Millisecond
	defaultLiveClientBuffer     = 1024
	defaultLiveMaxSubscriptions = 200
	liveTopicSuffix             = "-live"
)

// DefaultIntervals are the candle sizes used when none are configured.
func DefaultIntervals() []time.Duration {
	return []time.Duration{
		time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 4 * time.Hour, 24 * time.Hour,
	}
}

// applyDefaults fills every section, so readers may use the fields of
// Bootstrap directly, and replaces unset settings with their defaults.
func (bootstrap *Bootstrap) applyDefaults() {
	if bootstrap.Kafka == nil {
		bootstrap.Kafka = &Kafka{}
	}
	if bootstrap.Source == nil {
		bootstrap.Source = &Source{}
	}
	if bootstrap.Source.File == nil {
		bootstrap.Source.File = &FileSource{}
	}
	if bootstrap.Storage == nil {
		bootstrap.Storage = &Storage{}
	}
	if bootstrap.Storage.Clickhouse == nil {
		bootstrap.Storage.Clickhouse = &ClickHouse{}
	}
	if bootstrap.Storage.Clickhouse.Retention == nil {
		bootstrap.Storage.Clickhouse.Retention = &Retention{}
	}
	if bootstrap.State == nil {
		bootstrap.State = &State{}
	}
	if bootstrap.State.Postgres == nil {
		bootstrap.State.Postgres = &Postgres{}
	}
	if bootstrap.Market == nil {
		bootstrap.Market = &Market{}
	}
	if bootstrap.Api == nil {
		bootstrap.Api = &API{}
	}
	if bootstrap.Holders == nil {
		bootstrap.Holders = &Holders{}
	}
	if bootstrap.Registry == nil {
		bootstrap.Registry = &Registry{}
	}
	if bootstrap.Rankings == nil {
		bootstrap.Rankings = &Rankings{}
	}
	if bootstrap.ChLoader == nil {
		bootstrap.ChLoader = &ChLoader{}
	}
	if bootstrap.Live == nil {
		bootstrap.Live = &Live{}
	}
	if bootstrap.Live.Redis == nil {
		bootstrap.Live.Redis = &Redis{}
	}
	if bootstrap.Metrics == nil {
		bootstrap.Metrics = &Metrics{}
	}

	kafka := bootstrap.Kafka
	if kafka.FetchMaxBytes == 0 {
		kafka.FetchMaxBytes = defaultFetchMaxBytes
	}
	defaultDuration(&kafka.FetchMaxWait, defaultFetchMaxWait)

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
	defaultDuration(&market.BackfillWait, defaultBackfillWait)

	if strings.TrimSpace(bootstrap.Storage.Driver) == "" {
		bootstrap.Storage.Driver = StorageMemory
	}
	holders := bootstrap.Holders
	if holders.HistoryDepth == 0 {
		holders.HistoryDepth = defaultHistoryDepth
	}
	if bootstrap.Registry.HistoryDepth == 0 {
		bootstrap.Registry.HistoryDepth = defaultHistoryDepth
	}
	if holders.ReconcileEvery == 0 {
		holders.ReconcileEvery = defaultHoldersReconcileEvery
	}
	if holders.ReconcileBatch == 0 {
		holders.ReconcileBatch = defaultHoldersReconcileBatch
	}
	if holders.RpcBatchSize == 0 {
		holders.RpcBatchSize = defaultRPCBatchSize
	}
	defaultDuration(&holders.RpcTimeout, defaultRPCTimeout)

	rankings := bootstrap.Rankings
	defaultDuration(&rankings.RefreshInterval, defaultRankingsRefreshInterval)
	if rankings.CandleInterval.AsDuration() == 0 {
		intervals := market.IntervalDurations()
		candleInterval := slices.Min(intervals)
		if slices.Contains(intervals, defaultRankingsCandleInterval) {
			candleInterval = defaultRankingsCandleInterval
		}
		rankings.CandleInterval = durationpb.New(candleInterval)
	}
	defaultDuration(&rankings.ReorgWindow, defaultRankingsReorgWindow)

	loader := bootstrap.ChLoader
	defaultDuration(&loader.BatchLag, defaultLoaderBatchLag)
	if loader.FlushRows == 0 {
		loader.FlushRows = defaultLoaderFlushRows
	}
	defaultDuration(&loader.FlushInterval, defaultLoaderFlushInterval)

	if strings.TrimSpace(bootstrap.Source.Driver) == "" {
		bootstrap.Source.Driver = SourceKafka
	}
	defaultDuration(&bootstrap.Source.File.PollInterval, defaultSourcePollInterval)

	live := bootstrap.Live
	if strings.TrimSpace(live.Driver) == "" {
		live.Driver = LiveKafka
	}
	defaultDuration(&live.Redis.DialTimeout, defaultRedisDialTimeout)
	defaultDuration(&live.MaxLag, defaultLiveMaxLag)
	defaultDuration(&live.CandleThrottle, defaultLiveCandleThrottle)
	if live.ClientBuffer == 0 {
		live.ClientBuffer = defaultLiveClientBuffer
	}
	if live.MaxSubscriptions == 0 {
		live.MaxSubscriptions = defaultLiveMaxSubscriptions
	}
	for _, chain := range bootstrap.Chains {
		if strings.TrimSpace(chain.LiveTopic) == "" && chain.Topic != "" {
			chain.LiveTopic = chain.Topic + liveTopicSuffix
		}
	}
	if strings.TrimSpace(bootstrap.State.Driver) == "" {
		bootstrap.State.Driver = StateMemory
	}
	if strings.TrimSpace(bootstrap.Api.Addr) == "" {
		bootstrap.Api.Addr = defaultAPIAddr
	}
	clickhouse := bootstrap.Storage.Clickhouse
	if bootstrap.Storage.Driver == StorageClickHouse && clickhouse.Database == "" {
		clickhouse.Database = defaultClickHouseDatabase
	}
	if clickhouse.Retention.ColdVolume == "" {
		clickhouse.Retention.ColdVolume = defaultColdVolume
	}
}

// defaultDuration sets *value to fallback when it is unset or zero.
func defaultDuration(value **durationpb.Duration, fallback time.Duration) {
	if (*value).AsDuration() == 0 {
		*value = durationpb.New(fallback)
	}
}

// IntervalDurations returns the candle intervals.
func (market *Market) IntervalDurations() []time.Duration {
	intervals := make([]time.Duration, len(market.GetIntervals()))
	for index, interval := range market.GetIntervals() {
		intervals[index] = interval.AsDuration()
	}
	return intervals
}

// CandleDayDurations returns candle_days keyed by interval. Validate has
// checked that every key parses.
func (retention *Retention) CandleDayDurations() map[time.Duration]int {
	days := make(map[time.Duration]int, len(retention.GetCandleDays()))
	for key, age := range retention.GetCandleDays() {
		if interval, err := time.ParseDuration(key); err == nil {
			days[interval] = int(age)
		}
	}
	return days
}

// LiveTopics maps chain IDs to their live topics, or returns nil when live
// push is disabled.
func (bootstrap *Bootstrap) LiveTopics() map[uint64]string {
	if !bootstrap.GetLive().GetEnabled() {
		return nil
	}
	topics := make(map[uint64]string, len(bootstrap.Chains))
	for _, chain := range bootstrap.Chains {
		topics[chain.ChainId] = chain.LiveTopic
	}
	return topics
}
