package conf

import (
	"slices"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultAPIAddr = ":8080"

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
)

// DefaultIntervals are the candle sizes used when none are configured.
func DefaultIntervals() []time.Duration {
	return []time.Duration{
		time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 4 * time.Hour, 24 * time.Hour,
	}
}

// ApplyDefaults fills every section, so readers may use the fields of
// Bootstrap directly, and replaces unset settings with their defaults.
func (bootstrap *Bootstrap) ApplyDefaults() {
	if bootstrap.Kafka == nil {
		bootstrap.Kafka = &config.Kafka{}
	}
	if bootstrap.Source == nil {
		bootstrap.Source = &config.Source{}
	}
	if bootstrap.Storage == nil {
		bootstrap.Storage = &config.Storage{}
	}
	if bootstrap.State == nil {
		bootstrap.State = &config.State{}
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
		bootstrap.Live = &config.Live{}
	}
	if bootstrap.Metrics == nil {
		bootstrap.Metrics = &config.Metrics{}
	}
	bootstrap.Kafka.ApplyDefaults()
	bootstrap.Source.ApplyDefaults()
	bootstrap.Storage.ApplyDefaults()
	bootstrap.State.ApplyDefaults()
	bootstrap.Live.ApplyDefaults()
	config.ApplyChainDefaults(bootstrap.Chains)

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
	config.DefaultDuration(&holders.RpcTimeout, defaultRPCTimeout)

	rankings := bootstrap.Rankings
	config.DefaultDuration(&rankings.RefreshInterval, defaultRankingsRefreshInterval)
	if rankings.CandleInterval.AsDuration() == 0 {
		intervals := market.IntervalDurations()
		candleInterval := slices.Min(intervals)
		if slices.Contains(intervals, defaultRankingsCandleInterval) {
			candleInterval = defaultRankingsCandleInterval
		}
		rankings.CandleInterval = durationpb.New(candleInterval)
	}
	config.DefaultDuration(&rankings.ReorgWindow, defaultRankingsReorgWindow)

	loader := bootstrap.ChLoader
	config.DefaultDuration(&loader.BatchLag, defaultLoaderBatchLag)
	if loader.FlushRows == 0 {
		loader.FlushRows = defaultLoaderFlushRows
	}
	config.DefaultDuration(&loader.FlushInterval, defaultLoaderFlushInterval)

	if strings.TrimSpace(bootstrap.Api.Addr) == "" {
		bootstrap.Api.Addr = defaultAPIAddr
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

// LiveTopics maps chain IDs to their live topics, or returns nil when live
// push is disabled.
func (bootstrap *Bootstrap) LiveTopics() map[uint64]string {
	return config.LiveTopics(bootstrap.GetLive(), bootstrap.GetChains())
}
