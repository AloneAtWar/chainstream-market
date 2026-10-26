package conf

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
)

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
	market := bootstrap.GetMarket()
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
	intervals := market.IntervalDurations()
	for _, interval := range intervals {
		if interval < time.Second || interval%time.Second != 0 {
			return fmt.Errorf("conf: market interval %s must be whole seconds", interval)
		}
	}
	if err := bootstrap.GetStorage().Validate(); err != nil {
		return err
	}
	if bootstrap.GetStorage().GetDriver() == config.StorageClickHouse {
		retention := bootstrap.GetStorage().GetClickhouse().GetRetention()
		if err := retention.Validate(intervals); err != nil {
			return err
		}
		if err := validateRankingCandleDays(retention, bootstrap.GetRankings()); err != nil {
			return err
		}
	}
	holders := bootstrap.GetHolders()
	if holders.GetHistoryDepth() < 0 || holders.GetReconcileBatch() < 0 ||
		holders.GetRpcBatchSize() < 0 || holders.GetRpcTimeout().AsDuration() < 0 {
		return errors.New("conf: holders settings must be positive")
	}
	if bootstrap.GetRegistry().GetHistoryDepth() < 0 {
		return errors.New("conf: registry.history_depth must be positive")
	}
	rankings := bootstrap.GetRankings()
	if rankings.GetRefreshInterval().AsDuration() < 0 || rankings.GetReorgWindow().AsDuration() < 0 {
		return errors.New("conf: rankings durations must be positive")
	}
	candleInterval := rankings.GetCandleInterval().AsDuration()
	if !slices.Contains(intervals, candleInterval) || candleInterval > 24*time.Hour {
		return fmt.Errorf("conf: rankings.candle_interval %s must be one of market.intervals and at most 24h", candleInterval)
	}
	loader := bootstrap.GetChLoader()
	if loader.GetBatchLag().AsDuration() < 0 || loader.GetFlushRows() < 0 || loader.GetFlushInterval().AsDuration() < 0 {
		return errors.New("conf: ch_loader settings must be positive")
	}
	if err := bootstrap.GetState().Validate(); err != nil {
		return err
	}
	return config.ValidateChains(bootstrap.GetChains(), live)
}

// validateRankingCandleDays checks that the retention of the candle interval
// market-rankings reads keeps the buckets it sums over 24h.
func validateRankingCandleDays(retention *config.Retention, rankings *Rankings) error {
	for interval, days := range retention.CandleDayDurations() {
		if interval == rankings.GetCandleInterval().AsDuration() && days > 0 && days < minRankingCandleDays {
			return fmt.Errorf("conf: storage.clickhouse.retention.candle_days %s must be at least %d, since rankings sum it over 24h",
				interval, minRankingCandleDays)
		}
	}
	return nil
}
