package conf

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Validate reports the first invalid setting of a defaulted Bootstrap.
func Validate(bootstrap *Bootstrap) error {
	if bootstrap == nil {
		return errors.New("conf: bootstrap config is required")
	}
	kafka := bootstrap.GetKafka()
	if len(kafka.GetBrokers()) == 0 {
		return errors.New("conf: kafka.brokers is required")
	}
	for _, broker := range kafka.GetBrokers() {
		if strings.TrimSpace(broker) == "" {
			return errors.New("conf: kafka.brokers must not contain empty values")
		}
	}
	if kafka.GetFetchMaxBytes() < 0 || kafka.GetFetchMaxWait().AsDuration() < 0 {
		return errors.New("conf: kafka fetch limits must be positive")
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
	switch bootstrap.GetStorage().GetDriver() {
	case StorageMemory:
	case StorageClickHouse:
		if len(bootstrap.GetStorage().GetClickhouse().GetAddr()) == 0 {
			return errors.New("conf: storage.clickhouse.addr is required")
		}
		if err := validateRetention(bootstrap); err != nil {
			return err
		}
	default:
		return fmt.Errorf("conf: unsupported storage.driver %q", bootstrap.GetStorage().GetDriver())
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
	live := bootstrap.GetLive()
	if live.GetCandleThrottle().AsDuration() < 0 || live.GetClientBuffer() < 0 || live.GetMaxSubscriptions() < 0 {
		return errors.New("conf: live settings must be positive")
	}
	switch bootstrap.GetState().GetDriver() {
	case StateMemory:
	case StatePostgres:
		if strings.TrimSpace(bootstrap.GetState().GetPostgres().GetDsn()) == "" {
			return errors.New("conf: state.postgres.dsn is required")
		}
	default:
		return fmt.Errorf("conf: unsupported state.driver %q", bootstrap.GetState().GetDriver())
	}
	chains := bootstrap.GetChains()
	if len(chains) == 0 {
		return errors.New("conf: at least one chain is required")
	}
	names := make(map[string]struct{}, len(chains))
	ids := make(map[uint64]struct{}, len(chains))
	topics := make(map[string]struct{}, len(chains))
	for index, chain := range chains {
		if strings.TrimSpace(chain.GetName()) == "" || chain.GetChainId() == 0 || strings.TrimSpace(chain.GetTopic()) == "" {
			return fmt.Errorf("conf: chains[%d] needs name, chain_id and topic", index)
		}
		if _, ok := names[chain.Name]; ok {
			return fmt.Errorf("conf: duplicate chain name %q", chain.Name)
		}
		if _, ok := ids[chain.ChainId]; ok {
			return fmt.Errorf("conf: duplicate chain_id %d", chain.ChainId)
		}
		if _, ok := topics[chain.Topic]; ok {
			return fmt.Errorf("conf: duplicate topic %q", chain.Topic)
		}
		names[chain.Name], ids[chain.ChainId], topics[chain.Topic] = struct{}{}, struct{}{}, struct{}{}
		if err := validateFeeds(index, chain); err != nil {
			return err
		}
	}
	return validateLiveTopics(bootstrap)
}

// validateRetention checks that ages are not negative, that the cold tier
// is configured whole, and that candle ages name market intervals and keep
// what market-rankings reads.
func validateRetention(bootstrap *Bootstrap) error {
	retention := bootstrap.GetStorage().GetClickhouse().GetRetention()
	intervals := bootstrap.GetMarket().IntervalDurations()
	ages := []int32{
		retention.GetColdAfterDays(), retention.GetTradesDays(), retention.GetTokenPriceRollupDays(),
		retention.GetCandleCompactDays(), retention.GetRawTransfersDays(), retention.GetRawTradesDays(),
		retention.GetOracleUpdatesDays(),
	}
	for key, age := range retention.GetCandleDays() {
		interval, err := time.ParseDuration(key)
		if err != nil || !slices.Contains(intervals, interval) {
			return fmt.Errorf("conf: storage.clickhouse.retention.candle_days %s is not one of market.intervals", key)
		}
		if interval == bootstrap.GetRankings().GetCandleInterval().AsDuration() && age > 0 && age < minRankingCandleDays {
			return fmt.Errorf("conf: storage.clickhouse.retention.candle_days %s must be at least %d, since rankings sum it over 24h",
				key, minRankingCandleDays)
		}
		ages = append(ages, age)
	}
	if slices.Min(ages) < 0 {
		return errors.New("conf: storage.clickhouse.retention days must not be negative")
	}
	if (strings.TrimSpace(retention.GetStoragePolicy()) == "") != (retention.GetColdAfterDays() == 0) {
		return errors.New("conf: storage.clickhouse.retention.storage_policy and cold_after_days must be set together")
	}
	return nil
}

// validateFeeds checks that each feed names one proxy or aggregator and a
// token, and that no feed prices the same token twice.
func validateFeeds(index int, chain *Chain) error {
	type pair struct{ feed, token string }
	seen := make(map[pair]struct{}, len(chain.GetFeeds()))
	for feedIndex, feed := range chain.GetFeeds() {
		proxy := strings.ToLower(strings.TrimSpace(feed.GetProxy()))
		address := strings.ToLower(strings.TrimSpace(feed.GetAddress()))
		token := strings.ToLower(strings.TrimSpace(feed.GetToken()))
		if (proxy == "") == (address == "") {
			return fmt.Errorf("conf: chains[%d].feeds[%d] needs exactly one of proxy and address", index, feedIndex)
		}
		if token == "" || feed.GetDecimals() > 36 {
			return fmt.Errorf("conf: chains[%d].feeds[%d] needs a token and decimals <= 36", index, feedIndex)
		}
		if proxy != "" && strings.TrimSpace(chain.GetRpcUrl()) == "" {
			return fmt.Errorf("conf: chain %s needs an rpc_url to read feed proxy %s", chain.GetName(), proxy)
		}
		key := pair{feed: proxy + address, token: token}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("conf: chain %s has duplicate feed %s for %s", chain.GetName(), key.feed, token)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// validateLiveTopics checks that no live topic collides with another chain's
// live topic or with any chain topic.
func validateLiveTopics(bootstrap *Bootstrap) error {
	if !bootstrap.GetLive().GetEnabled() {
		return nil
	}
	used := make(map[string]struct{}, 2*len(bootstrap.GetChains()))
	for _, chain := range bootstrap.GetChains() {
		used[chain.Topic] = struct{}{}
	}
	for _, chain := range bootstrap.GetChains() {
		if _, ok := used[chain.LiveTopic]; ok {
			return fmt.Errorf("conf: chain %s live_topic %q is already used", chain.Name, chain.LiveTopic)
		}
		used[chain.LiveTopic] = struct{}{}
	}
	return nil
}
