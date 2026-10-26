package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Validate checks the brokers and fetch limits. A component whose transports
// read from kafka passes required, which rejects an empty broker list.
func (kafka *Kafka) Validate(required bool) error {
	if required && len(kafka.GetBrokers()) == 0 {
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
	return nil
}

// Validate checks source.driver and the settings of the driver it names.
func (source *Source) Validate() error {
	switch source.GetDriver() {
	case SourceKafka:
	case SourceFile:
		if strings.TrimSpace(source.GetFile().GetDir()) == "" {
			return errors.New("conf: source.file.dir is required")
		}
		if source.GetFile().GetPollInterval().AsDuration() < 0 {
			return errors.New("conf: source.file.poll_interval must be positive")
		}
	default:
		return fmt.Errorf("conf: unsupported source.driver %q", source.GetDriver())
	}
	return nil
}

// NeedsKafka reports whether chain topics are read from the kafka brokers.
func (source *Source) NeedsKafka() bool { return source.GetDriver() == SourceKafka }

// Validate checks live.driver, the settings of the driver it names and the
// push limits.
func (live *Live) Validate() error {
	switch live.GetDriver() {
	case LiveKafka:
	case LiveRedis:
		if live.GetEnabled() && strings.TrimSpace(live.GetRedis().GetAddr()) == "" {
			return errors.New("conf: live.redis.addr is required")
		}
		if live.GetRedis().GetDatabase() < 0 || live.GetRedis().GetDialTimeout().AsDuration() < 0 {
			return errors.New("conf: live.redis settings must be positive")
		}
	default:
		return fmt.Errorf("conf: unsupported live.driver %q", live.GetDriver())
	}
	if live.GetCandleThrottle().AsDuration() < 0 || live.GetClientBuffer() < 0 || live.GetMaxSubscriptions() < 0 {
		return errors.New("conf: live settings must be positive")
	}
	return nil
}

// NeedsKafka reports whether live updates are carried by the kafka brokers.
func (live *Live) NeedsKafka() bool {
	return live.GetEnabled() && live.GetDriver() == LiveKafka
}

// Validate checks storage.driver and the settings of the driver it names.
func (storage *Storage) Validate() error {
	switch storage.GetDriver() {
	case StorageMemory:
	case StorageClickHouse:
		if len(storage.GetClickhouse().GetAddr()) == 0 {
			return errors.New("conf: storage.clickhouse.addr is required")
		}
	default:
		return fmt.Errorf("conf: unsupported storage.driver %q", storage.GetDriver())
	}
	return nil
}

// Validate checks that the clickhouse section names a server.
func (clickhouse *ClickHouse) Validate() error {
	if len(clickhouse.GetAddr()) == 0 {
		return errors.New("conf: clickhouse.addr is required")
	}
	return nil
}

// Validate checks that the postgres section names a database.
func (postgres *Postgres) Validate() error {
	if strings.TrimSpace(postgres.GetDsn()) == "" {
		return errors.New("conf: postgres.dsn is required")
	}
	return nil
}

// Validate checks state.driver and the settings of the driver it names.
func (state *State) Validate() error {
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

// Validate checks that no age is negative, that the cold tier is configured
// whole, and that every candle age names one of intervals. A component
// without candles passes nil intervals, and its candle ages are not checked.
func (retention *Retention) Validate(intervals []time.Duration) error {
	ages := []int32{
		retention.GetColdAfterDays(), retention.GetTradesDays(), retention.GetTokenPriceRollupDays(),
		retention.GetCandleCompactDays(), retention.GetRawTransfersDays(), retention.GetRawTradesDays(),
		retention.GetOracleUpdatesDays(),
	}
	for key, age := range retention.GetCandleDays() {
		interval, err := time.ParseDuration(key)
		if intervals != nil && (err != nil || !slices.Contains(intervals, interval)) {
			return fmt.Errorf("conf: storage.clickhouse.retention.candle_days %s is not one of market.intervals", key)
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

// ValidateChains checks that there is at least one chain, that each names a
// topic and a unique id, and that their feeds and live topics are sound.
func ValidateChains(chains []*Chain, live *Live) error {
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
		if _, ok := names[chain.GetName()]; ok {
			return fmt.Errorf("conf: duplicate chain name %q", chain.GetName())
		}
		if _, ok := ids[chain.GetChainId()]; ok {
			return fmt.Errorf("conf: duplicate chain_id %d", chain.GetChainId())
		}
		if _, ok := topics[chain.GetTopic()]; ok {
			return fmt.Errorf("conf: duplicate topic %q", chain.GetTopic())
		}
		names[chain.GetName()], ids[chain.GetChainId()], topics[chain.GetTopic()] = struct{}{}, struct{}{}, struct{}{}
		if err := validateFeeds(index, chain); err != nil {
			return err
		}
	}
	return validateLiveTopics(chains, live)
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
func validateLiveTopics(chains []*Chain, live *Live) error {
	if !live.GetEnabled() {
		return nil
	}
	used := make(map[string]struct{}, 2*len(chains))
	for _, chain := range chains {
		used[chain.GetTopic()] = struct{}{}
	}
	for _, chain := range chains {
		if _, ok := used[chain.GetLiveTopic()]; ok {
			return fmt.Errorf("conf: chain %s live_topic %q is already used", chain.GetName(), chain.GetLiveTopic())
		}
		used[chain.GetLiveTopic()] = struct{}{}
	}
	return nil
}
