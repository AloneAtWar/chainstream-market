package config

import (
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

// The driver names of the shared sections.
const (
	StorageMemory     = "memory"
	StorageClickHouse = "clickhouse"

	StateMemory   = "memory"
	StatePostgres = "postgres"

	SourceKafka = "kafka"
	SourceFile  = "file"

	LiveKafka = "kafka"
	LiveRedis = "redis"
)

const (
	defaultClickHouseDatabase = "market"
	defaultColdVolume         = "cold"

	defaultFetchMaxBytes = 104857600
	defaultFetchMaxWait  = 500 * time.Millisecond

	defaultSourcePollInterval = 200 * time.Millisecond
	defaultRedisDialTimeout   = 5 * time.Second

	defaultLiveMaxLag           = 10 * time.Minute
	defaultLiveCandleThrottle   = 250 * time.Millisecond
	defaultLiveClientBuffer     = 1024
	defaultLiveMaxSubscriptions = 200
	liveTopicSuffix             = "-live"
)

// DefaultDuration sets *value to fallback when it is unset or zero.
func DefaultDuration(value **durationpb.Duration, fallback time.Duration) {
	if (*value).AsDuration() == 0 {
		*value = durationpb.New(fallback)
	}
}

// ApplyDefaults fills the kafka client settings.
func (kafka *Kafka) ApplyDefaults() {
	if kafka.FetchMaxBytes == 0 {
		kafka.FetchMaxBytes = defaultFetchMaxBytes
	}
	DefaultDuration(&kafka.FetchMaxWait, defaultFetchMaxWait)
}

// ApplyDefaults fills the source section and defaults its driver to kafka.
func (source *Source) ApplyDefaults() {
	if source.File == nil {
		source.File = &FileSource{}
	}
	if strings.TrimSpace(source.Driver) == "" {
		source.Driver = SourceKafka
	}
	DefaultDuration(&source.File.PollInterval, defaultSourcePollInterval)
}

// ApplyDefaults fills the storage section and defaults its driver to memory.
func (storage *Storage) ApplyDefaults() {
	if storage.Clickhouse == nil {
		storage.Clickhouse = &ClickHouse{}
	}
	if strings.TrimSpace(storage.Driver) == "" {
		storage.Driver = StorageMemory
	}
	if storage.Driver == StorageClickHouse {
		storage.Clickhouse.ApplyDefaults()
	} else {
		storage.Clickhouse.applyRetentionDefaults()
	}
}

// ApplyDefaults fills the clickhouse section and its retention.
func (clickhouse *ClickHouse) ApplyDefaults() {
	if clickhouse.Database == "" {
		clickhouse.Database = defaultClickHouseDatabase
	}
	clickhouse.applyRetentionDefaults()
}

func (clickhouse *ClickHouse) applyRetentionDefaults() {
	if clickhouse.Retention == nil {
		clickhouse.Retention = &Retention{}
	}
	if clickhouse.Retention.ColdVolume == "" {
		clickhouse.Retention.ColdVolume = defaultColdVolume
	}
}

// ApplyDefaults fills the state section and defaults its driver to memory.
func (state *State) ApplyDefaults() {
	if state.Postgres == nil {
		state.Postgres = &Postgres{}
	}
	if strings.TrimSpace(state.Driver) == "" {
		state.Driver = StateMemory
	}
}

// ApplyDefaults fills the live section and defaults its driver to kafka.
func (live *Live) ApplyDefaults() {
	if live.Redis == nil {
		live.Redis = &Redis{}
	}
	if strings.TrimSpace(live.Driver) == "" {
		live.Driver = LiveKafka
	}
	DefaultDuration(&live.Redis.DialTimeout, defaultRedisDialTimeout)
	DefaultDuration(&live.MaxLag, defaultLiveMaxLag)
	DefaultDuration(&live.CandleThrottle, defaultLiveCandleThrottle)
	if live.ClientBuffer == 0 {
		live.ClientBuffer = defaultLiveClientBuffer
	}
	if live.MaxSubscriptions == 0 {
		live.MaxSubscriptions = defaultLiveMaxSubscriptions
	}
}

// ApplyChainDefaults names each chain's live topic after its topic.
func ApplyChainDefaults(chains []*Chain) {
	for _, chain := range chains {
		if strings.TrimSpace(chain.LiveTopic) == "" && chain.Topic != "" {
			chain.LiveTopic = chain.Topic + liveTopicSuffix
		}
	}
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
func LiveTopics(live *Live, chains []*Chain) map[uint64]string {
	if !live.GetEnabled() {
		return nil
	}
	topics := make(map[uint64]string, len(chains))
	for _, chain := range chains {
		topics[chain.GetChainId()] = chain.GetLiveTopic()
	}
	return topics
}
