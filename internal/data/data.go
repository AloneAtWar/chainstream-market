// Package data opens the stores and clients the commands share. Stores are
// opened by the driver the config names and provided by role, see
// storage.go. A provider whose resource the config turns off returns nil, as
// in chainstream-evm.
package data

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/live"
	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/google/wire"
)

// ProviderSet is the data-layer provider set.
var ProviderSet = wire.NewSet(
	NewMarketBackend,
	NewRawBackend,
	NewStateBackend,
	NewOutput,
	NewReader,
	NewRankingSource,
	NewRawStore,
	NewTransferReader,
	NewHistory,
	NewStateStore,
	NewHolderStore,
	NewRegistryStore,
	NewRankingStore,
	NewStateReaders,
	NewMemory,
	NewSources,
	NewPublisher,
	NewHub,
)

// NewMemory keeps market-engine's checkpoints when state.driver keeps no
// state.
func NewMemory() *storage.Memory { return storage.NewMemory() }

// Sources opens chain topics through the driver source.driver names.
type Sources struct {
	open func(topic string, start int64) (source.Source, error)
}

// sourceDrivers open a chain topic at an offset, by source.driver.
var sourceDrivers = map[string]func(bootstrap *conf.Bootstrap) func(topic string, start int64) (source.Source, error){
	conf.SourceKafka: func(bootstrap *conf.Bootstrap) func(string, int64) (source.Source, error) {
		kafka := bootstrap.GetKafka()
		options := source.KafkaOptions{
			Brokers: kafka.GetBrokers(), ClientID: kafka.GetClientId(),
			FetchMaxBytes: kafka.GetFetchMaxBytes(), FetchMaxWait: kafka.GetFetchMaxWait().AsDuration(),
		}
		return func(topic string, start int64) (source.Source, error) {
			topicOptions := options
			topicOptions.Topic = topic
			return source.NewKafka(topicOptions, start)
		}
	},
	conf.SourceFile: func(bootstrap *conf.Bootstrap) func(string, int64) (source.Source, error) {
		file := bootstrap.GetSource().GetFile()
		return func(topic string, start int64) (source.Source, error) {
			return source.NewFile(source.FileOptions{
				Path: filepath.Join(file.GetDir(), topic+".jsonl"), PollInterval: file.GetPollInterval().AsDuration(),
			}, start)
		}
	},
}

// NewSources reads chain topics with the driver source.driver names.
func NewSources(bootstrap *conf.Bootstrap) (*Sources, error) {
	driver := bootstrap.GetSource().GetDriver()
	open, ok := sourceDrivers[driver]
	if !ok {
		return nil, fmt.Errorf("source.driver %q is not supported; use one of: %s", driver, driverNames(sourceDrivers))
	}
	return &Sources{open: open(bootstrap)}, nil
}

// Opener returns the engine's opener of topic.
func (sources *Sources) Opener(topic string) func(start int64) (source.Source, error) {
	return func(start int64) (source.Source, error) { return sources.open(topic, start) }
}

// liveDrivers open the live transport live.driver names: the sink
// market-engine publishes through and the feed market-api follows.
var liveDrivers = map[string]struct {
	sink func(bootstrap *conf.Bootstrap) (live.Sink, error)
	feed func(bootstrap *conf.Bootstrap, logger *slog.Logger) (live.Feed, error)
}{
	conf.LiveKafka: {
		sink: func(bootstrap *conf.Bootstrap) (live.Sink, error) {
			return live.NewKafkaSink(liveKafkaOptions(bootstrap, nil))
		},
		feed: func(bootstrap *conf.Bootstrap, logger *slog.Logger) (live.Feed, error) {
			return live.NewKafkaFeed(liveKafkaOptions(bootstrap, logger))
		},
	},
	conf.LiveRedis: {
		sink: func(bootstrap *conf.Bootstrap) (live.Sink, error) {
			return live.NewRedisSink(liveRedisOptions(bootstrap))
		},
		feed: func(bootstrap *conf.Bootstrap, _ *slog.Logger) (live.Feed, error) {
			return live.NewRedisFeed(liveRedisOptions(bootstrap))
		},
	},
}

func liveKafkaOptions(bootstrap *conf.Bootstrap, logger *slog.Logger) live.KafkaOptions {
	kafka := bootstrap.GetKafka()
	return live.KafkaOptions{
		Brokers: kafka.GetBrokers(), ClientID: kafka.GetClientId(),
		FetchMaxBytes: kafka.GetFetchMaxBytes(), FetchMaxWait: kafka.GetFetchMaxWait().AsDuration(), Logger: logger,
	}
}

func liveRedisOptions(bootstrap *conf.Bootstrap) live.RedisOptions {
	redis := bootstrap.GetLive().GetRedis()
	return live.RedisOptions{
		Addr: redis.GetAddr(), Username: redis.GetUsername(), Password: redis.GetPassword(),
		DB: int(redis.GetDatabase()), DialTimeout: redis.GetDialTimeout().AsDuration(),
	}
}

func liveDriver(bootstrap *conf.Bootstrap) (string, error) {
	driver := bootstrap.GetLive().GetDriver()
	if _, ok := liveDrivers[driver]; !ok {
		return "", fmt.Errorf("live.driver %q is not supported; use one of: %s", driver, driverNames(liveDrivers))
	}
	return driver, nil
}

// NewPublisher returns market-engine's live publisher, or nil when live push
// is off.
func NewPublisher(bootstrap *conf.Bootstrap) (engine.Notifier, func(), error) {
	if !bootstrap.GetLive().GetEnabled() {
		return nil, func() {}, nil
	}
	driver, err := liveDriver(bootstrap)
	if err != nil {
		return nil, nil, err
	}
	sink, err := liveDrivers[driver].sink(bootstrap)
	if err != nil {
		return nil, nil, err
	}
	publisher, err := live.NewPublisher(live.PublisherOptions{
		Sink: sink, Topics: bootstrap.LiveTopics(), MaxLag: bootstrap.GetLive().GetMaxLag().AsDuration(),
	})
	if err != nil {
		sink.Close()
		return nil, nil, err
	}
	return publisher, publisher.Close, nil
}

// NewHub returns market-api's WebSocket hub, or nil when live push is off.
func NewHub(bootstrap *conf.Bootstrap, logger *slog.Logger) (*live.Hub, error) {
	if !bootstrap.GetLive().GetEnabled() {
		return nil, nil
	}
	driver, err := liveDriver(bootstrap)
	if err != nil {
		return nil, err
	}
	feed, err := liveDrivers[driver].feed(bootstrap, logger)
	if err != nil {
		return nil, err
	}
	liveConfig := bootstrap.GetLive()
	return live.NewHub(live.HubOptions{
		Feed: feed, Topics: bootstrap.LiveTopics(), Intervals: bootstrap.GetMarket().IntervalDurations(),
		Throttle: liveConfig.GetCandleThrottle().AsDuration(), ClientBuffer: int(liveConfig.GetClientBuffer()),
		MaxSubscriptions: int(liveConfig.GetMaxSubscriptions()), OriginPatterns: liveConfig.GetAllowedOrigins(),
		Logger: logger,
	})
}

// Retention turns storage.clickhouse.retention into the store's retention.
func Retention(retention *conf.Retention) chstore.Retention {
	return chstore.Retention{
		StoragePolicy: retention.GetStoragePolicy(), ColdVolume: retention.GetColdVolume(),
		ColdAfterDays: int(retention.GetColdAfterDays()), TradesDays: int(retention.GetTradesDays()),
		TokenPriceRollupDays: int(retention.GetTokenPriceRollupDays()),
		CandleCompactDays:    int(retention.GetCandleCompactDays()), CandleDays: retention.CandleDayDurations(),
		RawTransfersDays: int(retention.GetRawTransfersDays()), RawTradesDays: int(retention.GetRawTradesDays()),
		OracleUpdatesDays: int(retention.GetOracleUpdatesDays()),
	}
}

// closer logs the error of closing a store, since a cleanup cannot return
// one.
func closer(closeStore func() error, logger *slog.Logger, kind string) func() {
	return func() {
		if err := closeStore(); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("close storage", "kind", kind, "error", err)
		}
	}
}
