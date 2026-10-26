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

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/live"
	"github.com/AloneAtWar/chainstream-market/internal/market"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/google/wire"
)

// ProviderSet is the data-layer provider set.
var ProviderSet = wire.NewSet(
	NewMarketBackend,
	NewRawBackend,
	NewStateBackend,
	NewOutput,
	NewReader,
	NewTransferReader,
	NewHistory,
	NewStateStore,
	NewStateReaders,
	NewMemory,
	NewSources,
	NewPublisher,
	NewHub,
)

// NewMemory keeps market-engine's checkpoints when state.driver keeps no
// state.
func NewMemory() *storage.Memory { return storage.NewMemory() }

// NewSources reads chain topics with the driver source.driver names.
func NewSources(bootstrap *conf.Bootstrap) (*source.Sources, error) {
	return source.NewSources(bootstrap.GetSource(), bootstrap.GetKafka())
}

// liveDrivers open the live transport live.driver names: the sink
// market-engine publishes through and the feed market-api follows.
var liveDrivers = map[string]struct {
	sink func(bootstrap *conf.Bootstrap) (live.Sink, error)
	feed func(bootstrap *conf.Bootstrap, logger *slog.Logger) (live.Feed, error)
}{
	config.LiveKafka: {
		sink: func(bootstrap *conf.Bootstrap) (live.Sink, error) {
			return live.NewKafkaSink(liveKafkaOptions(bootstrap, nil))
		},
		feed: func(bootstrap *conf.Bootstrap, logger *slog.Logger) (live.Feed, error) {
			return live.NewKafkaFeed(liveKafkaOptions(bootstrap, logger))
		},
	},
	config.LiveRedis: {
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
func NewPublisher(bootstrap *conf.Bootstrap) (market.Notifier, func(), error) {
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

// closer logs the error of closing a store, since a cleanup cannot return
// one.
func closer(closeStore func() error, logger *slog.Logger, kind string) func() {
	return func() {
		if err := closeStore(); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("close storage", "kind", kind, "error", err)
		}
	}
}
