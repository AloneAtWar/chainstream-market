// Package data opens the stores and clients the commands share. Stores are
// opened by the driver the config names and provided by role, see
// storage.go. A provider whose resource the config turns off returns nil, as
// in chainstream-evm.
package data

import (
	"context"
	"errors"
	"log/slog"

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

// Sources opens chain topics.
type Sources struct {
	options source.KafkaOptions
}

// NewSources reads chain topics with the kafka settings.
func NewSources(kafka *conf.Kafka) *Sources {
	return &Sources{options: source.KafkaOptions{
		Brokers: kafka.GetBrokers(), ClientID: kafka.GetClientId(),
		FetchMaxBytes: kafka.GetFetchMaxBytes(), FetchMaxWait: kafka.GetFetchMaxWait().AsDuration(),
	}}
}

// Opener returns the engine's opener of topic.
func (sources *Sources) Opener(topic string) func(start int64) (source.Source, error) {
	options := sources.options
	options.Topic = topic
	return func(start int64) (source.Source, error) {
		return source.NewKafka(options, start)
	}
}

// NewPublisher returns market-engine's live publisher, or nil when live push
// is off.
func NewPublisher(bootstrap *conf.Bootstrap) (engine.Notifier, func(), error) {
	if !bootstrap.GetLive().GetEnabled() {
		return nil, func() {}, nil
	}
	kafka, liveConfig := bootstrap.GetKafka(), bootstrap.GetLive()
	publisher, err := live.NewPublisher(live.PublisherOptions{
		Brokers: kafka.GetBrokers(), ClientID: kafka.GetClientId(),
		Topics: bootstrap.LiveTopics(), MaxLag: liveConfig.GetMaxLag().AsDuration(),
	})
	if err != nil {
		return nil, nil, err
	}
	return publisher, publisher.Close, nil
}

// NewHub returns market-api's WebSocket hub, or nil when live push is off.
func NewHub(bootstrap *conf.Bootstrap, logger *slog.Logger) (*live.Hub, error) {
	if !bootstrap.GetLive().GetEnabled() {
		return nil, nil
	}
	kafka, liveConfig := bootstrap.GetKafka(), bootstrap.GetLive()
	return live.NewHub(live.HubOptions{
		Brokers: kafka.GetBrokers(), ClientID: kafka.GetClientId(),
		FetchMaxBytes: kafka.GetFetchMaxBytes(), FetchMaxWait: kafka.GetFetchMaxWait().AsDuration(),
		Topics: bootstrap.LiveTopics(), Intervals: bootstrap.GetMarket().IntervalDurations(),
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
