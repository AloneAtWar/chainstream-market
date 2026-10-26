// Package data opens the stores and clients the commands share. A provider
// whose resource the config turns off returns nil, as in chainstream-evm,
// and the commands that need it check the config before wiring.
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
	"github.com/AloneAtWar/chainstream-market/internal/storage/chraw"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
	"github.com/google/wire"
)

// ProviderSet is the data-layer provider set.
var ProviderSet = wire.NewSet(
	NewPostgres,
	NewClickHouseOptions,
	NewClickHouse,
	NewRawStore,
	NewHistory,
	NewMemory,
	NewStateStore,
	NewSources,
	NewPublisher,
	NewHub,
)

// NewPostgres opens the state.postgres store, or returns nil when
// state.driver is not postgres.
func NewPostgres(ctx context.Context, state *conf.State, logger *slog.Logger) (*pgstore.Store, func(), error) {
	if state.GetDriver() != conf.StatePostgres {
		return nil, func() {}, nil
	}
	store, err := pgstore.Open(ctx, pgstore.Options{
		DSN: state.GetPostgres().GetDsn(), Schema: state.GetPostgres().GetSchema(), Logger: logger,
	})
	if err != nil {
		return nil, nil, err
	}
	return store, store.Close, nil
}

// NewClickHouseOptions turns storage.clickhouse into store options.
func NewClickHouseOptions(storageConfig *conf.Storage, logger *slog.Logger) chstore.Options {
	clickhouse := storageConfig.GetClickhouse()
	return chstore.Options{
		Addr: clickhouse.GetAddr(), Database: clickhouse.GetDatabase(),
		Username: clickhouse.GetUsername(), Password: clickhouse.GetPassword(),
		DialTimeout: clickhouse.GetDialTimeout().AsDuration(), Logger: logger,
	}
}

// NewClickHouse opens market-engine's output store, or returns nil when
// storage.driver is not clickhouse.
func NewClickHouse(ctx context.Context, storageConfig *conf.Storage, options chstore.Options, logger *slog.Logger) (*chstore.Store, func(), error) {
	if storageConfig.GetDriver() != conf.StorageClickHouse {
		return nil, func() {}, nil
	}
	store, err := chstore.Open(ctx, options)
	if err != nil {
		return nil, nil, err
	}
	return store, closer(store.Close, logger, "clickhouse"), nil
}

// NewRawStore opens ch-loader's raw store, or returns nil when
// storage.driver is not clickhouse.
func NewRawStore(ctx context.Context, storageConfig *conf.Storage, options chstore.Options, logger *slog.Logger) (*chraw.Store, func(), error) {
	if storageConfig.GetDriver() != conf.StorageClickHouse {
		return nil, func() {}, nil
	}
	store, err := chraw.Open(ctx, options)
	if err != nil {
		return nil, nil, err
	}
	return store, closer(store.Close, logger, "clickhouse raw"), nil
}

// NewHistory returns ch-loader's raw trades for market-engine's backfills,
// or nil when backfills are off or the output is not in ClickHouse. It opens
// its own raw store, so market-engine only touches ch-loader's tables when
// it backfills.
func NewHistory(ctx context.Context, storageConfig *conf.Storage, market *conf.Market, options chstore.Options, logger *slog.Logger) (storage.History, func(), error) {
	if storageConfig.GetDriver() != conf.StorageClickHouse || market.GetBackfillBlocks() <= 0 {
		return nil, func() {}, nil
	}
	store, err := chraw.Open(ctx, options)
	if err != nil {
		return nil, nil, err
	}
	return chraw.NewHistory(store, market.GetBackfillWait().AsDuration()), closer(store.Close, logger, "clickhouse history"), nil
}

// NewMemory is the in-memory store market-engine keeps checkpoints in, and
// writes its output to without ClickHouse.
func NewMemory() *storage.Memory { return storage.NewMemory() }

// NewStateStore returns the persistent state store, or nil when checkpoints
// stay in memory.
func NewStateStore(store *pgstore.Store) storage.StateStore {
	if store == nil {
		return nil
	}
	return store
}

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
