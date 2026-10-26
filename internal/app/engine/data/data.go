// Package data is market-engine's data layer: the chain topics it reads, its
// output in ClickHouse or memory, its state in PostgreSQL or memory, the raw
// trades its backfills read back, and the live publisher.
package data

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/engine/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/google/wire"
)

// ProviderSet is the data-layer provider set.
var ProviderSet = wire.NewSet(source.NewSources, NewOutput, NewState, NewHistory, NewPublisher)

// NewOutput opens the output store storage.driver names. In ClickHouse it
// creates the market tables if needed and applies the retention to them.
func NewOutput(ctx context.Context, storage *conf.Storage, logger *slog.Logger) (biz.Output, func(), error) {
	if storage.GetDriver() == conf.StorageMemory {
		return biz.NewMemory(), func() {}, nil
	}
	db, cleanup, err := chdb.OpenConfig(ctx, storage.GetClickhouse(), logger)
	if err != nil {
		return nil, nil, err
	}
	if err = chdb.Migrate(ctx, db, chdb.MarketSchema); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("clickhouse output: migrate: %w", err)
	}
	if err = applyRetention(ctx, db, chdb.NewRetention(storage.GetClickhouse().GetRetention())); err != nil {
		cleanup()
		return nil, nil, err
	}
	output, err := NewOutputRepo(ctx, db)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return output, cleanup, nil
}

// NewState opens the state store state.driver names, or returns nil when it
// keeps state in memory: each topic is then replayed from offset zero.
func NewState(ctx context.Context, state *conf.State, logger *slog.Logger) (biz.StateStore, func(), error) {
	if state.GetDriver() == conf.StateMemory {
		return nil, func() {}, nil
	}
	db, cleanup, err := pgdb.OpenConfig(ctx, state.GetPostgres(), logger)
	if err != nil {
		return nil, nil, err
	}
	return NewStateRepo(db), cleanup, nil
}

// NewHistory reads ch-loader's raw trades for backfills, or returns nil when
// backfills are off or the output is not in ClickHouse. It opens a
// connection of its own and creates the raw tables if needed, so
// market-engine can start before ch-loader.
func NewHistory(ctx context.Context, storage *conf.Storage, market *conf.Market, logger *slog.Logger) (biz.History, func(), error) {
	if storage.GetDriver() != conf.StorageClickHouse || market.GetBackfillBlocks() <= 0 {
		return nil, func() {}, nil
	}
	db, cleanup, err := chdb.OpenConfig(ctx, storage.GetClickhouse(), logger)
	if err != nil {
		return nil, nil, err
	}
	if err = chdb.Migrate(ctx, db, chdb.RawSchema); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("clickhouse history: migrate: %w", err)
	}
	return NewHistoryRepo(db, market.GetBackfillWait().AsDuration()), cleanup, nil
}

// NewPublisher returns the live publisher, or nil when live push is off.
func NewPublisher(liveConfig *config.Live, kafka *config.Kafka, chains []*config.Chain) (biz.Notifier, func(), error) {
	if !liveConfig.GetEnabled() {
		return nil, func() {}, nil
	}
	sink, err := live.OpenSink(liveConfig, kafka)
	if err != nil {
		return nil, nil, err
	}
	publisher, err := live.NewPublisher(live.PublisherOptions{
		Sink: sink, Topics: config.LiveTopics(liveConfig, chains), MaxLag: liveConfig.GetMaxLag().AsDuration(),
	})
	if err != nil {
		sink.Close()
		return nil, nil, err
	}
	return publisher, publisher.Close, nil
}
