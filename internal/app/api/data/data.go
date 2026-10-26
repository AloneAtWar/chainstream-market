// Package data is market-api's data layer: the ClickHouse tables of
// market-engine and ch-loader, the PostgreSQL tables of holder-indexer,
// market-registry and market-rankings, and the live feed from market-engine.
package data

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/google/wire"
	"gorm.io/gorm"
)

// ProviderSet is the data-layer provider set.
var ProviderSet = wire.NewSet(
	NewData, NewMarketReader, NewTransferReader, NewHolderReader, NewRegistryReader, NewRankingReader, NewHub,
)

// Data holds market-api's two databases.
type Data struct {
	// market holds market-engine's output and ch-loader's raw tables.
	market *gorm.DB
	// state holds the holder, registry and ranking tables.
	state *gorm.DB
}

// NewData connects to both databases and creates their tables if needed,
// so market-api can start before the components that write them.
func NewData(ctx context.Context, clickhouse *config.ClickHouse, postgres *config.Postgres, logger *slog.Logger) (*Data, func(), error) {
	market, closeMarket, err := chdb.OpenConfig(ctx, clickhouse, logger)
	if err != nil {
		return nil, nil, err
	}
	for _, schema := range [][]string{chdb.MarketSchema, chdb.RawSchema} {
		if err = chdb.Migrate(ctx, market, schema); err != nil {
			closeMarket()
			return nil, nil, fmt.Errorf("clickhouse: migrate: %w", err)
		}
	}
	state, closeState, err := pgdb.OpenConfig(ctx, postgres, logger)
	if err != nil {
		closeMarket()
		return nil, nil, err
	}
	return &Data{market: market, state: state}, func() {
		closeState()
		closeMarket()
	}, nil
}

// NewHub returns the WebSocket hub of the live stream, or nil when live push
// is off.
func NewHub(bootstrap *conf.Bootstrap, logger *slog.Logger) (*live.Hub, error) {
	liveConfig := bootstrap.GetLive()
	if !liveConfig.GetEnabled() {
		return nil, nil
	}
	feed, err := live.OpenFeed(liveConfig, bootstrap.GetKafka(), logger)
	if err != nil {
		return nil, err
	}
	return live.NewHub(live.HubOptions{
		Feed: feed, Topics: config.LiveTopics(liveConfig, bootstrap.GetChains()),
		Intervals: bootstrap.GetApi().IntervalDurations(), Throttle: liveConfig.GetCandleThrottle().AsDuration(),
		ClientBuffer: int(liveConfig.GetClientBuffer()), MaxSubscriptions: int(liveConfig.GetMaxSubscriptions()),
		OriginPatterns: liveConfig.GetAllowedOrigins(), Logger: logger,
	})
}
