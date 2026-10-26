// Package data is market-rankings' data layer: market-engine's ClickHouse
// output it reads, and the PostgreSQL tables it keeps the rankings in.
package data

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/google/wire"
	"gorm.io/gorm"
)

// ProviderSet is the data-layer provider set.
var ProviderSet = wire.NewSet(NewData, NewRankingSource, NewRankingRepo)

// Data holds market-rankings' two databases.
type Data struct {
	// market is market-engine's ClickHouse output.
	market *gorm.DB
	// state holds token_rankings and the tables it is joined with.
	state *gorm.DB
}

// NewData connects to both databases and creates their tables if needed,
// so market-rankings can start before market-engine.
func NewData(ctx context.Context, clickhouse *config.ClickHouse, postgres *config.Postgres, logger *slog.Logger) (*Data, func(), error) {
	market, closeMarket, err := chdb.OpenConfig(ctx, clickhouse, logger)
	if err != nil {
		return nil, nil, err
	}
	if err = chdb.Migrate(ctx, market, chdb.MarketSchema); err != nil {
		closeMarket()
		return nil, nil, fmt.Errorf("clickhouse store: migrate: %w", err)
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
