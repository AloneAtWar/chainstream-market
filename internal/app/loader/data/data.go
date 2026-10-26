// Package data is ch-loader's data layer: the chain topics it reads and the
// ClickHouse tables it copies raw facts into.
package data

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/google/wire"
	"gorm.io/gorm"
)

// ProviderSet is the data-layer provider set.
var ProviderSet = wire.NewSet(NewDB, source.NewSources, NewRawRepo)

// NewDB connects to the clickhouse database, creates the raw tables if
// needed and applies clickhouse.retention to them. Only ch-loader sets their
// retention, so market-api and market-engine, which read them, leave it
// alone.
func NewDB(ctx context.Context, clickhouse *config.ClickHouse, logger *slog.Logger) (*gorm.DB, func(), error) {
	db, cleanup, err := chdb.OpenConfig(ctx, clickhouse, logger)
	if err != nil {
		return nil, nil, err
	}
	retention := chdb.NewRetention(clickhouse.GetRetention())
	if err := chdb.Migrate(ctx, db, chdb.RawSchema); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("clickhouse raw store: migrate: %w", err)
	}
	if err := chdb.ApplyTTL(ctx, db, retention, retention.RawTTL()); err != nil {
		cleanup()
		return nil, nil, err
	}
	return db, cleanup, nil
}
