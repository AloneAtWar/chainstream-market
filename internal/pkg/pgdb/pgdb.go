// Package pgdb opens the PostgreSQL database every component shares and
// holds the DDL of all its tables: market-engine's checkpoints and state,
// holder balances, the token and pool registry and the token rankings. Every
// component creates them all, so services can start in any order; each keeps
// its own queries.
//
// pgdb 包打开所有组件共享的 PostgreSQL 数据库，保存全部表的 DDL 以及多组件共用表的
// 行模型与查询。各服务启动时在以 schema 名为 key 的 advisory lock 下建表，所以服务可以
// 任意顺序同时启动。
package pgdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"runtime"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Options struct {
	// DSN is a PostgreSQL connection string, such as
	// postgres://user:password@127.0.0.1:5432/market.
	DSN string
	// Schema holds the tables; it is created if needed. Empty uses public.
	Schema string
	// Logger receives GORM's slow query and error log; nil discards it.
	Logger *slog.Logger
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Open connects and creates the schema and tables if needed.
func Open(ctx context.Context, options Options) (*gorm.DB, error) {
	if options.DSN == "" {
		return nil, errors.New("postgres store: dsn is required")
	}
	name := options.Schema
	if name == "" {
		name = "public"
	}
	if !identifier.MatchString(name) {
		return nil, fmt.Errorf("postgres store: invalid schema name %q", name)
	}
	config, err := pgx.ParseConfig(options.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres store: parse dsn: %w", err)
	}
	config.RuntimeParams["search_path"] = name
	pool := stdlib.OpenDB(*config)
	// pgxpool's default size, which the store used before GORM.
	pool.SetMaxOpenConns(max(4, runtime.NumCPU()))
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: pool}), &gorm.Config{
		Logger:                 newLogger(options.Logger),
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("postgres store: open: %w", err), pool.Close())
	}
	db = db.WithContext(ctx)
	if err := pool.PingContext(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("postgres store: connect: %w", err), pool.Close())
	}
	if err := Migrate(db, name); err != nil {
		return nil, errors.Join(fmt.Errorf("postgres store: migrate: %w", err), pool.Close())
	}
	return db.WithContext(context.Background()), nil
}

func newLogger(slogger *slog.Logger) logger.Interface {
	if slogger == nil {
		return logger.Discard
	}
	return logger.NewSlogLogger(slogger.With("component", "postgres"), logger.Config{
		SlowThreshold:             time.Second,
		LogLevel:                  logger.Warn,
		IgnoreRecordNotFoundError: true,
	})
}

// Migrate creates the schema and tables in one transaction that holds an
// advisory lock keyed by the schema name. CREATE ... IF NOT EXISTS is not
// safe to run concurrently: when several services start on an empty schema,
// all but one fail with a duplicate key error in the system catalogs. The
// lock makes them run one after another, so the later ones find everything
// already created. It is released when the transaction ends.
//
// The tables are created from DDL rather than AutoMigrate: their partial,
// expression and GIN indexes and the one-time holder_counts backfill are
// not expressible as model tags, and AutoMigrate would try to alter the
// NUMERIC(78, 0) and JSONB columns of existing deployments.
func Migrate(db *gorm.DB, name string) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtextextended(?, 0))`, "chainstream-market/migrate/"+name).Error; err != nil {
			return fmt.Errorf("lock: %w", err)
		}
		statements := append([]string{"CREATE SCHEMA IF NOT EXISTS " + name}, engineSchema...)
		statements = append(statements, holderSchema...)
		statements = append(statements, registrySchema...)
		statements = append(statements, rankingSchema...)
		for _, statement := range statements {
			if err := tx.Exec(statement).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// Close closes db's connection pool.
func Close(db *gorm.DB) {
	if pool, err := db.DB(); err == nil {
		_ = pool.Close()
	}
}

// OpenConfig connects to the database the postgres section names, creating
// the schema and tables if needed, for a component's data layer.
func OpenConfig(ctx context.Context, postgres *config.Postgres, logger *slog.Logger) (*gorm.DB, func(), error) {
	db, err := Open(ctx, Options{DSN: postgres.GetDsn(), Schema: postgres.GetSchema(), Logger: logger})
	if err != nil {
		return nil, nil, err
	}
	return db, func() { Close(db) }, nil
}
