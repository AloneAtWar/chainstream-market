package chstore

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Connect creates options.Database if needed and opens a GORM handle on it.
// Deletes are synchronous on the returned handle.
func Connect(ctx context.Context, options Options) (*gorm.DB, error) {
	if len(options.Addr) == 0 {
		return nil, errors.New("clickhouse store: at least one address is required")
	}
	if !identifier.MatchString(options.Database) {
		return nil, fmt.Errorf("clickhouse store: invalid database name %q", options.Database)
	}
	admin, err := open(options, "default")
	if err != nil {
		return nil, err
	}
	createErr := admin.WithContext(ctx).Exec("CREATE DATABASE IF NOT EXISTS " + options.Database).Error
	if joined := errors.Join(createErr, Close(admin)); joined != nil {
		return nil, fmt.Errorf("clickhouse store: create database: %w", joined)
	}
	db, err := open(options, options.Database)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func open(options Options, database string) (*gorm.DB, error) {
	pool := chgo.OpenDB(&chgo.Options{
		Addr:        options.Addr,
		Auth:        chgo.Auth{Database: database, Username: options.Username, Password: options.Password},
		DialTimeout: options.DialTimeout,
		Settings: chgo.Settings{
			// Make deletes visible to the inserts and reads that follow.
			"lightweight_deletes_sync": 2,
		},
	})
	db, err := gorm.Open(clickhouse.New(clickhouse.Config{Conn: pool}), &gorm.Config{
		Logger:                 newLogger(options.Logger),
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("clickhouse store: connect: %w", err), pool.Close())
	}
	return db, nil
}

func newLogger(slogger *slog.Logger) logger.Interface {
	if slogger == nil {
		return logger.Discard
	}
	return logger.NewSlogLogger(slogger.With("component", "chstore"), logger.Config{
		SlowThreshold:             5 * time.Second,
		LogLevel:                  logger.Warn,
		IgnoreRecordNotFoundError: true,
	})
}

// Close closes db's connection pool.
func Close(db *gorm.DB) error {
	pool, err := db.DB()
	if err != nil {
		return err
	}
	return pool.Close()
}

// Insert writes rows, a non-empty slice of one table's models, as a single
// native ClickHouse batch. The GORM dialect prepares the INSERT and appends
// each row to it; the batch is sent when the transaction commits.
func Insert(ctx context.Context, db *gorm.DB, rows any) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Create(rows).Error
	})
}

// ArrayParam passes a slice as one ClickHouse array parameter. GORM expands
// a plain slice into a parenthesized list, which suits IN but not hasAny.
type ArrayParam[T any] []T

func (values ArrayParam[T]) Value() (driver.Value, error) { return []T(values), nil }
