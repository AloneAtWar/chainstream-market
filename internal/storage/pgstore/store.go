// Package pgstore persists market-engine checkpoints and processor state in
// PostgreSQL. Each record's state changes and its checkpoint are committed in
// one transaction, so a restart resumes exactly where the last commit ended.
package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"runtime"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
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

// schema creates the tables. Undo rows are keyed by height first so that
// reverts and pruning delete a contiguous range.
var schema = []string{
	`CREATE TABLE IF NOT EXISTS engine_checkpoints (
		chain_id       BIGINT PRIMARY KEY,
		kafka_offset   BIGINT NOT NULL,
		block_height   BIGINT NOT NULL,
		block_hash     TEXT NOT NULL,
		pruned_through BIGINT NOT NULL,
		updated_at     TIMESTAMPTZ NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS engine_state (
		chain_id BIGINT NOT NULL,
		kind     TEXT NOT NULL,
		key      TEXT NOT NULL,
		value    JSONB NOT NULL,
		PRIMARY KEY (chain_id, kind, key)
	)`,
	`CREATE TABLE IF NOT EXISTS engine_state_undo (
		chain_id     BIGINT NOT NULL,
		block_height BIGINT NOT NULL,
		kind         TEXT NOT NULL,
		key          TEXT NOT NULL,
		prev         JSONB,
		PRIMARY KEY (chain_id, block_height, kind, key)
	)`,
}

// Store implements storage.CheckpointStore, storage.StateStore and the
// holder, registry and ranking interfaces on GORM. Single-row reads and
// upserts use GORM's builder; the per-block writes stay set-based SQL run
// through GORM, so a block of any size is a fixed number of statements.
type Store struct {
	db *gorm.DB
}

// Open connects and creates the schema and tables if needed.
func Open(ctx context.Context, options Options) (*Store, error) {
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
	if err := migrate(db, name); err != nil {
		return nil, errors.Join(fmt.Errorf("postgres store: migrate: %w", err), pool.Close())
	}
	return &Store{db: db.WithContext(context.Background())}, nil
}

func newLogger(slogger *slog.Logger) logger.Interface {
	if slogger == nil {
		return logger.Discard
	}
	return logger.NewSlogLogger(slogger.With("component", "pgstore"), logger.Config{
		SlowThreshold:             time.Second,
		LogLevel:                  logger.Warn,
		IgnoreRecordNotFoundError: true,
	})
}

// migrate creates the schema and tables in one transaction that holds an
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
func migrate(db *gorm.DB, name string) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtextextended(?, 0))`, "chainstream-market/migrate/"+name).Error; err != nil {
			return fmt.Errorf("lock: %w", err)
		}
		statements := append([]string{"CREATE SCHEMA IF NOT EXISTS " + name}, schema...)
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

// Close closes the connection pool.
func (store *Store) Close() {
	if pool, err := store.db.DB(); err == nil {
		_ = pool.Close()
	}
}

func (store *Store) Load(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	checkpoint, _, ok, err := loadCheckpoint(store.db.WithContext(ctx), engineCheckpoints, chainID)
	return checkpoint, ok, err
}

// Save stores a checkpoint without state changes.
func (store *Store) Save(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	return store.Commit(ctx, checkpoint, storage.StateDelta{})
}

// LoadState reads the checkpoint, every state entry and the retained undo
// history in one repeatable-read snapshot.
func (store *Store) LoadState(ctx context.Context, chainID uint64) (state storage.State, ok bool, err error) {
	err = store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		state.Checkpoint, state.PrunedThrough, ok, err = loadCheckpoint(tx, engineCheckpoints, chainID)
		if err != nil || !ok {
			return err
		}
		var entries []stateRow
		if findErr := tx.Model(&stateRow{}).Select("kind, key, value::text AS value").
			Where("chain_id = ?", int64(chainID)).Order("kind, key").Find(&entries).Error; findErr != nil {
			return fmt.Errorf("postgres store: load state: %w", findErr)
		}
		state.Entries = make([]storage.StateEntry, len(entries))
		for index, entry := range entries {
			state.Entries[index] = storage.StateEntry{Kind: entry.Kind, Key: entry.Key, Value: []byte(entry.Value)}
		}
		var undo []stateUndoRow
		if findErr := tx.Model(&stateUndoRow{}).Select("block_height, kind, key, prev::text AS prev").
			Where("chain_id = ?", int64(chainID)).Order("block_height, kind, key").Find(&undo).Error; findErr != nil {
			return fmt.Errorf("postgres store: load undo: %w", findErr)
		}
		state.Undo = make([]storage.StateUndo, len(undo))
		for index, row := range undo {
			state.Undo[index] = storage.StateUndo{Height: row.BlockHeight, Kind: row.Kind, Key: row.Key}
			if row.Prev != nil {
				state.Undo[index].Prev = []byte(*row.Prev)
			}
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return storage.State{}, false, err
	}
	if !ok {
		return storage.State{}, false, nil
	}
	return state, true, nil
}

// Commit applies delta and saves checkpoint in one transaction.
func (store *Store) Commit(ctx context.Context, checkpoint chainsync.Checkpoint, delta storage.StateDelta) error {
	if checkpoint.ChainID == 0 {
		return errors.New("postgres store: checkpoint chain id is required")
	}
	chainID := int64(checkpoint.ChainID)
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := runner{tx: tx}
		if delta.Reverted {
			run.exec(`DELETE FROM engine_state_undo WHERE chain_id = ? AND block_height > ?`, chainID, delta.RevertTo)
		}
		if len(delta.Undo) > 0 {
			heights := make([]int64, len(delta.Undo))
			kinds := make([]string, len(delta.Undo))
			keys := make([]string, len(delta.Undo))
			prevs := make([]*string, len(delta.Undo))
			for index, undo := range delta.Undo {
				heights[index], kinds[index], keys[index] = undo.Height, undo.Kind, undo.Key
				if undo.Prev != nil {
					prev := string(undo.Prev)
					prevs[index] = &prev
				}
			}
			// The first write at a height holds the value to revert to, so an
			// existing row wins.
			run.exec(`INSERT INTO engine_state_undo (chain_id, block_height, kind, key, prev)
				SELECT @chain, height, kind, key, prev::jsonb
				FROM unnest(CAST(@heights AS bigint[]), CAST(@kinds AS text[]), CAST(@keys AS text[]), CAST(@prevs AS text[])) AS undo(height, kind, key, prev)
				ON CONFLICT DO NOTHING`, map[string]any{
				"chain": chainID, "heights": array(heights), "kinds": array(kinds), "keys": array(keys), "prevs": array(prevs),
			})
		}
		run.exec(`DELETE FROM engine_state_undo WHERE chain_id = ? AND block_height <= ?`, chainID, delta.PrunedThrough)

		var upsertKinds, upsertKeys, values, deleteKinds, deleteKeys []string
		for _, entry := range delta.Entries {
			if entry.Value == nil {
				deleteKinds, deleteKeys = append(deleteKinds, entry.Kind), append(deleteKeys, entry.Key)
				continue
			}
			upsertKinds, upsertKeys = append(upsertKinds, entry.Kind), append(upsertKeys, entry.Key)
			values = append(values, string(entry.Value))
		}
		if len(upsertKeys) > 0 {
			run.exec(`INSERT INTO engine_state (chain_id, kind, key, value)
				SELECT @chain, kind, key, value::jsonb
				FROM unnest(CAST(@kinds AS text[]), CAST(@keys AS text[]), CAST(@values AS text[])) AS entry(kind, key, value)
				ON CONFLICT (chain_id, kind, key) DO UPDATE SET value = EXCLUDED.value`, map[string]any{
				"chain": chainID, "kinds": array(upsertKinds), "keys": array(upsertKeys), "values": array(values),
			})
		}
		if len(deleteKeys) > 0 {
			run.exec(`DELETE FROM engine_state AS state
				USING unnest(CAST(@kinds AS text[]), CAST(@keys AS text[])) AS gone(kind, key)
				WHERE state.chain_id = @chain AND state.kind = gone.kind AND state.key = gone.key`, map[string]any{
				"chain": chainID, "kinds": array(deleteKinds), "keys": array(deleteKeys),
			})
		}
		if run.err != nil {
			return run.err
		}
		return saveCheckpoint(tx, engineCheckpoints, checkpoint, delta.PrunedThrough)
	})
	if err != nil {
		return fmt.Errorf("postgres store: commit chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
	}
	return nil
}

var (
	_ storage.CheckpointStore = (*Store)(nil)
	_ storage.StateStore      = (*Store)(nil)
)
