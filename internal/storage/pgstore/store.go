// Package pgstore persists market-engine checkpoints and processor state in
// PostgreSQL. Each record's state changes and its checkpoint are committed in
// one transaction, so a restart resumes exactly where the last commit ended.
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	// DSN is a PostgreSQL connection string, such as
	// postgres://user:password@127.0.0.1:5432/market.
	DSN string
	// Schema holds the tables; it is created if needed. Empty uses public.
	Schema string
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
// holder interfaces.
type Store struct {
	pool *pgxpool.Pool
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
	config, err := pgxpool.ParseConfig(options.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres store: parse dsn: %w", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("postgres store: connect: %w", err)
	}
	statements := append([]string{"CREATE SCHEMA IF NOT EXISTS " + name}, schema...)
	statements = append(statements, holderSchema...)
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			pool.Close()
			return nil, fmt.Errorf("postgres store: migrate: %w", err)
		}
	}
	return &Store{pool: pool}, nil
}

func (store *Store) Close() { store.pool.Close() }

func (store *Store) Load(ctx context.Context, chainID uint64) (storage.Checkpoint, bool, error) {
	checkpoint, _, ok, err := store.loadCheckpoint(ctx, store.pool, chainID)
	return checkpoint, ok, err
}

// Save stores a checkpoint without state changes.
func (store *Store) Save(ctx context.Context, checkpoint storage.Checkpoint) error {
	return store.Commit(ctx, checkpoint, storage.StateDelta{})
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (*Store) loadCheckpoint(ctx context.Context, db querier, chainID uint64) (storage.Checkpoint, int64, bool, error) {
	checkpoint := storage.Checkpoint{ChainID: chainID}
	var prunedThrough int64
	err := db.QueryRow(ctx,
		`SELECT kafka_offset, block_height, block_hash, pruned_through, updated_at
		 FROM engine_checkpoints WHERE chain_id = $1`, int64(chainID),
	).Scan(&checkpoint.Offset, &checkpoint.Height, &checkpoint.Hash, &prunedThrough, &checkpoint.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.Checkpoint{}, 0, false, nil
	}
	if err != nil {
		return storage.Checkpoint{}, 0, false, fmt.Errorf("postgres store: load checkpoint: %w", err)
	}
	checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
	return checkpoint, prunedThrough, true, nil
}

// LoadState reads the checkpoint, every state entry and the retained undo
// history in one repeatable-read snapshot.
func (store *Store) LoadState(ctx context.Context, chainID uint64) (state storage.State, ok bool, err error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return storage.State{}, false, fmt.Errorf("postgres store: begin: %w", err)
	}
	defer func() { err = errors.Join(err, rollback(ctx, tx)) }()

	state.Checkpoint, state.PrunedThrough, ok, err = store.loadCheckpoint(ctx, tx, chainID)
	if err != nil || !ok {
		return storage.State{}, false, err
	}
	rows, err := tx.Query(ctx,
		`SELECT kind, key, value::text FROM engine_state WHERE chain_id = $1 ORDER BY kind, key`, int64(chainID))
	if err != nil {
		return storage.State{}, false, fmt.Errorf("postgres store: load state: %w", err)
	}
	state.Entries, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.StateEntry, error) {
		var entry storage.StateEntry
		var value string
		if scanErr := row.Scan(&entry.Kind, &entry.Key, &value); scanErr != nil {
			return entry, scanErr
		}
		entry.Value = []byte(value)
		return entry, nil
	})
	if err != nil {
		return storage.State{}, false, fmt.Errorf("postgres store: load state: %w", err)
	}
	rows, err = tx.Query(ctx,
		`SELECT block_height, kind, key, prev::text FROM engine_state_undo
		 WHERE chain_id = $1 ORDER BY block_height, kind, key`, int64(chainID))
	if err != nil {
		return storage.State{}, false, fmt.Errorf("postgres store: load undo: %w", err)
	}
	state.Undo, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.StateUndo, error) {
		var undo storage.StateUndo
		var prev *string
		if scanErr := row.Scan(&undo.Height, &undo.Kind, &undo.Key, &prev); scanErr != nil {
			return undo, scanErr
		}
		if prev != nil {
			undo.Prev = []byte(*prev)
		}
		return undo, nil
	})
	if err != nil {
		return storage.State{}, false, fmt.Errorf("postgres store: load undo: %w", err)
	}
	return state, true, nil
}

// Commit applies delta and saves checkpoint in one transaction.
func (store *Store) Commit(ctx context.Context, checkpoint storage.Checkpoint, delta storage.StateDelta) (err error) {
	if checkpoint.ChainID == 0 {
		return errors.New("postgres store: checkpoint chain id is required")
	}
	chainID := int64(checkpoint.ChainID)
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres store: begin: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, rollback(ctx, tx))
		}
	}()

	batch := &pgx.Batch{}
	if delta.Reverted {
		batch.Queue(`DELETE FROM engine_state_undo WHERE chain_id = $1 AND block_height > $2`, chainID, delta.RevertTo)
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
		batch.Queue(`INSERT INTO engine_state_undo (chain_id, block_height, kind, key, prev)
			SELECT $1, height, kind, key, prev::jsonb
			FROM unnest($2::bigint[], $3::text[], $4::text[], $5::text[]) AS undo(height, kind, key, prev)
			ON CONFLICT DO NOTHING`, chainID, heights, kinds, keys, prevs)
	}
	batch.Queue(`DELETE FROM engine_state_undo WHERE chain_id = $1 AND block_height <= $2`, chainID, delta.PrunedThrough)

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
		batch.Queue(`INSERT INTO engine_state (chain_id, kind, key, value)
			SELECT $1, kind, key, value::jsonb
			FROM unnest($2::text[], $3::text[], $4::text[]) AS entry(kind, key, value)
			ON CONFLICT (chain_id, kind, key) DO UPDATE SET value = EXCLUDED.value`,
			chainID, upsertKinds, upsertKeys, values)
	}
	if len(deleteKeys) > 0 {
		batch.Queue(`DELETE FROM engine_state AS state
			USING unnest($2::text[], $3::text[]) AS gone(kind, key)
			WHERE state.chain_id = $1 AND state.kind = gone.kind AND state.key = gone.key`,
			chainID, deleteKinds, deleteKeys)
	}
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	batch.Queue(`INSERT INTO engine_checkpoints (chain_id, kafka_offset, block_height, block_hash, pruned_through, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (chain_id) DO UPDATE SET kafka_offset = EXCLUDED.kafka_offset,
			block_height = EXCLUDED.block_height, block_hash = EXCLUDED.block_hash,
			pruned_through = GREATEST(engine_checkpoints.pruned_through, EXCLUDED.pruned_through),
			updated_at = EXCLUDED.updated_at`,
		chainID, checkpoint.Offset, checkpoint.Height, checkpoint.Hash, delta.PrunedThrough, updatedAt)

	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("postgres store: commit chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres store: commit: %w", err)
	}
	return nil
}

func rollback(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("postgres store: rollback: %w", err)
	}
	return nil
}

var (
	_ storage.CheckpointStore = (*Store)(nil)
	_ storage.StateStore      = (*Store)(nil)
)
