package pgstore

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/jackc/pgx/v5"
)

// holderSchema creates the holder tables. Balances are raw token units.
// verified_height is the last height balanceOf confirmed the balance; NULL
// means it was never confirmed.
var holderSchema = []string{
	`CREATE TABLE IF NOT EXISTS holder_checkpoints (
		chain_id       BIGINT PRIMARY KEY,
		kafka_offset   BIGINT NOT NULL,
		block_height   BIGINT NOT NULL,
		block_hash     TEXT NOT NULL,
		pruned_through BIGINT NOT NULL,
		updated_at     TIMESTAMPTZ NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS holder_balances (
		chain_id        BIGINT NOT NULL,
		token           TEXT NOT NULL,
		holder          TEXT NOT NULL,
		balance         NUMERIC(78, 0) NOT NULL,
		verified_height BIGINT,
		PRIMARY KEY (chain_id, token, holder)
	)`,
	`CREATE INDEX IF NOT EXISTS holder_balances_top
		ON holder_balances (chain_id, token, balance DESC) WHERE balance > 0`,
	`CREATE INDEX IF NOT EXISTS holder_balances_verified
		ON holder_balances (chain_id, verified_height NULLS FIRST)`,
	// Each row undoes one block's change to one holding: a created row is
	// deleted, otherwise delta is subtracted and verified_height restored.
	`CREATE TABLE IF NOT EXISTS holder_undo (
		chain_id      BIGINT NOT NULL,
		block_height  BIGINT NOT NULL,
		token         TEXT NOT NULL,
		holder        TEXT NOT NULL,
		delta         NUMERIC(78, 0) NOT NULL,
		created       BOOLEAN NOT NULL,
		prev_verified BIGINT,
		PRIMARY KEY (chain_id, block_height, token, holder)
	)`,
}

func (store *Store) LoadHolderCheckpoint(ctx context.Context, chainID uint64) (storage.Checkpoint, int64, bool, error) {
	checkpoint := storage.Checkpoint{ChainID: chainID}
	var prunedThrough int64
	err := store.pool.QueryRow(ctx,
		`SELECT kafka_offset, block_height, block_hash, pruned_through, updated_at
		 FROM holder_checkpoints WHERE chain_id = $1`, int64(chainID),
	).Scan(&checkpoint.Offset, &checkpoint.Height, &checkpoint.Hash, &prunedThrough, &checkpoint.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.Checkpoint{}, 0, false, nil
	}
	if err != nil {
		return storage.Checkpoint{}, 0, false, fmt.Errorf("postgres store: load holder checkpoint: %w", err)
	}
	checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
	return checkpoint, prunedThrough, true, nil
}

func (store *Store) Holdings(ctx context.Context, chainID uint64, keys []storage.HolderKey) ([]storage.Holding, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	tokens, holders := splitKeys(keys)
	rows, err := store.pool.Query(ctx,
		`SELECT b.token, b.holder, b.balance::text
		 FROM unnest($2::text[], $3::text[]) AS k(token, holder)
		 JOIN holder_balances b ON b.chain_id = $1 AND b.token = k.token AND b.holder = k.holder`,
		int64(chainID), tokens, holders)
	if err != nil {
		return nil, fmt.Errorf("postgres store: holdings: %w", err)
	}
	return collectHoldings(rows)
}

func (store *Store) StaleHoldings(ctx context.Context, chainID uint64, limit int) ([]storage.Holding, error) {
	rows, err := store.pool.Query(ctx,
		`SELECT token, holder, balance::text FROM holder_balances
		 WHERE chain_id = $1 ORDER BY verified_height NULLS FIRST LIMIT $2`, int64(chainID), limit)
	if err != nil {
		return nil, fmt.Errorf("postgres store: stale holdings: %w", err)
	}
	return collectHoldings(rows)
}

func (store *Store) TopHolders(ctx context.Context, chainID uint64, token string, limit int) ([]storage.Holding, error) {
	rows, err := store.pool.Query(ctx,
		`SELECT token, holder, balance::text AS raw FROM holder_balances AS b
		 WHERE chain_id = $1 AND token = $2 AND b.balance > 0
		 ORDER BY b.balance DESC, holder LIMIT $3`, int64(chainID), token, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres store: top holders: %w", err)
	}
	return collectHoldings(rows)
}

func (store *Store) HolderCount(ctx context.Context, chainID uint64, token string) (int64, error) {
	var count int64
	err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM holder_balances WHERE chain_id = $1 AND token = $2 AND balance > 0`,
		int64(chainID), token).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("postgres store: holder count: %w", err)
	}
	return count, nil
}

// CommitHolders applies change and saves checkpoint in one transaction.
func (store *Store) CommitHolders(ctx context.Context, checkpoint storage.Checkpoint, change storage.HolderChange) (err error) {
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
	if change.Reverted {
		// Created rows go first, so the update below only sees holdings
		// that existed before the reverted blocks.
		batch.Queue(`DELETE FROM holder_balances AS b USING holder_undo AS u
			WHERE u.chain_id = $1 AND u.block_height > $2 AND u.created
			  AND b.chain_id = u.chain_id AND b.token = u.token AND b.holder = u.holder`,
			chainID, change.RevertTo)
		batch.Queue(`UPDATE holder_balances AS b
			SET balance = b.balance - u.delta, verified_height = u.prev_verified
			FROM (
				SELECT token, holder, sum(delta) AS delta,
				       (array_agg(prev_verified ORDER BY block_height))[1] AS prev_verified
				FROM holder_undo WHERE chain_id = $1 AND block_height > $2 AND NOT created
				GROUP BY token, holder
			) AS u
			WHERE b.chain_id = $1 AND b.token = u.token AND b.holder = u.holder`,
			chainID, change.RevertTo)
		batch.Queue(`DELETE FROM holder_undo WHERE chain_id = $1 AND block_height > $2`, chainID, change.RevertTo)
	}
	if len(change.Created) > 0 {
		tokens := make([]string, len(change.Created))
		holders := make([]string, len(change.Created))
		balances := make([]string, len(change.Created))
		verified := make([]*int64, len(change.Created))
		for index, seed := range change.Created {
			tokens[index], holders[index], balances[index] = seed.Token, seed.Holder, seed.Balance.String()
			if seed.Verified {
				verified[index] = &change.Height
			}
		}
		batch.Queue(`INSERT INTO holder_balances (chain_id, token, holder, balance, verified_height)
			SELECT $1, token, holder, balance::numeric, verified
			FROM unnest($2::text[], $3::text[], $4::text[], $5::bigint[]) AS s(token, holder, balance, verified)`,
			chainID, tokens, holders, balances, verified)
		batch.Queue(`INSERT INTO holder_undo (chain_id, block_height, token, holder, delta, created)
			SELECT $1, $2, token, holder, 0, true FROM unnest($3::text[], $4::text[]) AS s(token, holder)`,
			chainID, change.Height, tokens, holders)
	}
	if len(change.Deltas) > 0 {
		tokens := make([]string, len(change.Deltas))
		holders := make([]string, len(change.Deltas))
		deltas := make([]string, len(change.Deltas))
		verified := make([]bool, len(change.Deltas))
		for index, delta := range change.Deltas {
			tokens[index], holders[index] = delta.Token, delta.Holder
			deltas[index], verified[index] = delta.Delta.String(), delta.Verified
		}
		// The undo row captures verified_height before the update changes it.
		batch.Queue(`INSERT INTO holder_undo (chain_id, block_height, token, holder, delta, created, prev_verified)
			SELECT $1, $2, d.token, d.holder, d.delta::numeric, false, b.verified_height
			FROM unnest($3::text[], $4::text[], $5::text[]) AS d(token, holder, delta)
			JOIN holder_balances b ON b.chain_id = $1 AND b.token = d.token AND b.holder = d.holder`,
			chainID, change.Height, tokens, holders, deltas)
		batch.Queue(`UPDATE holder_balances AS b
			SET balance = b.balance + d.delta::numeric,
			    verified_height = CASE WHEN d.verified THEN $2 ELSE b.verified_height END
			FROM unnest($3::text[], $4::text[], $5::text[], $6::bool[]) AS d(token, holder, delta, verified)
			WHERE b.chain_id = $1 AND b.token = d.token AND b.holder = d.holder`,
			chainID, change.Height, tokens, holders, deltas, verified)
	}
	batch.Queue(`DELETE FROM holder_undo WHERE chain_id = $1 AND block_height <= $2`, chainID, change.PrunedThrough)
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	batch.Queue(`INSERT INTO holder_checkpoints (chain_id, kafka_offset, block_height, block_hash, pruned_through, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (chain_id) DO UPDATE SET kafka_offset = EXCLUDED.kafka_offset,
			block_height = EXCLUDED.block_height, block_hash = EXCLUDED.block_hash,
			pruned_through = GREATEST(holder_checkpoints.pruned_through, EXCLUDED.pruned_through),
			updated_at = EXCLUDED.updated_at`,
		chainID, checkpoint.Offset, checkpoint.Height, checkpoint.Hash, change.PrunedThrough, updatedAt)

	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("postgres store: commit holders chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres store: commit: %w", err)
	}
	return nil
}

func splitKeys(keys []storage.HolderKey) ([]string, []string) {
	tokens := make([]string, len(keys))
	holders := make([]string, len(keys))
	for index, key := range keys {
		tokens[index], holders[index] = key.Token, key.Holder
	}
	return tokens, holders
}

func collectHoldings(rows pgx.Rows) ([]storage.Holding, error) {
	holdings, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.Holding, error) {
		var holding storage.Holding
		var balance string
		if err := row.Scan(&holding.Token, &holding.Holder, &balance); err != nil {
			return holding, err
		}
		value, ok := new(big.Int).SetString(balance, 10)
		if !ok {
			return holding, fmt.Errorf("invalid balance %q", balance)
		}
		holding.Balance = value
		return holding, nil
	})
	if err != nil {
		return nil, fmt.Errorf("postgres store: read holdings: %w", err)
	}
	return holdings, nil
}

var (
	_ storage.HolderStore  = (*Store)(nil)
	_ storage.HolderReader = (*Store)(nil)
)
