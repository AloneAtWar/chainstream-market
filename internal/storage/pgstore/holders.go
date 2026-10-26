package pgstore

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"gorm.io/gorm"
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
	// holder_counts keeps the number of positive holdings per token, moved
	// by CommitHolders whenever a balance crosses zero. A schema that had
	// holder_balances before this table existed is backfilled once.
	`DO $$
	BEGIN
		IF to_regclass('holder_counts') IS NULL THEN
			CREATE TABLE holder_counts (
				chain_id BIGINT NOT NULL,
				token    TEXT NOT NULL,
				holders  BIGINT NOT NULL,
				PRIMARY KEY (chain_id, token)
			);
			INSERT INTO holder_counts (chain_id, token, holders)
			SELECT chain_id, token, count(*) FROM holder_balances WHERE balance > 0 GROUP BY chain_id, token;
		END IF;
	END $$`,
}

// addHolderCounts ends a statement whose CTE "changed" returns the token and
// the old and new balance of each holding it touched. It adds one holder per
// balance that became positive and removes one per balance that stopped
// being positive. A holding that does not exist has a NULL balance.
const addHolderCounts = `
	INSERT INTO holder_counts AS c (chain_id, token, holders)
	SELECT @chain, token, sum((COALESCE(new_balance, 0) > 0)::int - (COALESCE(old_balance, 0) > 0)::int)
	FROM changed GROUP BY token
	HAVING sum((COALESCE(new_balance, 0) > 0)::int - (COALESCE(old_balance, 0) > 0)::int) <> 0
	ON CONFLICT (chain_id, token) DO UPDATE SET holders = c.holders + EXCLUDED.holders`

func (store *Store) LoadHolderCheckpoint(ctx context.Context, chainID uint64) (storage.Checkpoint, int64, bool, error) {
	return loadCheckpoint(store.db.WithContext(ctx), holderCheckpoints, chainID)
}

func (store *Store) Holdings(ctx context.Context, chainID uint64, keys []storage.HolderKey) ([]storage.Holding, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	tokens, holders := splitKeys(keys)
	var rows []holdingRow
	err := store.db.WithContext(ctx).Raw(`SELECT b.token, b.holder, b.balance::text AS balance
		FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[])) AS k(token, holder)
		JOIN holder_balances b ON b.chain_id = @chain AND b.token = k.token AND b.holder = k.holder`,
		map[string]any{"chain": int64(chainID), "tokens": array(tokens), "holders": array(holders)}).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: holdings: %w", err)
	}
	return toHoldings(rows)
}

func (store *Store) StaleHoldings(ctx context.Context, chainID uint64, limit int) ([]storage.Holding, error) {
	var rows []holdingRow
	err := store.db.WithContext(ctx).Table("holder_balances").Select("token, holder, balance::text AS balance").
		Where("chain_id = ?", int64(chainID)).Order("verified_height NULLS FIRST").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: stale holdings: %w", err)
	}
	return toHoldings(rows)
}

func (store *Store) TopHolders(ctx context.Context, chainID uint64, token string, limit int) ([]storage.Holding, error) {
	var rows []holdingRow
	err := store.db.WithContext(ctx).Table("holder_balances AS b").Select("token, holder, balance::text AS balance").
		Where("chain_id = ? AND token = ? AND b.balance > 0", int64(chainID), token).
		Order("b.balance DESC, holder").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: top holders: %w", err)
	}
	return toHoldings(rows)
}

func (store *Store) HolderCount(ctx context.Context, chainID uint64, token string) (int64, error) {
	var row holderCountRow
	err := store.db.WithContext(ctx).Where("chain_id = ? AND token = ?", int64(chainID), token).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("postgres store: holder count: %w", err)
	}
	return row.Holders, nil
}

// CommitHolders applies change and saves checkpoint in one transaction.
func (store *Store) CommitHolders(ctx context.Context, checkpoint storage.Checkpoint, change storage.HolderChange) error {
	if checkpoint.ChainID == 0 {
		return errors.New("postgres store: checkpoint chain id is required")
	}
	chainID := int64(checkpoint.ChainID)
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := runner{tx: tx}
		if change.Reverted {
			revert := map[string]any{"chain": chainID, "to": change.RevertTo}
			// Created rows go first, so the update below only sees holdings
			// that existed before the reverted blocks.
			run.exec(`WITH changed AS (
					DELETE FROM holder_balances AS b USING holder_undo AS u
					WHERE u.chain_id = @chain AND u.block_height > @to AND u.created
					  AND b.chain_id = u.chain_id AND b.token = u.token AND b.holder = u.holder
					RETURNING b.token, b.balance AS old_balance, NULL::numeric AS new_balance
				)`+addHolderCounts, revert)
			run.exec(`WITH changed AS (
					UPDATE holder_balances AS b
					SET balance = b.balance - u.delta, verified_height = u.prev_verified
					FROM (
						SELECT token, holder, sum(delta) AS delta,
						       (array_agg(prev_verified ORDER BY block_height))[1] AS prev_verified
						FROM holder_undo WHERE chain_id = @chain AND block_height > @to AND NOT created
						GROUP BY token, holder
					) AS u
					WHERE b.chain_id = @chain AND b.token = u.token AND b.holder = u.holder
					RETURNING b.token, b.balance + u.delta AS old_balance, b.balance AS new_balance
				)`+addHolderCounts, revert)
			run.exec(`DELETE FROM holder_undo WHERE chain_id = ? AND block_height > ?`, chainID, change.RevertTo)
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
			run.exec(`WITH changed AS (
					INSERT INTO holder_balances (chain_id, token, holder, balance, verified_height)
					SELECT @chain, token, holder, balance::numeric, verified
					FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[]), CAST(@balances AS text[]), CAST(@verified AS bigint[])) AS s(token, holder, balance, verified)
					RETURNING token, NULL::numeric AS old_balance, balance AS new_balance
				)`+addHolderCounts, map[string]any{
				"chain": chainID, "tokens": array(tokens), "holders": array(holders),
				"balances": array(balances), "verified": array(verified),
			})
			run.exec(`INSERT INTO holder_undo (chain_id, block_height, token, holder, delta, created)
				SELECT @chain, @height, token, holder, 0, true FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[])) AS s(token, holder)`,
				map[string]any{"chain": chainID, "height": change.Height, "tokens": array(tokens), "holders": array(holders)})
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
			values := map[string]any{
				"chain": chainID, "height": change.Height, "tokens": array(tokens), "holders": array(holders),
				"deltas": array(deltas), "verified": array(verified),
			}
			// The undo row captures verified_height before the update changes it.
			run.exec(`INSERT INTO holder_undo (chain_id, block_height, token, holder, delta, created, prev_verified)
				SELECT @chain, @height, d.token, d.holder, d.delta::numeric, false, b.verified_height
				FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[]), CAST(@deltas AS text[])) AS d(token, holder, delta)
				JOIN holder_balances b ON b.chain_id = @chain AND b.token = d.token AND b.holder = d.holder`, values)
			run.exec(`WITH changed AS (
					UPDATE holder_balances AS b
					SET balance = b.balance + d.delta::numeric,
					    verified_height = CASE WHEN d.verified THEN @height ELSE b.verified_height END
					FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[]), CAST(@deltas AS text[]), CAST(@verified AS bool[])) AS d(token, holder, delta, verified)
					WHERE b.chain_id = @chain AND b.token = d.token AND b.holder = d.holder
					RETURNING b.token, b.balance - d.delta::numeric AS old_balance, b.balance AS new_balance
				)`+addHolderCounts, values)
		}
		run.exec(`DELETE FROM holder_undo WHERE chain_id = ? AND block_height <= ?`, chainID, change.PrunedThrough)
		if run.err != nil {
			return run.err
		}
		return saveCheckpoint(tx, holderCheckpoints, checkpoint, change.PrunedThrough)
	})
	if err != nil {
		return fmt.Errorf("postgres store: commit holders chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
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

func toHoldings(rows []holdingRow) ([]storage.Holding, error) {
	holdings := make([]storage.Holding, len(rows))
	for index, row := range rows {
		value, ok := new(big.Int).SetString(row.Balance, 10)
		if !ok {
			return nil, fmt.Errorf("postgres store: read holdings: invalid balance %q", row.Balance)
		}
		holdings[index] = storage.Holding{HolderKey: storage.HolderKey{Token: row.Token, Holder: row.Holder}, Balance: value}
	}
	return holdings, nil
}

var (
	_ storage.HolderStore  = (*Store)(nil)
	_ storage.HolderReader = (*Store)(nil)
)
