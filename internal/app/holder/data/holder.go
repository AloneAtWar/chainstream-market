package data

import (
	"context"
	"errors"
	"fmt"

	"github.com/AloneAtWar/chainstream-market/internal/app/holder/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"gorm.io/gorm"
)

// holderRepo keeps the holder tables of the PostgreSQL database.
type holderRepo struct {
	db *gorm.DB
}

// NewHolderRepo returns holder-indexer's repository.
func NewHolderRepo(db *gorm.DB) biz.HolderRepo { return &holderRepo{db: db} }

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

// LoadHolderCheckpoint returns the last committed checkpoint and the
// highest height whose undo history was pruned.
func (repo *holderRepo) LoadHolderCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, int64, bool, error) {
	return pgdb.LoadCheckpoint(repo.db.WithContext(ctx), pgdb.HolderCheckpoints, chainID)
}

// Holdings returns the stored holdings among keys.
func (repo *holderRepo) Holdings(ctx context.Context, chainID uint64, keys []model.HolderKey) ([]model.Holding, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	tokens, holders := splitKeys(keys)
	var rows []pgdb.HoldingRow
	err := repo.db.WithContext(ctx).Raw(`SELECT b.token, b.holder, b.balance::text AS balance
		FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[])) AS k(token, holder)
		JOIN holder_balances b ON b.chain_id = @chain AND b.token = k.token AND b.holder = k.holder`,
		map[string]any{"chain": int64(chainID), "tokens": pgdb.Array(tokens), "holders": pgdb.Array(holders)}).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: holdings: %w", err)
	}
	return pgdb.Holdings(rows)
}

// StaleHoldings returns up to limit holdings, least recently verified
// first.
func (repo *holderRepo) StaleHoldings(ctx context.Context, chainID uint64, limit int) ([]model.Holding, error) {
	var rows []pgdb.HoldingRow
	err := repo.db.WithContext(ctx).Table("holder_balances").Select("token, holder, balance::text AS balance").
		Where("chain_id = ?", int64(chainID)).Order("verified_height NULLS FIRST").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: stale holdings: %w", err)
	}
	return pgdb.Holdings(rows)
}

// CommitHolders applies change and saves checkpoint in one transaction.
func (repo *holderRepo) CommitHolders(ctx context.Context, checkpoint chainsync.Checkpoint, change model.HolderChange) error {
	if checkpoint.ChainID == 0 {
		return errors.New("postgres store: checkpoint chain id is required")
	}
	chainID := int64(checkpoint.ChainID)
	err := repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := pgdb.Runner{Tx: tx}
		if change.Reverted {
			revert := map[string]any{"chain": chainID, "to": change.RevertTo}
			// Created rows go first, so the update below only sees holdings
			// that existed before the reverted blocks.
			run.Exec(`WITH changed AS (
					DELETE FROM holder_balances AS b USING holder_undo AS u
					WHERE u.chain_id = @chain AND u.block_height > @to AND u.created
					  AND b.chain_id = u.chain_id AND b.token = u.token AND b.holder = u.holder
					RETURNING b.token, b.balance AS old_balance, NULL::numeric AS new_balance
				)`+addHolderCounts, revert)
			run.Exec(`WITH changed AS (
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
			run.Exec(`DELETE FROM holder_undo WHERE chain_id = ? AND block_height > ?`, chainID, change.RevertTo)
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
			run.Exec(`WITH changed AS (
					INSERT INTO holder_balances (chain_id, token, holder, balance, verified_height)
					SELECT @chain, token, holder, balance::numeric, verified
					FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[]), CAST(@balances AS text[]), CAST(@verified AS bigint[])) AS s(token, holder, balance, verified)
					RETURNING token, NULL::numeric AS old_balance, balance AS new_balance
				)`+addHolderCounts, map[string]any{
				"chain": chainID, "tokens": pgdb.Array(tokens), "holders": pgdb.Array(holders),
				"balances": pgdb.Array(balances), "verified": pgdb.Array(verified),
			})
			run.Exec(`INSERT INTO holder_undo (chain_id, block_height, token, holder, delta, created)
				SELECT @chain, @height, token, holder, 0, true FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[])) AS s(token, holder)`,
				map[string]any{"chain": chainID, "height": change.Height, "tokens": pgdb.Array(tokens), "holders": pgdb.Array(holders)})
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
				"chain": chainID, "height": change.Height, "tokens": pgdb.Array(tokens), "holders": pgdb.Array(holders),
				"deltas": pgdb.Array(deltas), "verified": pgdb.Array(verified),
			}
			// The undo row captures verified_height before the update changes it.
			run.Exec(`INSERT INTO holder_undo (chain_id, block_height, token, holder, delta, created, prev_verified)
				SELECT @chain, @height, d.token, d.holder, d.delta::numeric, false, b.verified_height
				FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[]), CAST(@deltas AS text[])) AS d(token, holder, delta)
				JOIN holder_balances b ON b.chain_id = @chain AND b.token = d.token AND b.holder = d.holder`, values)
			run.Exec(`WITH changed AS (
					UPDATE holder_balances AS b
					SET balance = b.balance + d.delta::numeric,
					    verified_height = CASE WHEN d.verified THEN @height ELSE b.verified_height END
					FROM unnest(CAST(@tokens AS text[]), CAST(@holders AS text[]), CAST(@deltas AS text[]), CAST(@verified AS bool[])) AS d(token, holder, delta, verified)
					WHERE b.chain_id = @chain AND b.token = d.token AND b.holder = d.holder
					RETURNING b.token, b.balance - d.delta::numeric AS old_balance, b.balance AS new_balance
				)`+addHolderCounts, values)
		}
		run.Exec(`DELETE FROM holder_undo WHERE chain_id = ? AND block_height <= ?`, chainID, change.PrunedThrough)
		if run.Err != nil {
			return run.Err
		}
		return pgdb.SaveCheckpoint(tx, pgdb.HolderCheckpoints, checkpoint, change.PrunedThrough)
	})
	if err != nil {
		return fmt.Errorf("postgres store: commit holders chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
	}
	return nil
}

func splitKeys(keys []model.HolderKey) ([]string, []string) {
	tokens := make([]string, len(keys))
	holders := make([]string, len(keys))
	for index, key := range keys {
		tokens[index], holders[index] = key.Token, key.Holder
	}
	return tokens, holders
}
