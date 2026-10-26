package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"gorm.io/gorm"
)

// stateRepo persists market-engine checkpoints and processor state in
// PostgreSQL. Each record's state changes and its checkpoint are committed
// in one transaction, so a restart resumes exactly where the last commit
// ended. The per-block writes are set-based SQL run through GORM, so a block
// of any size is a fixed number of statements.
type stateRepo struct {
	db *gorm.DB
}

// NewStateRepo keeps state in db.
func NewStateRepo(db *gorm.DB) biz.StateStore { return &stateRepo{db: db} }

func (repo *stateRepo) Load(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	checkpoint, _, ok, err := pgdb.LoadCheckpoint(repo.db.WithContext(ctx), pgdb.EngineCheckpoints, chainID)
	return checkpoint, ok, err
}

// Save stores a checkpoint without state changes.
func (repo *stateRepo) Save(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	return repo.Commit(ctx, checkpoint, biz.StateDelta{})
}

// LoadState reads the checkpoint, every state entry and the retained undo
// history in one repeatable-read snapshot.
func (repo *stateRepo) LoadState(ctx context.Context, chainID uint64) (state biz.State, ok bool, err error) {
	err = repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		state.Checkpoint, state.PrunedThrough, ok, err = pgdb.LoadCheckpoint(tx, pgdb.EngineCheckpoints, chainID)
		if err != nil || !ok {
			return err
		}
		var entries []stateRow
		if findErr := tx.Model(&stateRow{}).Select("kind, key, value::text AS value").
			Where("chain_id = ?", int64(chainID)).Order("kind, key").Find(&entries).Error; findErr != nil {
			return fmt.Errorf("postgres state: load state: %w", findErr)
		}
		state.Entries = make([]biz.StateEntry, len(entries))
		for index, entry := range entries {
			state.Entries[index] = biz.StateEntry{Kind: entry.Kind, Key: entry.Key, Value: []byte(entry.Value)}
		}
		var undo []stateUndoRow
		if findErr := tx.Model(&stateUndoRow{}).Select("block_height, kind, key, prev::text AS prev").
			Where("chain_id = ?", int64(chainID)).Order("block_height, kind, key").Find(&undo).Error; findErr != nil {
			return fmt.Errorf("postgres state: load undo: %w", findErr)
		}
		state.Undo = make([]biz.StateUndo, len(undo))
		for index, row := range undo {
			state.Undo[index] = biz.StateUndo{Height: row.BlockHeight, Kind: row.Kind, Key: row.Key}
			if row.Prev != nil {
				state.Undo[index].Prev = []byte(*row.Prev)
			}
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return biz.State{}, false, err
	}
	if !ok {
		return biz.State{}, false, nil
	}
	return state, true, nil
}

// Commit applies delta and saves checkpoint in one transaction.
func (repo *stateRepo) Commit(ctx context.Context, checkpoint chainsync.Checkpoint, delta biz.StateDelta) error {
	if checkpoint.ChainID == 0 {
		return errors.New("postgres state: checkpoint chain id is required")
	}
	chainID := int64(checkpoint.ChainID)
	err := repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := pgdb.Runner{Tx: tx}
		if delta.Reverted {
			run.Exec(`DELETE FROM engine_state_undo WHERE chain_id = ? AND block_height > ?`, chainID, delta.RevertTo)
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
			run.Exec(`INSERT INTO engine_state_undo (chain_id, block_height, kind, key, prev)
				SELECT @chain, height, kind, key, prev::jsonb
				FROM unnest(CAST(@heights AS bigint[]), CAST(@kinds AS text[]), CAST(@keys AS text[]), CAST(@prevs AS text[])) AS undo(height, kind, key, prev)
				ON CONFLICT DO NOTHING`, map[string]any{
				"chain": chainID, "heights": pgdb.Array(heights), "kinds": pgdb.Array(kinds), "keys": pgdb.Array(keys), "prevs": pgdb.Array(prevs),
			})
		}
		run.Exec(`DELETE FROM engine_state_undo WHERE chain_id = ? AND block_height <= ?`, chainID, delta.PrunedThrough)

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
			run.Exec(`INSERT INTO engine_state (chain_id, kind, key, value)
				SELECT @chain, kind, key, value::jsonb
				FROM unnest(CAST(@kinds AS text[]), CAST(@keys AS text[]), CAST(@values AS text[])) AS entry(kind, key, value)
				ON CONFLICT (chain_id, kind, key) DO UPDATE SET value = EXCLUDED.value`, map[string]any{
				"chain": chainID, "kinds": pgdb.Array(upsertKinds), "keys": pgdb.Array(upsertKeys), "values": pgdb.Array(values),
			})
		}
		if len(deleteKeys) > 0 {
			run.Exec(`DELETE FROM engine_state AS state
				USING unnest(CAST(@kinds AS text[]), CAST(@keys AS text[])) AS gone(kind, key)
				WHERE state.chain_id = @chain AND state.kind = gone.kind AND state.key = gone.key`, map[string]any{
				"chain": chainID, "kinds": pgdb.Array(deleteKinds), "keys": pgdb.Array(deleteKeys),
			})
		}
		if run.Err != nil {
			return run.Err
		}
		return pgdb.SaveCheckpoint(tx, pgdb.EngineCheckpoints, checkpoint, delta.PrunedThrough)
	})
	if err != nil {
		return fmt.Errorf("postgres state: commit chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
	}
	return nil
}
