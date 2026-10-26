package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/AloneAtWar/chainstream-market/internal/app/registry/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"gorm.io/gorm"
)

// registryRepo keeps the registry tables of the PostgreSQL database.
type registryRepo struct {
	db *gorm.DB
}

// NewRegistryRepo returns the registry's repository.
func NewRegistryRepo(db *gorm.DB) biz.RegistryRepo { return &registryRepo{db: db} }

const (
	undoToken = "token"
	undoPool  = "pool"
)

// LoadRegistryCheckpoint returns the last committed checkpoint and the
// highest height whose undo history was pruned.
func (repo *registryRepo) LoadRegistryCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, int64, bool, error) {
	return pgdb.LoadCheckpoint(repo.db.WithContext(ctx), pgdb.RegistryCheckpoints, chainID)
}

// CommitRegistry applies change and saves checkpoint in one transaction.
func (repo *registryRepo) CommitRegistry(ctx context.Context, checkpoint chainsync.Checkpoint, change model.RegistryChange) error {
	if checkpoint.ChainID == 0 {
		return errors.New("postgres store: checkpoint chain id is required")
	}
	chainID := int64(checkpoint.ChainID)
	err := repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := &pgdb.Runner{Tx: tx}
		if change.Reverted {
			queueRegistryRevert(run, chainID, change.RevertTo, "registry_tokens", "address", undoToken)
			queueRegistryRevert(run, chainID, change.RevertTo, "registry_pools", "pool_id", undoPool)
			run.Exec(`DELETE FROM registry_undo WHERE chain_id = ? AND block_height > ?`, chainID, change.RevertTo)
		}
		if len(change.Tokens) > 0 {
			if err := queueTokens(run, chainID, change.Height, change.Tokens); err != nil {
				return err
			}
		}
		if len(change.Pools) > 0 {
			if err := queuePools(run, chainID, change.Height, change.Pools); err != nil {
				return err
			}
		}
		run.Exec(`DELETE FROM registry_undo WHERE chain_id = ? AND block_height <= ?`, chainID, change.PrunedThrough)
		if run.Err != nil {
			return run.Err
		}
		return pgdb.SaveCheckpoint(tx, pgdb.RegistryCheckpoints, checkpoint, change.PrunedThrough)
	})
	if err != nil {
		return fmt.Errorf("postgres store: commit registry chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
	}
	return nil
}

// queueRegistryRevert puts every row the reverted blocks touched back as it
// was before the first of them: the row is deleted, then the oldest undo
// image above rollbackTo is restored unless the row did not exist yet.
func queueRegistryRevert(run *pgdb.Runner, chainID, rollbackTo int64, table, keyColumn, kind string) {
	values := map[string]any{"chain": chainID, "to": rollbackTo, "kind": kind}
	run.Exec(`DELETE FROM `+table+` AS t USING registry_undo AS u
		WHERE u.chain_id = @chain AND u.block_height > @to AND u.kind = @kind
		  AND t.chain_id = u.chain_id AND t.`+keyColumn+` = u.key`, values)
	run.Exec(`INSERT INTO `+table+`
		SELECT (jsonb_populate_record(NULL::`+table+`, u.prev)).*
		FROM (
			SELECT DISTINCT ON (key) prev FROM registry_undo
			WHERE chain_id = @chain AND block_height > @to AND kind = @kind
			ORDER BY key, block_height
		) AS u
		WHERE u.prev IS NOT NULL`, values)
}

func queueTokens(run *pgdb.Runner, chainID, height int64, tokens []model.Token) error {
	count := len(tokens)
	addresses, standards, recognitions := make([]string, count), make([]string, count), make([]string, count)
	complete, created := make([]bool, count), make([]bool, count)
	names, symbols, metadata, deployments := make([]*string, count), make([]*string, count), make([]*string, count), make([]*string, count)
	decimals := make([]*int16, count)
	sourceHeights := make([]int64, count)
	for index, token := range tokens {
		addresses[index], standards[index], recognitions[index] = token.Address, token.Standard, token.Recognition
		complete[index], created[index], sourceHeights[index] = token.MetadataComplete, token.Created, token.SourceHeight
		names[index], symbols[index], metadata[index] = token.Name, token.Symbol, pgdb.JSONText(token.Metadata)
		if token.Decimals != nil {
			value := int16(*token.Decimals)
			decimals[index] = &value
		}
		if token.Deployment != nil {
			raw, err := json.Marshal(token.Deployment)
			if err != nil {
				return fmt.Errorf("postgres store: encode deployment of %s: %w", token.Address, err)
			}
			deployments[index] = pgdb.JSONText(raw)
		}
	}
	queueUndo(run, chainID, height, undoToken, "registry_tokens", "address", addresses)
	// Older metadata never replaces newer; a reported deployment is kept
	// whatever its height.
	run.Exec(`INSERT INTO registry_tokens AS t (chain_id, address, standard, recognition, metadata_complete,
			name, symbol, decimals, metadata, created, deployment, source_height, first_height, updated_height)
		SELECT @chain, address, standard, recognition, complete, name, symbol, decimals, metadata::jsonb,
			created, deployment::jsonb, source_height, @height, @height
		FROM unnest(CAST(@addresses AS text[]), CAST(@standards AS text[]), CAST(@recognitions AS text[]), CAST(@complete AS bool[]), CAST(@names AS text[]),
			CAST(@symbols AS text[]), CAST(@decimals AS int2[]), CAST(@metadata AS text[]), CAST(@created AS bool[]), CAST(@deployments AS text[]),
			CAST(@source_heights AS bigint[]))
			AS f(address, standard, recognition, complete, name, symbol, decimals, metadata, created, deployment, source_height)
		ON CONFLICT (chain_id, address) DO UPDATE SET standard = EXCLUDED.standard,
			recognition = EXCLUDED.recognition, metadata_complete = EXCLUDED.metadata_complete,
			name = EXCLUDED.name, symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals,
			metadata = EXCLUDED.metadata, created = t.created OR EXCLUDED.created,
			deployment = COALESCE(t.deployment, EXCLUDED.deployment),
			source_height = EXCLUDED.source_height, updated_height = EXCLUDED.updated_height
		WHERE EXCLUDED.source_height >= t.source_height`, map[string]any{
		"chain": chainID, "height": height, "addresses": pgdb.Array(addresses), "standards": pgdb.Array(standards),
		"recognitions": pgdb.Array(recognitions), "complete": pgdb.Array(complete), "names": pgdb.Array(names),
		"symbols": pgdb.Array(symbols), "decimals": pgdb.Array(decimals), "metadata": pgdb.Array(metadata),
		"created": pgdb.Array(created), "deployments": pgdb.Array(deployments), "source_heights": pgdb.Array(sourceHeights),
	})
	run.Exec(`UPDATE registry_tokens AS t SET created = true, deployment = COALESCE(t.deployment, f.deployment::jsonb)
		FROM unnest(CAST(@addresses AS text[]), CAST(@created AS bool[]), CAST(@deployments AS text[])) AS f(address, created, deployment)
		WHERE f.created AND t.chain_id = @chain AND t.address = f.address
		  AND (NOT t.created OR (t.deployment IS NULL AND f.deployment IS NOT NULL))`, map[string]any{
		"chain": chainID, "addresses": pgdb.Array(addresses), "created": pgdb.Array(created), "deployments": pgdb.Array(deployments),
	})
	return nil
}

func queuePools(run *pgdb.Runner, chainID, height int64, pools []model.Pool) error {
	count := len(pools)
	ids, protocols, dexes, tokens := make([]string, count), make([]string, count), make([]string, count), make([]string, count)
	created := make([]bool, count)
	sourceHeights, sourceTimes := make([]int64, count), make([]int64, count)
	metadata := make([]*string, count)
	for index, pool := range pools {
		ids[index], protocols[index], dexes[index] = pool.PoolID, pool.Protocol, pool.DexAddress
		created[index], sourceHeights[index], sourceTimes[index] = pool.Created, pool.SourceHeight, pool.SourceTime
		metadata[index] = pgdb.JSONText(pool.Metadata)
		raw, err := json.Marshal(append([]string{}, pool.Tokens...))
		if err != nil {
			return fmt.Errorf("postgres store: encode tokens of pool %s: %w", pool.PoolID, err)
		}
		tokens[index] = string(raw)
	}
	queueUndo(run, chainID, height, undoPool, "registry_pools", "pool_id", ids)
	run.Exec(`INSERT INTO registry_pools AS t (chain_id, pool_id, protocol, dex_address, tokens, created,
			source_height, source_time, metadata, first_height, updated_height)
		SELECT @chain, pool_id, protocol, dex_address, ARRAY(SELECT jsonb_array_elements_text(tokens::jsonb)),
			created, source_height, source_time, metadata::jsonb, @height, @height
		FROM unnest(CAST(@ids AS text[]), CAST(@protocols AS text[]), CAST(@dexes AS text[]), CAST(@tokens AS text[]), CAST(@created AS bool[]),
			CAST(@source_heights AS bigint[]), CAST(@source_times AS bigint[]), CAST(@metadata AS text[]))
			AS f(pool_id, protocol, dex_address, tokens, created, source_height, source_time, metadata)
		ON CONFLICT (chain_id, pool_id) DO UPDATE SET protocol = EXCLUDED.protocol,
			dex_address = EXCLUDED.dex_address, tokens = EXCLUDED.tokens, created = t.created OR EXCLUDED.created,
			source_height = EXCLUDED.source_height, source_time = EXCLUDED.source_time,
			metadata = COALESCE(EXCLUDED.metadata, t.metadata), updated_height = EXCLUDED.updated_height
		WHERE EXCLUDED.source_height >= t.source_height`, map[string]any{
		"chain": chainID, "height": height, "ids": pgdb.Array(ids), "protocols": pgdb.Array(protocols), "dexes": pgdb.Array(dexes),
		"tokens": pgdb.Array(tokens), "created": pgdb.Array(created), "source_heights": pgdb.Array(sourceHeights),
		"source_times": pgdb.Array(sourceTimes), "metadata": pgdb.Array(metadata),
	})
	run.Exec(`UPDATE registry_pools AS t SET created = true
		FROM unnest(CAST(@ids AS text[]), CAST(@created AS bool[])) AS f(pool_id, created)
		WHERE f.created AND NOT t.created AND t.chain_id = @chain AND t.pool_id = f.pool_id`, map[string]any{
		"chain": chainID, "ids": pgdb.Array(ids), "created": pgdb.Array(created),
	})
	return nil
}

// queueUndo records the rows keys held before this block. The first write
// at a height holds the image to revert to, so an existing row wins.
func queueUndo(run *pgdb.Runner, chainID, height int64, kind, table, keyColumn string, keys []string) {
	run.Exec(`INSERT INTO registry_undo (chain_id, block_height, kind, key, prev)
		SELECT @chain, @height, @kind, k.key, to_jsonb(t)
		FROM unnest(CAST(@keys AS text[])) AS k(key)
		LEFT JOIN `+table+` AS t ON t.chain_id = @chain AND t.`+keyColumn+` = k.key
		ON CONFLICT DO NOTHING`, map[string]any{"chain": chainID, "height": height, "kind": kind, "keys": pgdb.Array(keys)})
}
