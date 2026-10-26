package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"gorm.io/gorm"
)

const (
	undoToken = "token"
	undoPool  = "pool"
)

func (store *Store) LoadRegistryCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, int64, bool, error) {
	return loadCheckpoint(store.db.WithContext(ctx), registryCheckpoints, chainID)
}

// CommitRegistry applies change and saves checkpoint in one transaction.
func (store *Store) CommitRegistry(ctx context.Context, checkpoint chainsync.Checkpoint, change model.RegistryChange) error {
	if checkpoint.ChainID == 0 {
		return errors.New("postgres store: checkpoint chain id is required")
	}
	chainID := int64(checkpoint.ChainID)
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := &runner{tx: tx}
		if change.Reverted {
			queueRegistryRevert(run, chainID, change.RevertTo, "registry_tokens", "address", undoToken)
			queueRegistryRevert(run, chainID, change.RevertTo, "registry_pools", "pool_id", undoPool)
			run.exec(`DELETE FROM registry_undo WHERE chain_id = ? AND block_height > ?`, chainID, change.RevertTo)
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
		run.exec(`DELETE FROM registry_undo WHERE chain_id = ? AND block_height <= ?`, chainID, change.PrunedThrough)
		if run.err != nil {
			return run.err
		}
		return saveCheckpoint(tx, registryCheckpoints, checkpoint, change.PrunedThrough)
	})
	if err != nil {
		return fmt.Errorf("postgres store: commit registry chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
	}
	return nil
}

// queueRegistryRevert puts every row the reverted blocks touched back as it
// was before the first of them: the row is deleted, then the oldest undo
// image above rollbackTo is restored unless the row did not exist yet.
func queueRegistryRevert(run *runner, chainID, rollbackTo int64, table, keyColumn, kind string) {
	values := map[string]any{"chain": chainID, "to": rollbackTo, "kind": kind}
	run.exec(`DELETE FROM `+table+` AS t USING registry_undo AS u
		WHERE u.chain_id = @chain AND u.block_height > @to AND u.kind = @kind
		  AND t.chain_id = u.chain_id AND t.`+keyColumn+` = u.key`, values)
	run.exec(`INSERT INTO `+table+`
		SELECT (jsonb_populate_record(NULL::`+table+`, u.prev)).*
		FROM (
			SELECT DISTINCT ON (key) prev FROM registry_undo
			WHERE chain_id = @chain AND block_height > @to AND kind = @kind
			ORDER BY key, block_height
		) AS u
		WHERE u.prev IS NOT NULL`, values)
}

func queueTokens(run *runner, chainID, height int64, tokens []model.Token) error {
	count := len(tokens)
	addresses, standards, recognitions := make([]string, count), make([]string, count), make([]string, count)
	complete, created := make([]bool, count), make([]bool, count)
	names, symbols, metadata, deployments := make([]*string, count), make([]*string, count), make([]*string, count), make([]*string, count)
	decimals := make([]*int16, count)
	sourceHeights := make([]int64, count)
	for index, token := range tokens {
		addresses[index], standards[index], recognitions[index] = token.Address, token.Standard, token.Recognition
		complete[index], created[index], sourceHeights[index] = token.MetadataComplete, token.Created, token.SourceHeight
		names[index], symbols[index], metadata[index] = token.Name, token.Symbol, jsonText(token.Metadata)
		if token.Decimals != nil {
			value := int16(*token.Decimals)
			decimals[index] = &value
		}
		if token.Deployment != nil {
			raw, err := json.Marshal(token.Deployment)
			if err != nil {
				return fmt.Errorf("postgres store: encode deployment of %s: %w", token.Address, err)
			}
			deployments[index] = jsonText(raw)
		}
	}
	queueUndo(run, chainID, height, undoToken, "registry_tokens", "address", addresses)
	// Older metadata never replaces newer; a reported deployment is kept
	// whatever its height.
	run.exec(`INSERT INTO registry_tokens AS t (chain_id, address, standard, recognition, metadata_complete,
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
		"chain": chainID, "height": height, "addresses": array(addresses), "standards": array(standards),
		"recognitions": array(recognitions), "complete": array(complete), "names": array(names),
		"symbols": array(symbols), "decimals": array(decimals), "metadata": array(metadata),
		"created": array(created), "deployments": array(deployments), "source_heights": array(sourceHeights),
	})
	run.exec(`UPDATE registry_tokens AS t SET created = true, deployment = COALESCE(t.deployment, f.deployment::jsonb)
		FROM unnest(CAST(@addresses AS text[]), CAST(@created AS bool[]), CAST(@deployments AS text[])) AS f(address, created, deployment)
		WHERE f.created AND t.chain_id = @chain AND t.address = f.address
		  AND (NOT t.created OR (t.deployment IS NULL AND f.deployment IS NOT NULL))`, map[string]any{
		"chain": chainID, "addresses": array(addresses), "created": array(created), "deployments": array(deployments),
	})
	return nil
}

func queuePools(run *runner, chainID, height int64, pools []model.Pool) error {
	count := len(pools)
	ids, protocols, dexes, tokens := make([]string, count), make([]string, count), make([]string, count), make([]string, count)
	created := make([]bool, count)
	sourceHeights, sourceTimes := make([]int64, count), make([]int64, count)
	metadata := make([]*string, count)
	for index, pool := range pools {
		ids[index], protocols[index], dexes[index] = pool.PoolID, pool.Protocol, pool.DexAddress
		created[index], sourceHeights[index], sourceTimes[index] = pool.Created, pool.SourceHeight, pool.SourceTime
		metadata[index] = jsonText(pool.Metadata)
		raw, err := json.Marshal(append([]string{}, pool.Tokens...))
		if err != nil {
			return fmt.Errorf("postgres store: encode tokens of pool %s: %w", pool.PoolID, err)
		}
		tokens[index] = string(raw)
	}
	queueUndo(run, chainID, height, undoPool, "registry_pools", "pool_id", ids)
	run.exec(`INSERT INTO registry_pools AS t (chain_id, pool_id, protocol, dex_address, tokens, created,
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
		"chain": chainID, "height": height, "ids": array(ids), "protocols": array(protocols), "dexes": array(dexes),
		"tokens": array(tokens), "created": array(created), "source_heights": array(sourceHeights),
		"source_times": array(sourceTimes), "metadata": array(metadata),
	})
	run.exec(`UPDATE registry_pools AS t SET created = true
		FROM unnest(CAST(@ids AS text[]), CAST(@created AS bool[])) AS f(pool_id, created)
		WHERE f.created AND NOT t.created AND t.chain_id = @chain AND t.pool_id = f.pool_id`, map[string]any{
		"chain": chainID, "ids": array(ids), "created": array(created),
	})
	return nil
}

// queueUndo records the rows keys held before this block. The first write
// at a height holds the image to revert to, so an existing row wins.
func queueUndo(run *runner, chainID, height int64, kind, table, keyColumn string, keys []string) {
	run.exec(`INSERT INTO registry_undo (chain_id, block_height, kind, key, prev)
		SELECT @chain, @height, @kind, k.key, to_jsonb(t)
		FROM unnest(CAST(@keys AS text[])) AS k(key)
		LEFT JOIN `+table+` AS t ON t.chain_id = @chain AND t.`+keyColumn+` = k.key
		ON CONFLICT DO NOTHING`, map[string]any{"chain": chainID, "height": height, "kind": kind, "keys": array(keys)})
}

func jsonText(raw []byte) *string {
	if len(raw) == 0 {
		return nil
	}
	text := string(raw)
	return &text
}

const tokenColumns = `address, standard, recognition, metadata_complete, name, symbol, decimals,
	metadata::text AS metadata, created, deployment::text AS deployment, source_height, first_height, updated_height`

func (store *Store) Token(ctx context.Context, chainID uint64, address string) (model.Token, bool, error) {
	var row tokenRow
	err := store.db.WithContext(ctx).Select(tokenColumns).
		Where("chain_id = ? AND address = ?", int64(chainID), address).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.Token{}, false, nil
	}
	if err != nil {
		return model.Token{}, false, fmt.Errorf("postgres store: token: %w", err)
	}
	token, err := row.token()
	if err != nil {
		return model.Token{}, false, fmt.Errorf("postgres store: read token: %w", err)
	}
	return token, true, nil
}

func (row tokenRow) token() (model.Token, error) {
	token := model.Token{
		Address: row.Address, Standard: row.Standard, Recognition: row.Recognition,
		MetadataComplete: row.MetadataComplete, Name: row.Name, Symbol: row.Symbol, Created: row.Created,
		SourceHeight: row.SourceHeight, FirstHeight: row.FirstHeight, UpdatedHeight: row.UpdatedHeight,
	}
	if row.Decimals != nil {
		value := uint8(*row.Decimals)
		token.Decimals = &value
	}
	if row.Metadata != nil {
		token.Metadata = []byte(*row.Metadata)
	}
	if row.Deployment != nil {
		token.Deployment = &model.Deployment{}
		if err := json.Unmarshal([]byte(*row.Deployment), token.Deployment); err != nil {
			return token, fmt.Errorf("deployment of %s: %w", token.Address, err)
		}
	}
	return token, nil
}

const poolColumns = `pool_id, protocol, dex_address, tokens, created, source_height, source_time,
	metadata::text AS metadata, first_height, updated_height`

func (store *Store) Pool(ctx context.Context, chainID uint64, poolID string) (model.Pool, bool, error) {
	var row poolRow
	err := store.db.WithContext(ctx).Select(poolColumns).
		Where("chain_id = ? AND pool_id = ?", int64(chainID), poolID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.Pool{}, false, nil
	}
	if err != nil {
		return model.Pool{}, false, fmt.Errorf("postgres store: pool: %w", err)
	}
	return row.pool(), true, nil
}

func (store *Store) TokenPools(ctx context.Context, chainID uint64, token string, limit int) ([]model.Pool, error) {
	var rows []poolRow
	err := store.db.WithContext(ctx).Select(poolColumns).
		Where("chain_id = ? AND tokens @> ?::text[]", int64(chainID), array([]string{token})).
		Order("source_height DESC, pool_id").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: token pools: %w", err)
	}
	pools := make([]model.Pool, len(rows))
	for index, row := range rows {
		pools[index] = row.pool()
	}
	return pools, nil
}

func (row poolRow) pool() model.Pool {
	pool := model.Pool{
		PoolID: row.PoolID, Protocol: row.Protocol, DexAddress: row.DexAddress, Tokens: []string(row.Tokens),
		Created: row.Created, SourceHeight: row.SourceHeight, SourceTime: row.SourceTime,
		FirstHeight: row.FirstHeight, UpdatedHeight: row.UpdatedHeight,
	}
	if row.Metadata != nil {
		pool.Metadata = []byte(*row.Metadata)
	}
	return pool
}

var (
	_ storage.RegistryStore  = (*Store)(nil)
	_ storage.RegistryReader = (*Store)(nil)
)
