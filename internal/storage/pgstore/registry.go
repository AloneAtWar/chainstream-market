package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/jackc/pgx/v5"
)

// registrySchema creates the market-registry tables. Metadata columns hold
// the JSON chainstream-evm published; name, symbol and decimals are copied
// out of it so they can be queried.
var registrySchema = []string{
	`CREATE TABLE IF NOT EXISTS registry_checkpoints (
		chain_id       BIGINT PRIMARY KEY,
		kafka_offset   BIGINT NOT NULL,
		block_height   BIGINT NOT NULL,
		block_hash     TEXT NOT NULL,
		pruned_through BIGINT NOT NULL,
		updated_at     TIMESTAMPTZ NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS registry_tokens (
		chain_id          BIGINT NOT NULL,
		address           TEXT NOT NULL,
		standard          TEXT NOT NULL,
		recognition       TEXT NOT NULL,
		metadata_complete BOOLEAN NOT NULL,
		name              TEXT,
		symbol            TEXT,
		decimals          SMALLINT,
		metadata          JSONB,
		created           BOOLEAN NOT NULL,
		deployment        JSONB,
		source_height     BIGINT NOT NULL,
		first_height      BIGINT NOT NULL,
		updated_height    BIGINT NOT NULL,
		PRIMARY KEY (chain_id, address)
	)`,
	`CREATE INDEX IF NOT EXISTS registry_tokens_symbol ON registry_tokens (chain_id, lower(symbol))`,
	`CREATE TABLE IF NOT EXISTS registry_pools (
		chain_id       BIGINT NOT NULL,
		pool_id        TEXT NOT NULL,
		protocol       TEXT NOT NULL,
		dex_address    TEXT NOT NULL,
		tokens         TEXT[] NOT NULL,
		created        BOOLEAN NOT NULL,
		source_height  BIGINT NOT NULL,
		source_time    BIGINT NOT NULL,
		metadata       JSONB,
		first_height   BIGINT NOT NULL,
		updated_height BIGINT NOT NULL,
		PRIMARY KEY (chain_id, pool_id)
	)`,
	`CREATE INDEX IF NOT EXISTS registry_pools_tokens ON registry_pools USING GIN (tokens)`,
	// Each row holds a token or pool row as it was before block_height
	// first changed it, or NULL when that block created it.
	`CREATE TABLE IF NOT EXISTS registry_undo (
		chain_id     BIGINT NOT NULL,
		block_height BIGINT NOT NULL,
		kind         TEXT NOT NULL,
		key          TEXT NOT NULL,
		prev         JSONB,
		PRIMARY KEY (chain_id, block_height, kind, key)
	)`,
}

const (
	undoToken = "token"
	undoPool  = "pool"
)

func (store *Store) LoadRegistryCheckpoint(ctx context.Context, chainID uint64) (storage.Checkpoint, int64, bool, error) {
	checkpoint := storage.Checkpoint{ChainID: chainID}
	var prunedThrough int64
	err := store.pool.QueryRow(ctx,
		`SELECT kafka_offset, block_height, block_hash, pruned_through, updated_at
		 FROM registry_checkpoints WHERE chain_id = $1`, int64(chainID),
	).Scan(&checkpoint.Offset, &checkpoint.Height, &checkpoint.Hash, &prunedThrough, &checkpoint.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.Checkpoint{}, 0, false, nil
	}
	if err != nil {
		return storage.Checkpoint{}, 0, false, fmt.Errorf("postgres store: load registry checkpoint: %w", err)
	}
	checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
	return checkpoint, prunedThrough, true, nil
}

// CommitRegistry applies change and saves checkpoint in one transaction.
func (store *Store) CommitRegistry(ctx context.Context, checkpoint storage.Checkpoint, change storage.RegistryChange) (err error) {
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
		queueRegistryRevert(batch, chainID, change.RevertTo, "registry_tokens", "address", undoToken)
		queueRegistryRevert(batch, chainID, change.RevertTo, "registry_pools", "pool_id", undoPool)
		batch.Queue(`DELETE FROM registry_undo WHERE chain_id = $1 AND block_height > $2`, chainID, change.RevertTo)
	}
	if len(change.Tokens) > 0 {
		if err := queueTokens(batch, chainID, change.Height, change.Tokens); err != nil {
			return err
		}
	}
	if len(change.Pools) > 0 {
		if err := queuePools(batch, chainID, change.Height, change.Pools); err != nil {
			return err
		}
	}
	batch.Queue(`DELETE FROM registry_undo WHERE chain_id = $1 AND block_height <= $2`, chainID, change.PrunedThrough)
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	batch.Queue(`INSERT INTO registry_checkpoints (chain_id, kafka_offset, block_height, block_hash, pruned_through, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (chain_id) DO UPDATE SET kafka_offset = EXCLUDED.kafka_offset,
			block_height = EXCLUDED.block_height, block_hash = EXCLUDED.block_hash,
			pruned_through = GREATEST(registry_checkpoints.pruned_through, EXCLUDED.pruned_through),
			updated_at = EXCLUDED.updated_at`,
		chainID, checkpoint.Offset, checkpoint.Height, checkpoint.Hash, change.PrunedThrough, updatedAt)

	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("postgres store: commit registry chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres store: commit: %w", err)
	}
	return nil
}

// queueRegistryRevert puts every row the reverted blocks touched back as it
// was before the first of them: the row is deleted, then the oldest undo
// image above rollbackTo is restored unless the row did not exist yet.
func queueRegistryRevert(batch *pgx.Batch, chainID, rollbackTo int64, table, keyColumn, kind string) {
	batch.Queue(`DELETE FROM `+table+` AS t USING registry_undo AS u
		WHERE u.chain_id = $1 AND u.block_height > $2 AND u.kind = $3
		  AND t.chain_id = u.chain_id AND t.`+keyColumn+` = u.key`, chainID, rollbackTo, kind)
	batch.Queue(`INSERT INTO `+table+`
		SELECT (jsonb_populate_record(NULL::`+table+`, u.prev)).*
		FROM (
			SELECT DISTINCT ON (key) prev FROM registry_undo
			WHERE chain_id = $1 AND block_height > $2 AND kind = $3
			ORDER BY key, block_height
		) AS u
		WHERE u.prev IS NOT NULL`, chainID, rollbackTo, kind)
}

func queueTokens(batch *pgx.Batch, chainID, height int64, tokens []storage.Token) error {
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
	queueUndo(batch, chainID, height, undoToken, "registry_tokens", "address", addresses)
	// Older metadata never replaces newer; a reported deployment is kept
	// whatever its height.
	batch.Queue(`INSERT INTO registry_tokens AS t (chain_id, address, standard, recognition, metadata_complete,
			name, symbol, decimals, metadata, created, deployment, source_height, first_height, updated_height)
		SELECT $1, address, standard, recognition, complete, name, symbol, decimals, metadata::jsonb,
			created, deployment::jsonb, source_height, $2, $2
		FROM unnest($3::text[], $4::text[], $5::text[], $6::bool[], $7::text[], $8::text[], $9::int2[],
			$10::text[], $11::bool[], $12::text[], $13::bigint[])
			AS f(address, standard, recognition, complete, name, symbol, decimals, metadata, created, deployment, source_height)
		ON CONFLICT (chain_id, address) DO UPDATE SET standard = EXCLUDED.standard,
			recognition = EXCLUDED.recognition, metadata_complete = EXCLUDED.metadata_complete,
			name = EXCLUDED.name, symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals,
			metadata = EXCLUDED.metadata, created = t.created OR EXCLUDED.created,
			deployment = COALESCE(t.deployment, EXCLUDED.deployment),
			source_height = EXCLUDED.source_height, updated_height = EXCLUDED.updated_height
		WHERE EXCLUDED.source_height >= t.source_height`,
		chainID, height, addresses, standards, recognitions, complete, names, symbols, decimals,
		metadata, created, deployments, sourceHeights)
	batch.Queue(`UPDATE registry_tokens AS t SET created = true, deployment = COALESCE(t.deployment, f.deployment::jsonb)
		FROM unnest($2::text[], $3::bool[], $4::text[]) AS f(address, created, deployment)
		WHERE f.created AND t.chain_id = $1 AND t.address = f.address
		  AND (NOT t.created OR (t.deployment IS NULL AND f.deployment IS NOT NULL))`,
		chainID, addresses, created, deployments)
	return nil
}

func queuePools(batch *pgx.Batch, chainID, height int64, pools []storage.Pool) error {
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
	queueUndo(batch, chainID, height, undoPool, "registry_pools", "pool_id", ids)
	batch.Queue(`INSERT INTO registry_pools AS t (chain_id, pool_id, protocol, dex_address, tokens, created,
			source_height, source_time, metadata, first_height, updated_height)
		SELECT $1, pool_id, protocol, dex_address, ARRAY(SELECT jsonb_array_elements_text(tokens::jsonb)),
			created, source_height, source_time, metadata::jsonb, $2, $2
		FROM unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::bool[], $8::bigint[], $9::bigint[], $10::text[])
			AS f(pool_id, protocol, dex_address, tokens, created, source_height, source_time, metadata)
		ON CONFLICT (chain_id, pool_id) DO UPDATE SET protocol = EXCLUDED.protocol,
			dex_address = EXCLUDED.dex_address, tokens = EXCLUDED.tokens, created = t.created OR EXCLUDED.created,
			source_height = EXCLUDED.source_height, source_time = EXCLUDED.source_time,
			metadata = COALESCE(EXCLUDED.metadata, t.metadata), updated_height = EXCLUDED.updated_height
		WHERE EXCLUDED.source_height >= t.source_height`,
		chainID, height, ids, protocols, dexes, tokens, created, sourceHeights, sourceTimes, metadata)
	batch.Queue(`UPDATE registry_pools AS t SET created = true
		FROM unnest($2::text[], $3::bool[]) AS f(pool_id, created)
		WHERE f.created AND NOT t.created AND t.chain_id = $1 AND t.pool_id = f.pool_id`,
		chainID, ids, created)
	return nil
}

// queueUndo records the rows keys held before this block. The first write
// at a height holds the image to revert to, so an existing row wins.
func queueUndo(batch *pgx.Batch, chainID, height int64, kind, table, keyColumn string, keys []string) {
	batch.Queue(`INSERT INTO registry_undo (chain_id, block_height, kind, key, prev)
		SELECT $1, $2, $3, k.key, to_jsonb(t)
		FROM unnest($4::text[]) AS k(key)
		LEFT JOIN `+table+` AS t ON t.chain_id = $1 AND t.`+keyColumn+` = k.key
		ON CONFLICT DO NOTHING`, chainID, height, kind, keys)
}

func jsonText(raw []byte) *string {
	if len(raw) == 0 {
		return nil
	}
	text := string(raw)
	return &text
}

const tokenColumns = `address, standard, recognition, metadata_complete, name, symbol, decimals,
	metadata::text, created, deployment::text, source_height, first_height, updated_height`

func (store *Store) Token(ctx context.Context, chainID uint64, address string) (storage.Token, bool, error) {
	rows, err := store.pool.Query(ctx,
		`SELECT `+tokenColumns+` FROM registry_tokens WHERE chain_id = $1 AND address = $2`, int64(chainID), address)
	if err != nil {
		return storage.Token{}, false, fmt.Errorf("postgres store: token: %w", err)
	}
	tokens, err := pgx.CollectRows(rows, scanToken)
	if err != nil {
		return storage.Token{}, false, fmt.Errorf("postgres store: read token: %w", err)
	}
	if len(tokens) == 0 {
		return storage.Token{}, false, nil
	}
	return tokens[0], true, nil
}

func scanToken(row pgx.CollectableRow) (storage.Token, error) {
	var token storage.Token
	var decimals *int16
	var metadata, deployment *string
	if err := row.Scan(&token.Address, &token.Standard, &token.Recognition, &token.MetadataComplete,
		&token.Name, &token.Symbol, &decimals, &metadata, &token.Created, &deployment,
		&token.SourceHeight, &token.FirstHeight, &token.UpdatedHeight); err != nil {
		return token, err
	}
	if decimals != nil {
		value := uint8(*decimals)
		token.Decimals = &value
	}
	if metadata != nil {
		token.Metadata = []byte(*metadata)
	}
	if deployment != nil {
		token.Deployment = &storage.Deployment{}
		if err := json.Unmarshal([]byte(*deployment), token.Deployment); err != nil {
			return token, fmt.Errorf("deployment of %s: %w", token.Address, err)
		}
	}
	return token, nil
}

const poolColumns = `pool_id, protocol, dex_address, tokens, created, source_height, source_time,
	metadata::text, first_height, updated_height`

func (store *Store) Pool(ctx context.Context, chainID uint64, poolID string) (storage.Pool, bool, error) {
	rows, err := store.pool.Query(ctx,
		`SELECT `+poolColumns+` FROM registry_pools WHERE chain_id = $1 AND pool_id = $2`, int64(chainID), poolID)
	if err != nil {
		return storage.Pool{}, false, fmt.Errorf("postgres store: pool: %w", err)
	}
	pools, err := pgx.CollectRows(rows, scanPool)
	if err != nil {
		return storage.Pool{}, false, fmt.Errorf("postgres store: read pool: %w", err)
	}
	if len(pools) == 0 {
		return storage.Pool{}, false, nil
	}
	return pools[0], true, nil
}

func (store *Store) TokenPools(ctx context.Context, chainID uint64, token string, limit int) ([]storage.Pool, error) {
	rows, err := store.pool.Query(ctx,
		`SELECT `+poolColumns+` FROM registry_pools
		 WHERE chain_id = $1 AND tokens @> ARRAY[$2::text]
		 ORDER BY source_height DESC, pool_id LIMIT $3`, int64(chainID), token, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres store: token pools: %w", err)
	}
	pools, err := pgx.CollectRows(rows, scanPool)
	if err != nil {
		return nil, fmt.Errorf("postgres store: read pools: %w", err)
	}
	return pools, nil
}

func scanPool(row pgx.CollectableRow) (storage.Pool, error) {
	var pool storage.Pool
	var metadata *string
	if err := row.Scan(&pool.PoolID, &pool.Protocol, &pool.DexAddress, &pool.Tokens, &pool.Created,
		&pool.SourceHeight, &pool.SourceTime, &metadata, &pool.FirstHeight, &pool.UpdatedHeight); err != nil {
		return pool, err
	}
	if metadata != nil {
		pool.Metadata = []byte(*metadata)
	}
	return pool, nil
}

var (
	_ storage.RegistryStore  = (*Store)(nil)
	_ storage.RegistryReader = (*Store)(nil)
)
