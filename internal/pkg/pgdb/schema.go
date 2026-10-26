package pgdb

// engineSchema creates market-engine's tables. Undo rows are keyed by height first so that
// reverts and pruning delete a contiguous range.
var engineSchema = []string{
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

// rankingSchema creates the token rankings, a rollup that market-rankings
// rewrites from the ClickHouse output, holder_counts and registry_tokens.
// Each sort column has an index led by chain_id, so a page reads only the
// rows it returns.
var rankingSchema = []string{
	`CREATE TABLE IF NOT EXISTS ranking_checkpoints (
		chain_id     BIGINT PRIMARY KEY,
		head_time    TIMESTAMPTZ NOT NULL,
		refreshed_at TIMESTAMPTZ NOT NULL
	)`,
	// price_* are NULL for a token that traded in the window without a
	// price of its own.
	`CREATE TABLE IF NOT EXISTS token_rankings (
		chain_id         BIGINT NOT NULL,
		token            TEXT NOT NULL,
		price_usd        NUMERIC,
		price_height     BIGINT,
		price_time       TIMESTAMPTZ,
		liquidity_usd    NUMERIC NOT NULL DEFAULT 0,
		volume_24h_usd   NUMERIC NOT NULL DEFAULT 0,
		trades_24h       BIGINT NOT NULL DEFAULT 0,
		price_change_24h NUMERIC NOT NULL DEFAULT 0,
		holders          BIGINT NOT NULL DEFAULT 0,
		created_height   BIGINT,
		created_time     BIGINT,
		PRIMARY KEY (chain_id, token)
	)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_volume ON token_rankings (chain_id, volume_24h_usd, token)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_change ON token_rankings (chain_id, price_change_24h, token)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_liquidity ON token_rankings (chain_id, liquidity_usd, token)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_holders ON token_rankings (chain_id, holders, token)`,
	`CREATE INDEX IF NOT EXISTS token_rankings_created ON token_rankings (chain_id, created_height, token)
		WHERE created_height IS NOT NULL`,
	`CREATE INDEX IF NOT EXISTS token_rankings_price_time ON token_rankings (chain_id, price_time)`,
	// Symbol search matches by prefix, which a btree serves only with
	// pattern ops under a non-C collation.
	`CREATE INDEX IF NOT EXISTS registry_tokens_symbol_prefix
		ON registry_tokens (chain_id, lower(symbol) text_pattern_ops)`,
}
