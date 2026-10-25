package chstore

// schema creates market-engine's tables. Every table leads its sort key with
// chain_id and the block height column that WriteBlock and Rewind delete by.
//
// candles keeps every version of a bucket, one per block that changed it, so
// a rewind restores the previous version by deleting the newer ones. Read the
// current bucket with argMax(..., update_height) or LIMIT 1 BY.
var schema = []string{
	`CREATE TABLE IF NOT EXISTS blocks (
		chain_id     UInt64,
		block_height Int64,
		block_hash   String,
		block_time   DateTime('UTC'),
		finalized    Bool
	) ENGINE = MergeTree
	ORDER BY (chain_id, block_height)`,

	`CREATE TABLE IF NOT EXISTS trades (
		chain_id          UInt64,
		block_height      Int64,
		block_time        DateTime('UTC'),
		transaction_hash  String,
		transaction_index Int64,
		log_index         UInt32,
		operation_index   UInt32,
		protocol          LowCardinality(String),
		pool_id           String,
		trader            String,
		side              Enum8('unknown' = 0, 'buy' = 1, 'sell' = 2, 'liquidity_added' = 3, 'liquidity_removed' = 4),
		base_token        String,
		quote_token       String,
		base_amount       Decimal256(36),
		quote_amount      Decimal256(36),
		price_quote       Decimal256(36),
		price_usd         Decimal256(36),
		value_usd         Decimal256(36),
		finalized         Bool,
		revision_height   Int64,
		outlier           Bool
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(block_time)
	ORDER BY (chain_id, block_height, log_index, operation_index)`,

	// revision_height is the block that wrote the row: block_height, or the
	// later block that replayed a trade held for its token's decimals.
	`ALTER TABLE trades ADD COLUMN IF NOT EXISTS revision_height Int64 DEFAULT block_height`,
	// outlier marks a swap priced too far from its base token's reference
	// price, which market-engine left out of the token's candles.
	`ALTER TABLE trades ADD COLUMN IF NOT EXISTS outlier Bool DEFAULT false`,

	// The API reads trades by base token or pool; skip indexes let it avoid
	// scanning every granule of a height-ordered table.
	`ALTER TABLE trades ADD INDEX IF NOT EXISTS trades_base_token base_token TYPE bloom_filter GRANULARITY 4`,
	`ALTER TABLE trades ADD INDEX IF NOT EXISTS trades_pool_id pool_id TYPE bloom_filter GRANULARITY 4`,

	`CREATE TABLE IF NOT EXISTS token_prices (
		chain_id      UInt64,
		token         String,
		block_height  Int64,
		block_time    DateTime('UTC'),
		price_usd     Decimal256(36),
		source        Enum8('unknown' = 0, 'anchor' = 1, 'graph' = 2),
		hops          UInt8,
		liquidity_usd Decimal256(36)
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(block_time)
	ORDER BY (chain_id, token, block_height)`,

	`CREATE TABLE IF NOT EXISTS candles (
		chain_id         UInt64,
		subject          Enum8('unknown' = 0, 'pool' = 1, 'token' = 2),
		key              String,
		interval_seconds UInt32,
		open_time        DateTime('UTC'),
		open             Decimal256(36),
		high             Decimal256(36),
		low              Decimal256(36),
		close            Decimal256(36),
		volume_base      Decimal256(36),
		volume_usd       Decimal256(36),
		trades           UInt32,
		update_height    Int64
	) ENGINE = MergeTree
	PARTITION BY (interval_seconds, toYYYYMM(open_time))
	ORDER BY (chain_id, subject, key, interval_seconds, open_time, update_height)`,
}
