package chraw

// schema creates ch-loader's tables. Every fact table leads its sort key with
// chain_id and block_height, the height of the block that published the
// fact, which is what replays and reverts delete by. Addresses are lowercase.
var schema = []string{
	`CREATE TABLE IF NOT EXISTS raw_transfers (
		chain_id          UInt64,
		block_height      Int64,
		block_time        DateTime('UTC'),
		transaction_hash  String,
		transaction_index Int64,
		log_index         UInt32,
		token             String,
		from_address      String,
		to_address        String,
		amount            UInt256
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(block_time)
	ORDER BY (chain_id, block_height, log_index)`,

	// The API reads transfers by token or by either party. One index over
	// both parties lets has([from_address, to_address], holder) skip granules,
	// which separate indexes on an OR cannot.
	`ALTER TABLE raw_transfers ADD INDEX IF NOT EXISTS raw_transfers_token token TYPE bloom_filter GRANULARITY 4`,
	`ALTER TABLE raw_transfers ADD INDEX IF NOT EXISTS raw_transfers_party [from_address, to_address] TYPE bloom_filter GRANULARITY 4`,

	`CREATE TABLE IF NOT EXISTS raw_trades (
		chain_id              UInt64,
		block_height          Int64,
		block_time            DateTime('UTC'),
		transaction_hash      String,
		transaction_index     Int64,
		log_index             UInt32,
		operation_index       UInt32,
		protocol              LowCardinality(String),
		dex_address           String,
		pool_id               String,
		action                LowCardinality(String),
		sender                String,
		trader                String,
		recipient             String,
		amount_assets         Array(String),
		amount_raw            Array(String),
		amount_values         Array(String),
		amount_decimals       Array(UInt32),
		amount_decimals_known Array(Bool),
		reserves_after        Array(String),
		sqrt_price_x96        String,
		liquidity             String,
		tick                  String,
		active_bin_id         Nullable(UInt32)
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(block_time)
	ORDER BY (chain_id, block_height, log_index, operation_index)`,

	`ALTER TABLE raw_trades ADD INDEX IF NOT EXISTS raw_trades_pool_id pool_id TYPE bloom_filter GRANULARITY 4`,

	`CREATE TABLE IF NOT EXISTS oracle_updates (
		chain_id          UInt64,
		block_height      Int64,
		block_time        DateTime('UTC'),
		transaction_hash  String,
		transaction_index Int64,
		log_index         UInt32,
		feed              String,
		answer            Int256,
		round_id          UInt256,
		updated_at        Int64
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(block_time)
	ORDER BY (chain_id, block_height, log_index)`,

	`CREATE TABLE IF NOT EXISTS asset_facts (
		chain_id          UInt64,
		block_height      Int64,
		block_time        DateTime('UTC'),
		address           String,
		standard          LowCardinality(String),
		recognition       LowCardinality(String),
		metadata_complete Bool,
		type              LowCardinality(String),
		source_height     Int64,
		deployment_height Int64,
		deployment_time   Int64,
		deployment_tx_index Int64,
		deployment_tx_hash String,
		deployment_trace_id String,
		metadata_json     String
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(block_time)
	ORDER BY (chain_id, block_height, address)`,

	`CREATE TABLE IF NOT EXISTS pool_facts (
		chain_id        UInt64,
		block_height    Int64,
		block_time      DateTime('UTC'),
		pool_id         String,
		protocol        LowCardinality(String),
		dex_address     String,
		token_addresses Array(String),
		source_height   Int64,
		source_time     Int64,
		created         Bool,
		metadata_json   String
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(block_time)
	ORDER BY (chain_id, block_height, pool_id)`,

	// A retention TTL drops a month's parts whole once all their rows have
	// expired, instead of rewriting parts that are partly expired.
	`ALTER TABLE raw_transfers MODIFY SETTING ttl_only_drop_parts = 1`,
	`ALTER TABLE raw_trades MODIFY SETTING ttl_only_drop_parts = 1`,
	`ALTER TABLE oracle_updates MODIFY SETTING ttl_only_drop_parts = 1`,

	// One row per flush. The newest offset is the checkpoint; older rows
	// merge away.
	`CREATE TABLE IF NOT EXISTS raw_checkpoints (
		chain_id     UInt64,
		kafka_offset Int64,
		block_height Int64,
		block_hash   String,
		updated_at   DateTime64(3, 'UTC')
	) ENGINE = ReplacingMergeTree(kafka_offset)
	ORDER BY chain_id`,
}
