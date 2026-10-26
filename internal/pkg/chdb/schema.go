package chdb

import (
	"context"

	"gorm.io/gorm"
)

// MarketSchema creates market-engine's tables. Every table leads its sort key with
// chain_id and the block height column that WriteBlock and Rewind delete by.
//
// candles keeps every version of a bucket, one per block that changed it, so
// a rewind restores the previous version by deleting the newer ones. Read the
// current bucket with argMax(..., update_height) or LIMIT 1 BY.
var MarketSchema = []string{
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
	// A retention TTL drops a month's parts whole once all their rows have
	// expired, instead of rewriting parts that are partly expired.
	`ALTER TABLE trades MODIFY SETTING ttl_only_drop_parts = 1`,

	`CREATE TABLE IF NOT EXISTS token_prices (
		chain_id      UInt64,
		token         String,
		block_height  Int64,
		block_time    DateTime('UTC'),
		price_usd     Decimal256(36),
		source        Enum8('unknown' = 0, 'anchor' = 1, 'graph' = 2),
		hops          UInt8,
		liquidity_usd Decimal256(36),
		revision_height Int64
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(block_time)
	ORDER BY (chain_id, token, block_height)`,

	// revision_height is the block that wrote the row: block_height, or the
	// later block that recomputed the price of a token whose decimals
	// arrived late.
	`ALTER TABLE token_prices ADD COLUMN IF NOT EXISTS revision_height Int64 DEFAULT block_height`,

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

// RawSchema creates ch-loader's tables. Every fact table leads its sort key with
// chain_id and block_height, the height of the block that published the
// fact, which is what replays and reverts delete by. Addresses are lowercase.
var RawSchema = []string{
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
	// market-engine reads a token's trades back with hasAny(amount_assets, ...)
	// when the token's decimals arrive late.
	`ALTER TABLE raw_trades ADD INDEX IF NOT EXISTS raw_trades_assets amount_assets TYPE bloom_filter GRANULARITY 4`,

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

// Migrate runs schema's DDL. ClickHouse table engines, sort keys, partitions
// and skip indexes are not expressible as GORM model tags.
func Migrate(ctx context.Context, db *gorm.DB, schema []string) error {
	for _, statement := range schema {
		if err := db.WithContext(ctx).Exec(statement).Error; err != nil {
			return err
		}
	}
	return nil
}
