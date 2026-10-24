# chainstream-market

Market data services built on the [chainstream-evm](https://github.com/AloneAtWar/chainstream-evm) block stream: trade orientation, USD pricing, candles, holders and raw transfers for ETH, BSC and Base.

## Layout

| Path | What it is |
| --- | --- |
| `proto/` | Separate Go module `github.com/AloneAtWar/chainstream-market/proto` holding the `stream.v1` wire format. It depends only on protobuf so producers can import it cheaply. |
| `cmd/market-engine` | Consumes one chainstream-evm topic per chain and writes derived market data. |
| `cmd/market-api` | HTTP API over the ClickHouse store, plus holders from PostgreSQL. |
| `cmd/holder-indexer` | Consumes the same chain topics and keeps ERC-20 balances per holder in PostgreSQL. |
| `cmd/ch-loader` | Consumes the same chain topics and copies their raw facts, every ERC-20 transfer among them, into ClickHouse. |
| `internal/api` | market-api handlers. |
| `internal/engine` | Per-chain ordered apply loop: reverts, continuity checks and checkpoints, driving a `Handler`. |
| `internal/holders` | holder-indexer's handler: transfer deltas, balanceOf seeding and reconciliation. |
| `internal/chloader` | ch-loader's handler: converts each block's facts and writes them in batches. |
| `internal/rpc` | Minimal JSON-RPC client for batched `balanceOf` calls. |
| `internal/market` | Per-chain processor: pool state, base/quote orientation, the USD price graph and candles. |
| `internal/source` | Kafka reader for partition zero of a chain topic. |
| `internal/storage` | Output models (oriented trades, token prices, candles), store interfaces and the in-memory backend. |
| `internal/storage/chstore` | ClickHouse output store and schema. |
| `internal/storage/chraw` | ClickHouse raw fact store and schema for ch-loader. |
| `internal/storage/pgstore` | PostgreSQL checkpoints, market state and holder balances. |
| `internal/conf` | YAML configuration. |

## market-engine

Each chain runs its own engine, which reads the chain topic in offset order without a consumer group. After each record the engine saves a checkpoint (offset, height, hash), so a restart resumes at the next unapplied record. If a checkpoint falls outside Kafka retention, the engine stops rather than skipping data.

The engine only orders records; a `Handler` does the work. market-engine's handler runs the market processor and writes its output, and holder-indexer's handler updates balances. Each record ends with `Handler.Commit`, which saves the checkpoint with everything the record changed.

- `BlockRevert` calls `Handler.Revert`, which for market-engine runs `Processor.Revert` then `Output.Rewind`, and drops everything above `rollback_to`. A revert at or above the applied height is a no-op, so redelivery is safe.
- `BlockEvent` must extend the last applied block. Blocks at or below the applied height are treated as redeliveries from a retried publish and are skipped. A height gap or a parent hash mismatch without a preceding revert stops the engine.
- Stores must make `WriteBlock` replace a height, because a crash between the write and the checkpoint replays that block.

## Storage

`storage.driver` is either `memory` or `clickhouse`. The ClickHouse store creates its database and these tables on startup:

| Table | Rows |
| --- | --- |
| `blocks` | One per applied block. It is written last, so it marks the height as complete. |
| `trades` | Oriented trades with USD values. |
| `token_prices` | Each token price emitted by a block. |
| `candles` | One version per block that changed a bucket. Read the current bucket with `argMax(col, update_height)` or `LIMIT 1 BY`. |

Rows are keyed by `(chain_id, height)`. A revert deletes every row above `rollback_to`; candles drop the newer versions, so the previous version becomes current again. Writing a height that may already exist deletes it first, which makes a replay idempotent. Deletes use ClickHouse lightweight `DELETE`.

## State

`state.driver` decides where checkpoints and market state (pools, prices, orientations, open candles, oracle answers and their undo history) are kept.

- `memory`: a restart replays each topic from offset zero and rewrites the heights already in the output store.
- `postgres`: after each record, the engine commits that record's state changes and its checkpoint in one transaction. A restart loads the state and resumes at the next record, with no replay.

The PostgreSQL store creates these tables in `state.postgres.schema`:

| Table | Contents |
| --- | --- |
| `engine_checkpoints` | One row per chain: Kafka offset, height, hash and the pruned height. |
| `engine_state` | Current state as `(chain_id, kind, key) -> value JSONB`, where kind is `pools`, `prices`, `orientations`, `candles`, `decimals` or `oracle`. |
| `engine_state_undo` | For each key a block changed, the value it held before that block. A revert deletes rows above `rollback_to`; pruning deletes rows at or below the finalized height and `history_depth`. |

Computation stays in memory, so PostgreSQL is written once per block and never read on the hot path. A crash between the output write and the commit replays one block, which the output store overwrites.

## market-api

```sh
go run ./cmd/market-api -conf configs/config.yaml   # needs storage.driver: clickhouse
```

| Endpoint | Returns |
| --- | --- |
| `GET /v1/chains/{chain}/tokens/{token}/price` | The latest USD price, its source, hops and liquidity. |
| `GET /v1/chains/{chain}/tokens/{token}/candles?interval=1m` | The token's aggregated, liquidity-filtered candles. |
| `GET /v1/chains/{chain}/pools/{pool}/candles?interval=1m` | Pool candles. |
| `GET /v1/chains/{chain}/tokens/{token}/trades` | Recent trades with the token as base, newest first. |
| `GET /v1/chains/{chain}/pools/{pool}/trades` | Recent trades in the pool. |
| `GET /v1/chains/{chain}/tokens/{token}/holders` | Holder count and the largest holders, in raw token units. Served only with `state.driver: postgres`. |
| `GET /v1/chains/{chain}/tokens/{token}/transfers` | The token's ERC-20 transfers from ch-loader, newest first. `holder` narrows them to one party. |
| `GET /v1/chains/{chain}/holders/{holder}/transfers` | Transfers to or from the holder across tokens, newest first. `token` narrows them to one token. |
| `GET /healthz` | 204 when the process is up. |

- `{chain}` is the numeric chain ID. The native pseudo-address is an alias for the wrapped token.
- Candles accept `from` and `to` (unix seconds, `from <= open_time < to`) and `limit`. `interval` must be one of `market.intervals`. Buckets with no trades are omitted, and each returned bucket opens at the previous close.
- Trades accept `limit` and `before_height` for paging backwards.
- Transfers accept `limit`, `before_height` and `before_log_index`. A full page returns `next: {before_height, before_log_index}`; pass both back to continue without skipping the rest of a block. Amounts are raw token units in `amount_raw`, and the native pseudo-address is not mapped, since only ERC-20 transfers are stored.
- Decimal values are returned as strings.

Queries go straight to ClickHouse, which is fast enough at local scale. Add a Redis hot view only if latency calls for it.

## holder-indexer

```sh
go run ./cmd/holder-indexer -conf configs/config.yaml   # needs state.driver: postgres and chains[].rpc_url
```

holder-indexer reads each chain topic on its own checkpoint and keeps every ERC-20 holding it has seen.

- **Seeding.** The first time a `(token, holder)` appears, its balance is `balanceOf` read at the hash of that block, so the seed already includes the block's transfers and history before the indexer started. The node must serve state for that block, which any full node does near the head. A node without the state stops the indexer rather than guessing. When `balanceOf` itself reverts, the block's net transfers seed the holding and it is marked unverified.
- **Accumulating.** After that, each block adds its net transfers per holding. Mints and burns use the zero address, which is not a holder. Zero-value transfers are ignored.
- **Reconciling.** Every `holders.reconcile_every` blocks, the `reconcile_batch` least recently verified holdings are checked against `balanceOf` and corrected. This catches rebasing and fee-on-transfer tokens, whose balances move without a matching `Transfer`.
- **Reorgs.** Every change is stored as an undoable delta, so a revert subtracts what the reverted blocks added and deletes holdings they created.

| Table | Contents |
| --- | --- |
| `holder_checkpoints` | One row per chain: Kafka offset, height, hash and the pruned height. |
| `holder_balances` | `(chain_id, token, holder) -> balance NUMERIC(78,0)` and the height `balanceOf` last confirmed it. |
| `holder_undo` | Per block and holding, the delta added or whether the row was created, with the previous verified height. Pruned like `engine_state_undo`. |

Balances live only in PostgreSQL, so memory does not grow with the number of holders.

## ch-loader

```sh
go run ./cmd/ch-loader -conf configs/config.yaml   # needs storage.driver: clickhouse
```

ch-loader reads each chain topic on its own checkpoint and stores the facts chainstream-evm published, without deriving anything. Addresses are lowercased; everything else is as published. It writes these tables in the `storage.clickhouse` database:

| Table | Rows |
| --- | --- |
| `raw_transfers` | Every ERC-20 transfer, zero-value ones included, with `amount UInt256`. Bloom filter indexes on `token` and on `[from_address, to_address]`. |
| `raw_trades` | Pool operations before orientation: action, sender, trader, recipient, the signed pool-perspective amounts as parallel arrays, and the post-trade pool state. |
| `oracle_updates` | Every `AnswerUpdated` log from any contract, with `answer Int256`. |
| `asset_facts` | Token observations and creations: standard, recognition, deployment and `metadata_json`. |
| `pool_facts` | Pool observations and creations: protocol, tokens, source height and `metadata_json`. |
| `raw_checkpoints` | One row per flush: Kafka offset, height and hash. The newest offset is the checkpoint. |

Fact tables are sorted by `(chain_id, block_height, ...)`, where `block_height` is the block that published the fact.

- **Checkpoint in ClickHouse.** ClickHouse has no transactions, so a flush inserts the facts first and the checkpoint after them. The checkpoint lives in the same database as the facts, so dropping or restoring that database moves them together, and ch-loader needs no PostgreSQL.
- **Replays.** A crash can leave facts above the checkpoint. After a restart, the first block or revert deletes everything above the checkpoint's next height before inserting, so a replay leaves one copy. A flush that failed part way is cleared the same way before it is retried.
- **Reorgs.** A revert drops buffered blocks above `rollback_to` and deletes stored facts above it with lightweight `DELETE`. Raw facts need no undo history, so any depth of reorg can be reverted.
- **Batching.** Blocks older than `ch_loader.batch_lag` (catching up) are buffered and written together once `flush_rows` facts or `flush_interval` accumulate, one insert per table. Newer blocks are written on their own commit, so the API sees them right away. A clean stop writes the buffer.

## Pricing and candles

Each block goes through `internal/market` in three steps:

1. **Pool state.** Every two-token trade updates its pool:
   - Constant-product pools use `reserves_after`.
   - Concentrated-liquidity pools use virtual reserves derived from `sqrt_price_x96` and `liquidity`.
   - Other pools use the swap's execution price and have no reserves, so they cannot propagate prices. Liquidity-book pools fall in this group for now.
2. **Price graph.** Anchors are the catalog's USD stablecoins. Each one takes its latest trusted oracle answer when a feed covers it, and $1 otherwise. Every token touched in the block is priced through the pool whose priced counterpart reserve is worth the most in USD. That reserve must reach `min_liquidity_usd`, and the price can spread at most `max_hops` from an anchor. A token is never priced through a counterpart that was itself priced through that token. The wrapped native coin is priced through the graph like any other token, and the native pseudo-address shares its price.
3. **Trades and candles.**
   - Each pool's quote token comes from catalog rank first, then from the priced token with fewer hops. Once a pair has a known quote, its orientation stays fixed. Pairs with no known quote stay provisional.
   - Trades are valued at the block's closing quote price.
   - Swaps feed pool candles. They also feed the base token's aggregated candles when the pool's quote reserve reaches `candle_min_liquidity_usd`.
   - A new candle opens at the previous candle's close.

Oracle answers come from `oracle_updates` (Chainlink `AnswerUpdated` logs) whose feed address is listed under `chains[].feeds`. A feed can also cover a non-anchor token, such as native ETH or WBTC. That token keeps its per-block graph price while the price stays within `oracle_deviation` of the latest answer; beyond that, or when the graph has no price, the oracle answer is used instead. The default config lists no feeds. Add each aggregator address, which `proxy.aggregator()` returns, to turn this on.

All state is journaled by height. A revert undoes it exactly, as long as the reorg stays above the finalized height and within `history_depth` blocks. A deeper reorg stops the engine. State is computed in memory and persisted per block when `state.driver` is `postgres`; see [State](#state).

```sh
go run ./cmd/market-engine -conf configs/config.yaml
```

## Development

Requires Go 1.26.10 and Buf.

```sh
make generate   # regenerate proto/stream/v1 and tidy both modules
make verify     # buf lint, gofmt, golangci-lint, go test -race
```

The ClickHouse tests, including ch-loader's end-to-end run over an in-process kfake broker, run only when `CLICKHOUSE_ADDR` is set, for example `CLICKHOUSE_ADDR=127.0.0.1:9000 make test`. The PostgreSQL tests run only when `POSTGRES_DSN` is set, for example `POSTGRES_DSN=postgres://market:market@127.0.0.1:5432/market make test`. CI starts both services.

`proto/stream/v1/event.proto` is copied from chainstream-evm's `api/stream/v1`, with only `go_package` changed. chainstream-evm still publishes from its own copy. The two must stay wire-compatible until chainstream-evm imports this module.
