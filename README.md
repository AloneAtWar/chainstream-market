# chainstream-market

Market data services built on the [chainstream-evm](https://github.com/AloneAtWar/chainstream-evm) block stream: trade orientation, USD pricing, candles and holders for ETH, BSC and Base.

## Layout

| Path | What it is |
| --- | --- |
| `proto/` | Separate Go module `github.com/AloneAtWar/chainstream-market/proto` holding the `stream.v1` wire format. It depends only on protobuf so producers can import it cheaply. |
| `cmd/market-engine` | Consumes one chainstream-evm topic per chain and writes derived market data. |
| `cmd/market-api` | HTTP API over the ClickHouse store, plus holders from PostgreSQL. |
| `cmd/holder-indexer` | Consumes the same chain topics and keeps ERC-20 balances per holder in PostgreSQL. |
| `cmd/market-registry` | Consumes the same chain topics and keeps the latest token and pool metadata in PostgreSQL. |
| `cmd/market-rankings` | Periodically rolls prices, 24h activity, holder counts and metadata into the PostgreSQL token rankings. |
| `internal/api` | market-api handlers. |
| `internal/engine` | Per-chain ordered apply loop: reverts, continuity checks and checkpoints, driving a `Handler`. |
| `internal/holders` | holder-indexer's handler: transfer deltas, balanceOf seeding and reconciliation. |
| `internal/registry` | market-registry's handler: asset and pool facts to token and pool metadata. |
| `internal/rankings` | market-rankings' refresher: reads the ClickHouse output and commits one chain's rankings. |
| `internal/rpc` | Minimal JSON-RPC client for batched `balanceOf` calls. |
| `internal/market` | Per-chain processor: pool state, base/quote orientation, the USD price graph and candles. |
| `internal/source` | Kafka reader for partition zero of a chain topic. |
| `internal/storage` | Output models (oriented trades, token prices, candles), store interfaces and the in-memory backend. |
| `internal/storage/chstore` | ClickHouse output store and schema. |
| `internal/storage/pgstore` | PostgreSQL checkpoints, market state, holder balances and the token and pool registry. |
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
| `GET /v1/chains/{chain}/tokens/{token}/holders` | Holder count and the largest holders. `balance_raw` is in raw token units; when market-registry knows the token's decimals, `decimals` and a converted `balance` are added. Served only with `state.driver: postgres`. |
| `GET /v1/chains/{chain}/tokens/{token}` | Token metadata from market-registry: standard, recognition, name, symbol, decimals, deployment and the raw metadata JSON. Served only with `state.driver: postgres`. |
| `GET /v1/chains/{chain}/tokens/{token}/pools` | Pools that trade the token, most recently observed first. Accepts `limit`. Served only with `state.driver: postgres`. |
| `GET /v1/chains/{chain}/pools/{pool}` | Pool metadata: protocol, DEX, tokens and the raw metadata JSON. Served only with `state.driver: postgres`. |
| `GET /v1/chains/{chain}/tokens?sort=volume_24h` | The token list from market-rankings, see [market-rankings](#market-rankings). Served only with `state.driver: postgres`. |
| `GET /healthz` | 204 when the process is up. |

- `{chain}` is the numeric chain ID. For prices, candles and trades the native pseudo-address is an alias for the wrapped token; holders and registry routes take addresses as they are.
- Candles accept `from` and `to` (unix seconds, `from <= open_time < to`) and `limit`. `interval` must be one of `market.intervals`. Buckets with no trades are omitted, and each returned bucket opens at the previous close.
- Trades accept `limit` and `before_height` for paging backwards.
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

## market-registry

```sh
go run ./cmd/market-registry -conf configs/config.yaml   # needs state.driver: postgres
```

market-registry reads each chain topic on its own checkpoint and keeps the latest metadata for every asset (`AssetFact`) and pool (`PoolFact`) it has seen.

- **Tokens.** One row per address with standard, recognition, `metadata_complete`, the published `asset.Info` JSON, and the name, symbol and decimals read from it. ERC-20 fields only count when chainstream-evm marked them known. A `created` fact, or any fact with a deployment, sets `created` and keeps the deployment.
- **Pools.** One row per pool ID with protocol, DEX address, tokens, source height and time, and the protocol metadata JSON. A fact without metadata keeps the stored metadata. A GIN index on `tokens` serves the pools of a token.
- **Newer wins.** A fact replaces stored metadata unless the stored metadata was read at a higher source height, so a late observation of older metadata is ignored. `created` and the deployment are kept regardless.
- **Reorgs.** Before a block first changes a row, the row (or its absence) is saved in `registry_undo`. A revert restores the oldest saved image above `rollback_to`. Undo history is pruned at the finalized height and `registry.history_depth`.
- **Cleaning.** Metadata that is not valid JSON is dropped. NUL characters, which PostgreSQL rejects and token names can contain, are stripped.

| Table | Contents |
| --- | --- |
| `registry_checkpoints` | One row per chain: Kafka offset, height, hash and the pruned height. |
| `registry_tokens` | `(chain_id, address)` with the fields above, plus the heights the token was first stored and last updated. |
| `registry_pools` | `(chain_id, pool_id)` with the fields above, plus first and last updated heights. |
| `registry_undo` | Per block and row, the row's previous value as JSONB, or NULL when the block created it. |

## market-rankings

```sh
go run ./cmd/market-rankings -conf configs/config.yaml   # needs storage.driver: clickhouse and state.driver: postgres
```

market-rankings keeps `token_rankings` in PostgreSQL, a rollup that market-api's token list reads with one indexed query. Every `rankings.refresh_interval` it rebuilds each chain's rows and commits them in one transaction:

- **24h activity.** Volume, trade count and price change come from the token's aggregated candles of `rankings.candle_interval`, summed over the 24h before the chain head (the newest block in ClickHouse, not the wall clock). The window starts at a bucket boundary, so it can be up to one interval longer than 24h. Each bucket opens at the previous close, so the first bucket's open is the price before the window, and `price_change_24h` is the ratio `close / open - 1` (`-0.25` means down 25%). Tokens that did not trade drop to zero.
- **Price and liquidity.** The latest `token_prices` row. Only tokens priced since the last refresh's head, less `rankings.reorg_window`, are read again; the first refresh reads every token. A token whose recent prices a reorg removed falls back to its older price, or loses it. Liquidity is that of the pool the price came through.
- **Holders.** The count of positive `holder_balances`, recounted for every ranked token through the `holder_balances_top` index.
- **Created.** The deployment height and time market-registry recorded, when known.

A token is ranked once it has a price or traded in the window. Activity, holders and recent prices are recomputed on every refresh, so the rankings follow a reorg within one refresh. Recounting holders reads every positive holding of every ranked token; if that grows too slow, holder-indexer should keep the counts itself.

| Table | Contents |
| --- | --- |
| `token_rankings` | `(chain_id, token)` with price, liquidity, 24h volume, trades and price change, holders and creation height, with an index per sort column. |
| `ranking_checkpoints` | One row per chain: the chain head time of the last refresh, which starts the next price scan, and when it ran. |

`GET /v1/chains/{chain}/tokens` accepts:

- `sort`: `volume_24h` (default), `price_change_24h`, `liquidity`, `holders` or `created`. `created` lists only tokens with a known deployment.
- `order`: `desc` (default) or `asc`. Ties order by address in the same direction.
- `q`: a full address finds that token; anything else matches symbols by case-insensitive prefix. Search covers every registry token, including ones with no market data (`"ranked": false`). Exact symbol matches come first, then ranked tokens, then `sort`.
- `min_liquidity_usd`, `limit` (default 200, at most 1000) and `offset` (at most 10000).

Name, symbol and decimals come from market-registry. The response includes `refreshed_at`, the unix time of the last refresh.

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

The ClickHouse store tests run only when `CLICKHOUSE_ADDR` is set, for example `CLICKHOUSE_ADDR=127.0.0.1:9000 make test`. The PostgreSQL tests run only when `POSTGRES_DSN` is set, for example `POSTGRES_DSN=postgres://market:market@127.0.0.1:5432/market make test`. CI starts both services.

`proto/stream/v1/event.proto` is copied from chainstream-evm's `api/stream/v1`, with only `go_package` changed. chainstream-evm still publishes from its own copy. The two must stay wire-compatible until chainstream-evm imports this module.
