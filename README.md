# chainstream-market

Market data services built on the [chainstream-evm](https://github.com/AloneAtWar/chainstream-evm) block stream: trade orientation, USD pricing, candles, holders and raw transfers for ETH, BSC and Base.

## Layout

| Path | What it is |
| --- | --- |
| `proto/` | Separate Go module `github.com/AloneAtWar/chainstream-market/proto` holding the `stream.v1` wire format. It depends only on protobuf so producers can import it cheaply. |
| `cmd/market-engine` | Consumes one chainstream-evm topic per chain and writes derived market data. |
| `cmd/market-api` | HTTP API over the ClickHouse store, plus holders from PostgreSQL. |
| `cmd/holder-indexer` | Consumes the same chain topics and keeps ERC-20 and native coin balances per holder in PostgreSQL. |
| `cmd/ch-loader` | Consumes the same chain topics and copies their raw facts, every ERC-20 transfer among them, into ClickHouse. |
| `cmd/market-registry` | Consumes the same chain topics and keeps the latest token and pool metadata in PostgreSQL. |
| `cmd/market-rankings` | Periodically rolls prices, 24h activity, holder counts and metadata into the PostgreSQL token rankings. |
| `internal/app/<component>` | One component per command, each with its own configuration and layers: `engine` (market-engine), `api` (market-api), `holder` (holder-indexer), `loader` (ch-loader), `registry` (market-registry) and `rankings` (market-rankings). See [Components](#components). |
| `internal/pkg/config` | The config loader, and the sections more than one component reads in `common.proto`: the chain source, Kafka, the databases, live push, metrics and the chains. |
| `internal/pkg/model` | The data the components exchange: block output (oriented trades, token prices, candles), holder balances, token and pool metadata, raw facts and rankings. |
| `internal/pkg/chdb` | The shared ClickHouse connection, the DDL of every table in it, their retention TTLs, and the row models and writes of the tables more than one component touches. |
| `internal/pkg/pgdb` | The shared PostgreSQL connection, the DDL of every table in it, and the row models and queries of the tables more than one component touches. Each service creates the tables under an advisory lock, so services can start in any order. |
| `internal/pkg/chainsync` | Per-chain ordered apply loop: reverts, continuity checks and checkpoints, driving a `Handler`. Shared by every command that consumes chain topics. |
| `internal/pkg/source` | Chain topic readers: Kafka partition zero, or a followed file of protobuf JSON records. |
| `internal/pkg/asset` | Token addresses every command shares: the per-chain catalog of USD anchors, quote preferences and aliases, and chainstream-evm's asset metadata. |
| `internal/pkg/rpc` | Contract reads over oh-my-blockchain's EVM SDK: `balanceOf` pinned to a block hash, and calls at the latest block. Adds the indexers' failure rules and metrics. |
| `internal/pkg/chainlink` | Resolves Chainlink feed proxies to the aggregators of every phase. |
| `internal/pkg/contract` | Contract ABIs copied into the repo (ERC-20, Chainlink's EACAggregatorProxy) and the abigen bindings generated from them. |
| `internal/pkg/live` | Realtime push: market-engine's live topic publisher and market-api's WebSocket hub, over a Kafka or Redis Pub/Sub transport. |
| `internal/pkg/transport` | Kratos servers every command shares: HTTP, and a Runner for long-lived work. |
| `internal/pkg/metrics` | Prometheus metrics every command serves at `/metrics`. |
| `web/` | The frontend: token rankings, token and pool pages with live candles and trades, holders and transfers. See [Web frontend](#web-frontend). |
| `deploy/` | docker-compose stack: infrastructure, chainstream-evm, every service, Prometheus and Grafana. |

## market-engine

Each chain runs its own engine, which reads the chain topic in offset order without a consumer group. `source.driver` decides where the topic is read from: `kafka`, or `file`, which follows `<source.file.dir>/<topic>.jsonl` with one `StreamRecord` in protobuf JSON per line, such as chainstream-evm's file output writes with `output.file.format: protojson`. A file record's offset is its index among the non-blank lines, so checkpoints work the same way. A line is read only once its newline is written; until then it is read again from its start on each poll, since chainstream-evm cuts an unterminated line after a crash or failed write and writes a different record in its place. Since one read of a line can start before such a cut and finish after it, each complete line is read again from its start once its newline is seen, when its bytes can no longer change, and that copy is the one used. Lines already read never change, and the source stops with an error if the file shrinks below them. A file lets the whole pipeline run without Kafka, or replay a recorded stream. After each record the engine saves a checkpoint (offset, height, hash), so a restart resumes at the next unapplied record. If a checkpoint falls outside Kafka retention, the engine stops rather than skipping data.

The engine only orders records; a `Handler` does the work. market-engine's handler runs the market processor and writes its output, and holder-indexer's handler updates balances. Each record ends with `Handler.Commit`, which saves the checkpoint with everything the record changed.

- `BlockRevert` calls `Handler.Revert`, which for market-engine runs `Processor.Revert` then `Output.Rewind`, and drops everything above `rollback_to`. A revert at or above the applied height is a no-op, so redelivery is safe.
- `BlockEvent` must extend the last applied block. Blocks at or below the applied height are treated as redeliveries from a retried publish and are skipped. A height gap or a parent hash mismatch without a preceding revert stops the engine.
- Stores must make `WriteBlock` replace a height, because a crash between the write and the checkpoint replays that block.

## Storage

`storage.driver` is either `memory` or `clickhouse`. The ClickHouse store creates its database and these tables on startup:

| Table | Rows |
| --- | --- |
| `blocks` | One per applied block. It is written last, so it marks the height as complete. |
| `trades` | Oriented trades with USD values, and `outlier` for swaps left out of token candles. |
| `token_prices` | Each token price emitted by a block, plus the earlier prices of a token whose decimals arrived late (see [Backfill](#backfill)). |
| `candles` | One version per block that changed a bucket. Read the current bucket with `argMax(col, update_height)` or `LIMIT 1 BY`. |

Rows are keyed by `(chain_id, height)`. A revert deletes every row above `rollback_to`; candles drop the newer versions, so the previous version becomes current again. `trades` and `token_prices` carry `revision_height`, the block that wrote the row, which is later than `block_height` for rows recomputed after a token's decimals arrived; reverts delete those by `revision_height`. Writing a height that may already exist deletes it first, which makes a replay idempotent. Deletes use ClickHouse lightweight `DELETE`.

### Retention and cold storage

Every fact table except `blocks` is partitioned by month (`candles` by interval and month). `storage.clickhouse.retention` turns that into TTLs, in whole days of block time (a candle's open time); zero keeps rows. market-engine sets the TTLs of `trades`, `token_prices` and `candles` at startup, and ch-loader, from its own `clickhouse.retention`, those of its raw tables. market-api and market-rankings open the same tables without touching them.

| Setting | Effect |
| --- | --- |
| `trades_days` | Deletes trades. |
| `token_price_rollup_days` | Rolls older `token_prices` rows up to the latest row per token (about one per token and month after merges), so a token that stopped trading keeps its last price. |
| `candle_compact_days` | Keeps only the final version of older candle buckets. It has to reach past the deepest reorg, since a rewind can only restore versions that are still stored. |
| `candle_days` | Deletes candles per interval, e.g. `{1m: 30, 5m: 180}`. Intervals not listed are kept; `rankings.candle_interval` needs at least 2 days. |
| `raw_transfers_days`, `raw_trades_days`, `oracle_updates_days` | Delete ch-loader's raw facts. `asset_facts` and `pool_facts` are kept. |
| `storage_policy`, `cold_volume`, `cold_after_days` | Move parts older than `cold_after_days` to `cold_volume`. The policy is defined in the ClickHouse server config and must contain a volume named `default` (where tables created without a policy live) and the cold volume; ClickHouse cannot switch a table back to its old policy. |

`trades` and the raw tables set `ttl_only_drop_parts`, so ClickHouse drops a month's parts whole once their newest row expires instead of rewriting them; rows can outlive their age by up to a month. A TTL is changed only when the configured one differs, because changing it rewrites the table's parts in the background. Removing a setting removes its rule.

A policy for a second disk looks like this in the server config:

```xml
<storage_configuration>
  <disks><cold><path>/mnt/cold/clickhouse/</path></cold></disks>
  <policies><hot_cold><volumes>
    <default><disk>default</disk></default>
    <cold><disk>cold</disk></cold>
  </volumes></hot_cold></policies>
</storage_configuration>
```

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

Every service that uses PostgreSQL creates all the tables it might need when it starts, in one transaction that holds an advisory lock keyed by the schema name. Several services can therefore start together on an empty schema: they create the tables one after another instead of failing with a duplicate key error.

Computation stays in memory, so PostgreSQL is written once per block and never read on the hot path. A crash between the output write and the commit replays one block, which the output store overwrites.

## market-api

```sh
go run ./cmd/market-api -conf configs/market-api.yaml   # needs clickhouse.addr and postgres.dsn
```

| Endpoint | Returns |
| --- | --- |
| `GET /v1/chains/{chain}/tokens/{token}/price` | The latest USD price, its source, hops and liquidity. |
| `GET /v1/chains/{chain}/tokens/{token}/candles?interval=1m` | The token's aggregated, liquidity-filtered candles. |
| `GET /v1/chains/{chain}/pools/{pool}/candles?interval=1m` | Pool candles. |
| `GET /v1/chains/{chain}/tokens/{token}/trades` | Recent trades with the token as base, newest first. |
| `GET /v1/chains/{chain}/pools/{pool}/trades` | Recent trades in the pool. |
| `GET /v1/chains/{chain}/tokens/{token}/holders` | Holder count and the largest holders. `balance_raw` is in raw token units; when market-registry knows the token's decimals, `decimals` and a converted `balance` are added. The native pseudo-address lists native coin holders, always with 18 decimals. |
| `GET /v1/chains/{chain}/tokens/{token}` | Token metadata from market-registry: standard, recognition, name, symbol, decimals, deployment and the raw metadata JSON. |
| `GET /v1/chains/{chain}/tokens/{token}/pools` | Pools that trade the token, most recently observed first. Accepts `limit`. |
| `GET /v1/chains/{chain}/pools/{pool}` | Pool metadata: protocol, DEX, tokens and the raw metadata JSON. |
| `GET /v1/chains/{chain}/tokens?sort=volume_24h` | The token list from market-rankings, see [market-rankings](#market-rankings). |
| `GET /v1/chains/{chain}/tokens/{token}/transfers` | The token's ERC-20 transfers from ch-loader, newest first. `holder` narrows them to one party. |
| `GET /v1/chains/{chain}/holders/{holder}/transfers` | Transfers to or from the holder across tokens, newest first. `token` narrows them to one token. |
| `GET /v1/stream` | WebSocket stream of live trades and candles, see [Live push](#live-push). Served only with `live.enabled`. |
| `GET /healthz` | 204 when the process is up. |

- `{chain}` is the numeric chain ID. For prices, candles and trades the native pseudo-address is an alias for the wrapped token; holders and registry routes take addresses as they are.
- Candles accept `from` and `to` (unix seconds, `from <= open_time < to`) and `limit`. `interval` must be one of `api.intervals`, which must be among market-engine's `market.intervals`. Buckets with no trades are omitted, and each returned bucket opens at the previous close.
- Trades accept `limit` and `before_height` for paging backwards.
- Transfers accept `limit`, `before_height` and `before_log_index`. A full page returns `next: {before_height, before_log_index}`; pass both back to continue without skipping the rest of a block. Amounts are raw token units in `amount_raw`, and the native pseudo-address is not mapped, since only ERC-20 transfers are stored.
- Decimal values are returned as strings.

Queries go straight to ClickHouse, which is fast enough at local scale. Add a Redis hot view only if latency calls for it.

## Live push

With `live.enabled: true`, market-engine publishes what each block derived to a live topic per chain, and market-api streams it to WebSocket clients at `GET /v1/stream`. Unfinalized data is pushed at once and corrected after a reorg, the same way it is stored.

```
chain topic -> market-engine -> {topic}-live (Kafka or Redis) -> market-api hub -> WebSocket clients
```

`live.driver` picks the transport. `kafka` writes `{topic}-live` on partition 0 of the kafka brokers; `redis` publishes to the Redis Pub/Sub channel of the same name on `live.redis`. Either way the hub only follows what is published while it runs, so the two behave the same for clients. Redis needs no topics and suits a deployment without Kafka, for example with `source.driver: file`.

**Publishing.** After a block's output is stored and before its checkpoint is committed, market-engine writes the block's oriented trades and every candle bucket it changed to `chains[].live_topic` (default `<topic>-live`), as JSON records, through the transport. A revert writes the height it rolled back to and a reset for every candle series the reverted blocks touched. A failed write stops the engine like a failed store write, so a record is published at least once; a replay republishes the same values, which clients overwrite. Blocks older than `live.max_lag` are skipped so that a catch-up does not push history; reverts are always published. With Kafka, topics are created on first write when the broker allows auto-creation; otherwise create them with one partition.

**Fan-out.** market-api reads each live topic from its end, without a consumer group, so every instance serves its own clients. Trades go out as soon as their block arrives. Candle buckets and the chain head are coalesced and sent every `live.candle_throttle`, each bucket once at its latest value. A client that falls `live.client_buffer` messages behind is disconnected (close code 1013) rather than silently skipping corrections; it should reconnect and load history again.

**Protocol.** Send JSON requests:

```json
{"op": "subscribe", "channels": ["candles:1:token:0xc02a...:1m", "trades:1:pool:0x88e6..."]}
{"op": "unsubscribe", "channels": ["trades:1:pool:0x88e6..."]}
```

| Channel | Carries |
| --- | --- |
| `trades:<chain>:pool:<pool>` | The pool's trades, like `/pools/{pool}/trades`. |
| `trades:<chain>:token:<token>` | Trades with the token as base, like `/tokens/{token}/trades`. |
| `candles:<chain>:pool:<pool>:<interval>` | Pool candles. `interval` is one of `api.intervals`. |
| `candles:<chain>:token:<token>:<interval>` | The token's aggregated candles. |

Addresses are lowercased and token aliases, such as the native pseudo-address, map to the wrapped token. The `subscribed` reply lists the channels as the server spells them, and messages use that spelling. A connection holds at most `live.max_subscriptions` channels; an invalid request gets an `error` message and changes nothing.

| Message | Meaning |
| --- | --- |
| `{"type":"trades","channel","height","trades":[...]}` | New trades. Each has the REST trade fields plus `revision_height`, the block that emitted it. A held late trade keeps its own `block_height` but has the later `revision_height`. |
| `{"type":"candle","channel","candle":{...}}` | The whole current value of one bucket, with `update_height`. Replace the bucket with the same `open_time`. |
| `{"type":"head","chain_id","height","finalized_height"}` | The chain's newest pushed block. Blocks at or below `finalized_height` are never reverted. |
| `{"type":"revert","chain_id","rollback_to"}` | A reorg: drop every trade with `revision_height > rollback_to` on that chain. Sent to every connection subscribed to the chain. |
| `{"type":"candle_reset","channel","candle":{...} or null}` | Follows a revert, one per affected series. The candle is the series' newest bucket as of `rollback_to`: replace it and drop every newer bucket. `null` means the series had no buckets then; drop them all. |

A client that applies messages in order ends with what the store holds: before a revert, the hub sends whatever it had coalesced, so a reset always comes last. To combine history and live data without a gap, subscribe first, then load history over REST, then apply the messages received meanwhile; overwriting buckets by `open_time` and trades by `(block_height, transaction_hash, log_index, operation_index)` makes the overlap harmless. Empty buckets are never sent; draw them flat at the previous close, as the REST candles do.

The reset is exact because a series only gains buckets forward in time, and late trades only build buckets for a series that had none: the bucket current at `rollback_to` is the only older bucket a reverted block can have changed, and market-engine reads it from the same undo history it reverts with.

Browsers on another origin must be listed in `live.allowed_origins`, for example `localhost:5173`. The stream has no authentication.

## holder-indexer

```sh
go run ./cmd/holder-indexer -conf configs/holder-indexer.yaml   # needs postgres.dsn and chains[].rpc_url
```

holder-indexer reads each chain topic on its own checkpoint and keeps every ERC-20 holding it has seen, and optionally native coin balances.

- **Seeding.** The first time a `(token, holder)` appears, its balance is `balanceOf` read at the hash of that block, so the seed already includes the block's transfers and history before the indexer started. The node must serve state for that block, which any full node does near the head. A node without the state stops the indexer rather than guessing. When `balanceOf` itself reverts, the block's net transfers seed the holding and it is marked unverified.
- **Accumulating.** After that, each block adds its net transfers per holding. Mints and burns use the zero address, which is not a holder. Zero-value transfers are ignored.
- **Reconciling.** Every `holders.reconcile_every` blocks, the `reconcile_batch` least recently verified holdings are checked against `balanceOf` and corrected. This catches rebasing and fee-on-transfer tokens, whose balances move without a matching `Transfer`.
- **Native coin.** With `holders.native_balances`, ETH or BNB balances are kept in the same tables under the token `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee`. Gas, block rewards and L2 fees move them without a transfer, so they are never summed: every address in the block's `native_touched` (from chainstream-evm: senders, both sides of every native transfer, the fee recipient) is read with Multicall3's `getEthBalance` at the block's hash, batched with `balanceOf`, and the difference from the stored balance is applied as a verified delta. Addresses without a balance are not kept. Recipients of internal transfers when chainstream-evm runs without `need_internal`, and beacon-chain withdrawals, are not touched; reconciling corrects them. A chain without a known multicall contract, such as a local dev chain, logs a warning and skips native balances.
- **Reorgs.** Every change is stored as an undoable delta, so a revert subtracts what the reverted blocks added and deletes holdings they created.

| Table | Contents |
| --- | --- |
| `holder_checkpoints` | One row per chain: Kafka offset, height, hash and the pruned height. |
| `holder_balances` | `(chain_id, token, holder) -> balance NUMERIC(78,0)` and the height `balanceOf` last confirmed it. |
| `holder_undo` | Per block and holding, the delta added or whether the row was created, with the previous verified height. Pruned like `engine_state_undo`. |
| `holder_counts` | `(chain_id, token) -> holders`, the number of positive balances. Each commit adds one when a balance turns positive and removes one when it stops being positive, in the same transaction as the balances, so a revert moves it back too. A schema whose `holder_balances` predate the table is counted once when the table is created. |

Balances live only in PostgreSQL, so memory does not grow with the number of holders.

## market-registry

```sh
go run ./cmd/market-registry -conf configs/market-registry.yaml   # needs postgres.dsn
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
go run ./cmd/market-rankings -conf configs/market-rankings.yaml   # needs clickhouse.addr and postgres.dsn
```

market-rankings keeps `token_rankings` in PostgreSQL, a rollup that market-api's token list reads with one indexed query. Every `rankings.refresh_interval` it rebuilds each chain's rows and commits them in one transaction:

- **24h activity.** Volume, trade count and price change come from the token's aggregated candles of `rankings.candle_interval`, summed over the 24h before the chain head (the newest block in ClickHouse, not the wall clock). The window starts at a bucket boundary, so it can be up to one interval longer than 24h. Each bucket opens at the previous close, so the first bucket's open is the price before the window, and `price_change_24h` is the ratio `close / open - 1` (`-0.25` means down 25%). Tokens that did not trade drop to zero.
- **Price and liquidity.** The latest `token_prices` row. Only tokens priced since the last refresh's head, less `rankings.reorg_window`, are read again; the first refresh reads every token. A token whose recent prices a reorg removed falls back to its older price, or loses it. Liquidity is that of the pool the price came through.
- **Holders.** Read from `holder_counts`, which holder-indexer keeps current, so a refresh never counts `holder_balances`.
- **Created.** The deployment height and time market-registry recorded, when known.

A token is ranked once it has a price or traded in the window. Activity, holders and recent prices are recomputed on every refresh, so the rankings follow a reorg within one refresh.

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

## ch-loader

```sh
go run ./cmd/ch-loader -conf configs/ch-loader.yaml   # needs clickhouse.addr
```

ch-loader reads each chain topic on its own checkpoint and stores the facts chainstream-evm published, without deriving anything. Addresses are lowercased; everything else is as published. It writes these tables in the `clickhouse` database:

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
- **Batching.** Blocks older than `loader.batch_lag` (catching up) are buffered and written together once `flush_rows` facts or `flush_interval` accumulate, one insert per table. Newer blocks are written on their own commit, so the API sees them right away. A clean stop writes the buffer.

## Pricing and candles

Each block goes through market-engine's processor (`internal/app/engine/biz`) in three steps:

1. **Pool state.** Every two-token trade updates its pool:
   - Constant-product pools use `reserves_after`.
   - Concentrated-liquidity pools use virtual reserves derived from `sqrt_price_x96` and `liquidity`.
   - Other pools use the swap's execution price and have no reserves, so they cannot propagate prices. Liquidity-book pools fall in this group for now.
2. **Price graph.** Anchors are the catalog's USD stablecoins. Each one takes its latest trusted oracle answer when a feed covers it, and $1 otherwise. Every token touched in the block is priced through the pool whose priced counterpart reserve is worth the most in USD. That reserve must reach `min_liquidity_usd`, and the price can spread at most `max_hops` from an anchor. A token is never priced through a counterpart that was itself priced through that token. The wrapped native coin is priced through the graph like any other token, and the native pseudo-address shares its price.
3. **Trades and candles.**
   - Each pool's quote token comes from catalog rank first, then from the priced token with fewer hops. Once a pair has a known quote, its orientation stays fixed. Pairs with no known quote stay provisional.
   - Trades are valued at the block's closing quote price.
   - Swaps feed pool candles. They also feed the base token's aggregated candles when the pool's quote reserve reaches `candle_min_liquidity_usd` and the swap is not an outlier (see [Outlier swaps](#outlier-swaps)).
   - A new candle opens at the previous candle's close.

### Chainlink feeds

Oracle answers come from `oracle_updates` (Chainlink `AnswerUpdated` logs) emitted by an aggregator of a feed listed under `chains[].feeds`. A feed can also cover a non-anchor token, such as native ETH or WBTC. That token keeps its per-block graph price while the price stays within `oracle_deviation` of the latest answer; beyond that, or when the graph has no price, the oracle answer is used instead.

The default config lists the USD feeds for every catalog anchor and reference asset on ETH, BSC and Base: the stablecoins, the wrapped native coin, BTC and ETH. Each entry names the feed's **proxy**, the address Chainlink publishes and never changes. `AnswerUpdated` comes from the aggregator behind the proxy, which Chainlink replaces from time to time, so at startup market-engine reads every proxy over the chain's `rpc_url`:

- `decimals()` scales the answers; a `decimals` set in the config must match it.
- `description()`, such as `ETH / USD`, must match the entry's `description` when one is set, so a mistyped proxy stops the engine instead of trusting the wrong feed.
- `phaseAggregators(1..phaseId())` and `aggregator()` list the aggregator of every phase. All of them are trusted, so replaying old blocks still picks up the answers of the aggregator that was current then.

The engine needs a restart to pick up an aggregator Chainlink switches to while it runs. Without an `rpc_url`, list an aggregator directly as `{address, token, decimals}` and keep it current by hand. One feed may price several tokens, with one entry each: Base's USDC/USD feed prices both USDC and bridged USDbC.

### Outlier swaps

A thin or manipulated pool can print a swap far from a token's real price. Such a swap is still stored and served, but it would distort the token's aggregated candles and 24h volume, so market-engine marks it `outlier` and leaves it out of them. A priced buy or sell is an outlier when its USD price is more than `outlier_deviation` (default 0.5) away from its base token's reference price, as a ratio either way: with 0.5, prices between 1/1.5 and 1.5 times the reference pass.

- The reference is the token's price at the block's close: the anchor's oracle answer or peg, an oracle answer that overrode the graph, or the graph price from the token's most liquid priced pool.
- A swap in the pool that sets the reference is never an outlier, since that pool's reserves are the reference. A token with no price yet has no reference, so its swaps all pass.
- Outliers still join their own pool's candles, which show that pool's prices.
- `trades.outlier`, the REST trade field `outlier` and live trades carry the mark.
- The mark only depends on journaled state at the swap's block, so a reorg that replaces the block recomputes it against the new fork's prices.
- Replayed late trades are checked against the base token's recomputed price at their block when the block that brought its decimals could compute one (see [Late token metadata](#late-token-metadata)); otherwise they stay unmarked.

Set `outlier_deviation: -1` to turn the filter off.

### Late token metadata

chainstream-evm publishes a trade's amounts without decimals when it has not read the token's metadata yet. Such a trade cannot be oriented or valued, so market-engine holds it instead of dropping it:

- **Learning decimals.** Decimals come from trade amounts and from the metadata of `recognized` assets in the block's `assets`, which often arrives a few blocks after a new token's first trades. A trade whose amount lacks decimals the engine already learned is valued at once.
- **Holding.** A trade still missing decimals is kept, with its block's closing USD prices for its other tokens, for up to `late_trade_blocks` blocks (default 7200; -1 drops such trades). At most `late_trades_per_token` trades are held per missing token, so spam tokens that never get metadata stay bounded.
- **Replay.** The block that brings the decimals replays the held trades first, in chain order. Each keeps its own block height and time and is valued at the quote price of its own block. Pools seen only through held trades take their state from the latest one, so the token is priced in that same block. The replayed trades then build their pool and token candles, older buckets included, before the block's own trades join the latest bucket.
- **Earlier prices.** The token is also priced at each earlier block where a replayed trade touched one of its pools, the way that block would have priced it: through the pool whose priced counterpart reserve was worth the most, using the pool states the replayed trades leave and the counterpart's price at that block. These rows keep their own block height and time. Trades quoted in the token are valued at them, and its swaps are checked for [outliers](#outlier-swaps) against them, so a thin pool's swap stays out of the token's candles.
- **Reorgs.** Held trades are journaled like all other state. ClickHouse `trades` and `token_prices` carry `revision_height`, the block that wrote the row, and rewinds delete by it, so reverting the block that replayed a trade removes it and holds it again. Candle versions carry the replaying block as `update_height` and rewind the usual way.

A candle key that already has buckets is left alone: once a token's decimals are known its later trades are never held, so a replayed trade could only land before those buckets. Such keys only come up in pools with more than two tokens, or where the late token is the quote.

#### Backfill

Holding is bounded by `late_trade_blocks` and `late_trades_per_token`, and lives in market-engine's state. With `storage.driver: clickhouse`, the block that brings a token's decimals also reads the token's earlier trades back from the `raw_trades` that ch-loader stored, so nothing has to be consumed from Kafka again:

- **What is read.** market-engine records, per token, the first and last height that had a trade waiting on its decimals (state kind `missed`). The backfill reads the raw trades with a nonzero leg in the token from the first of those heights, at most `backfill_blocks` back, through the last. Trades it already holds are kept as they are; the rest join them and are replayed together, in chain order, as described above. A trade that still waits on another token's decimals is left for that token's backfill.
- **Prices.** A backfilled trade's other tokens are valued at their latest `token_prices` row at or below its block, or an anchor's peg when there is none. Where `token_price_rollup_days` has already rolled old rows up, those prices are coarser.
- **Waiting for ch-loader.** ch-loader consumes the topic on its own checkpoint. The backfill waits up to `backfill_wait` for that checkpoint to reach the last height it reads. If ch-loader has never stored the chain, or is still behind after the wait, the block replays only its held trades and logs a warning.
- **Cost.** A bloom filter index on `raw_trades.amount_assets` serves the lookup, and only tokens that actually missed their decimals are read. Tokens that stop missing their decimals for `backfill_blocks` blocks are forgotten.

The output is the same as for held trades: rows keep their own block height, belong to the block that brought the decimals, and rewind with it. Live clients receive the recomputed trades and candles with that block, like any held trade.

| Setting | Effect |
| --- | --- |
| `market.backfill_blocks` | How far back a backfill reads (default 50400, about a week on Ethereum). `-1` replays only held trades. Ignored without ClickHouse. |
| `market.backfill_wait` | How long a backfill waits for ch-loader (default 30s). |

All state is journaled by height. A revert undoes it exactly, as long as the reorg stays above the finalized height and within `history_depth` blocks. A deeper reorg stops the engine. State is computed in memory and persisted per block when `state.driver` is `postgres`; see [State](#state).

```sh
go run ./cmd/market-engine -conf configs/market-engine.yaml
```

## Local stack

`make up` builds the images and starts everything with docker compose: Kafka, ClickHouse, PostgreSQL and Redis, chainstream-evm scanning eth into `chainstream-eth`, the six services here, the web frontend, and Prometheus and Grafana. It needs Docker with Compose v2, an eth node, and a chainstream-evm checkout next to this one (`EVM_DIR=...` points elsewhere). Both images read the private oh-my-blockchain module through your SSH agent (`docker build --ssh default`); see chainstream-evm's README.

```sh
cp deploy/.env.example deploy/.env   # set ETH_RPC_URL
make up      # build and start; make logs follows them
make down    # stop, keeping data; make clean also drops the volumes
```

| Port | What |
| --- | --- |
| 8088 | The web frontend, proxying `/v1` to market-api |
| 8080 | market-api, with WebSocket push at `/v1/stream` |
| 3000 | Grafana, signed in anonymously, opening on the Chainstream dashboard |
| 9090 | Prometheus |
| 8000 | chainstream-evm health and `/metrics` |
| 19092 | Kafka for host tools (`localhost:19092`); containers use `kafka:9092` |
| 8123, 9000 | ClickHouse HTTP and native, user `default` without password, database `market` |
| 5432 | PostgreSQL, `market:market@localhost:5432/market` |

The node must answer `eth_call` by block hash (EIP-1898) near the head, for holder-indexer, and serve the Chainlink proxies market-engine resolves at startup. chainstream-evm starts at the chain tip unless `ETH_START_HEIGHT` is set, and keeps its cursor in Redis afterwards. Public endpoints limit request size: `ETH_CALL_BATCH_SIZE` (default 10) is how many `eth_call`s chainstream-evm sends per JSON-RPC batch, which publicnode accepts; drpc's free tier needs 3. Its config is `deploy/evm/config.yaml`; each service reads its own `deploy/configs/<service>.yaml`. A service that cannot start, such as market-engine without a reachable node, restarts until it can.

The topics are created by `kafka-init` with one partition and no retention limit, since every consumer resumes from its own committed offset. To add a chain, add a chainstream-evm service with its own config and topic, the topics to `kafka-init`, and the chain to each file in `deploy/configs`.

## Web frontend

`web/` is a React + TypeScript single-page app, built with Vite, that reads everything from market-api:

| Page | Shows |
| --- | --- |
| `/{chain}` | The token list from market-rankings, sorted by 24h volume, price change, liquidity, holders or creation, with a liquidity filter and symbol or address search. Refreshed every 15s. |
| `/{chain}/token/{address}` | Price, 24h stats, the token's aggregated candles and live trades, and tabs for holders, pools and transfers, beside the registry metadata and how the price was derived. |
| `/{chain}/pool/{pool}` | The pool's own candles and live trades, its tokens and metadata. |
| `/{chain}/holder/{address}` | The address's ERC-20 transfers across tokens. |

Candles are drawn with TradingView's lightweight-charts. Live data follows [Live push](#live-push): the page subscribes first, loads history over REST once the hub's `subscribed` reply confirms the channel, then applies what arrived meanwhile; it replaces candle buckets by `open_time` and keys trades by `(block_height, transaction_hash, log_index, operation_index)`. History is loaded again on every confirmation, which follows each reconnect, and after every revert, since REST trades do not carry `revision_height`; older candle pages requested before a reload, revert or reset are dropped. Scrolling the chart left pages back through older candles. Empty buckets are drawn flat at the previous close.

The page calls `/v1` on its own origin. In development Vite proxies it, WebSocket included, to `MARKET_API` (default `http://localhost:8080`); in the image, nginx proxies it to `MARKET_API` (default `http://market-api:8080`). Either way market-api sees the page's own host as the origin, so `live.allowed_origins` needs no entry for it.

```sh
cd web
npm ci
npm run mock     # optional: a fake market-api with generated data and live pushes on :8080
npm run dev      # http://localhost:5173
npm test         # unit tests: formatting, candle and trade merging, the WebSocket client
npm run build    # type-check and build into dist/
make docker-web  # from the repository root: the chainstream-web image
```

`mock/server.mjs` serves the same routes and stream protocol as market-api, mints a block per second on each chain, and pushes its trades, candles and head, so the UI can be worked on without the stack. It does not simulate reorgs. The chains and candle intervals the UI offers are in `web/src/lib/chains.ts` and must match market-api's `chains` and `api.intervals`.

## Metrics

Every command serves Prometheus metrics at `/metrics` on `metrics.addr`, off by default; `-metrics-addr :9101` overrides it, so several commands can share one host. All series are prefixed `chainstream_market_` and labelled by `chain`; Prometheus's `job` label tells the services apart.

| Metric | What |
| --- | --- |
| `engine_consumer_lag_records` | Records in the chain topic after the last committed one. |
| `engine_block_lag_seconds` | Clock time minus the last applied block's time. |
| `engine_height` | Last applied block height. |
| `engine_record_seconds{kind}` | Time to apply and commit one record, `block` or `revert`. |
| `engine_records_total{kind}` | Records committed. |
| `engine_reorgs_total`, `engine_reorg_depth_blocks` | Reverts that undid applied blocks, and how many each undid. |
| `rpc_requests_total{result}`, `rpc_request_seconds` | JSON-RPC batches sent by holder-indexer and market-engine's feed lookups, one per attempt: the SDK retries a failed batch up to three times. |
| `rpc_failures_total` | Failed batches, plus node errors on single calls other than a revert. |
| `rankings_refresh_seconds`, `rankings_refresh_failures_total` | market-rankings refreshes. |
| `http_requests_total{route,code}`, `http_request_seconds{route}` | market-api requests by matched route pattern. |

The engine metrics come from every consumer: market-engine, ch-loader, holder-indexer and market-registry. chainstream-evm exports its own under `chainstream_evm_`. The Grafana dashboard in `deploy/grafana/dashboards` charts both.

The config file may read the environment: `${NAME}` is replaced by the variable, and `${NAME:default}` by the default when it is unset. A variable that is set but empty is used as is.

## Development

Requires Go 1.26.10 and Buf.

```sh
make generate   # regenerate the config protos, the abigen bindings, wire_gen.go and proto/stream/v1, and tidy both modules
make verify     # buf lint, gofmt, golangci-lint, go test -race
```

Every command follows chainstream-evm's layout. `main.go` loads `-conf` (default `configs/<command>.yaml`) into its component's `conf.Bootstrap` with Kratos config, and `wire.go` lists the component's provider sets that `wireApp` assembles into a `kratos.App`. Change a constructor or a provider set, then run `make wire` (or `go generate ./cmd/...`) to rewrite the `wire_gen.go` files; change a component's `conf/conf.proto` or `internal/pkg/config/common.proto`, then run `make config`. CI runs `make generate` and fails when it changes or adds any file.

### Chain calls

Every read from a node goes through [oh-my-blockchain](https://github.com/AloneAtWar/oh-my-blockchain)'s EVM SDK (`internal/pkg/rpc`), which batches, retries and aggregates calls through the chain's multicall contract. Calldata is packed and return data decoded only with abigen bindings, never with hand-written selectors. The ABIs are copied unchanged into `internal/pkg/contract/<contract>/`, and the `*_generate.go` file next to each runs abigen at the go-ethereum version in `go.mod`; `TestAbigenMatchesGoMod` fails when the two drift. To call a new contract, add its ABI and a `*_generate.go` there, then run `make abi`.

oh-my-blockchain is private. Locally, set `GOPRIVATE=github.com/AloneAtWar` and give git access to it, for example with `git config --global url."git@github.com:".insteadOf "https://github.com/"`. CI needs the repository secret `OMB_SSH_PRIVATE_KEY`, an SSH key with read access to oh-my-blockchain, as in chainstream-evm.

### Components

Each command is one component in `internal/app/<component>`, laid out the same way:

| Package | What it holds |
| --- | --- |
| `conf` | The component's `Bootstrap` proto in `conf.proto`, with its defaults and validation. It is read from the component's own `configs/<command>.yaml`; only sections several components share, such as `chains` or `clickhouse`, come from `internal/pkg/config/common.proto`. |
| `biz` | The handler or refresher that does the work, and the repository interfaces it persists through. |
| `data` | The repositories over ClickHouse and PostgreSQL, and the other sources the component reads, such as the chain topics or the live feed. |
| `service` | market-api only: the HTTP handler over the readers `data` provides. |
| `server` | The Kratos servers: one runner per chain, market-api's HTTP server and live hub, and metrics. |

A component never imports another one, and `internal/pkg` never imports `internal/app`; golangci-lint's depguard rules fail the build otherwise. A table one component writes and another reads, such as `token_prices` or `holder_counts`, has its row model and shared queries in `internal/pkg/chdb` or `internal/pkg/pgdb`, so each side, and its tests, reach it without the other component.

Most components name their one database directly (`clickhouse:` or `postgres:`). market-engine keeps two drivers: `storage.driver` (`memory` or `clickhouse`) for its output and `state.driver` (`memory` or `postgres`) for its state, so it can run without either database.

PostgreSQL and ClickHouse are reached through GORM. The schemas stay hand-written DDL in `internal/pkg/chdb/schema.go` and `internal/pkg/pgdb/schema.go` rather than GORM migrations, since they rely on partial and GIN indexes, ClickHouse engines, sort keys and TTLs that model tags cannot express.

The ClickHouse tests, including ch-loader's end-to-end run over an in-process kfake broker, run only when `CLICKHOUSE_ADDR` is set, for example `CLICKHOUSE_ADDR=127.0.0.1:9000 make test`. The PostgreSQL tests run only when `POSTGRES_DSN` is set, for example `POSTGRES_DSN=postgres://market:market@127.0.0.1:5432/market make test`. market-engine's late-trade run over kfake needs both. CI starts both services.

`proto/stream/v1/event.proto` is copied from chainstream-evm's `api/stream/v1`, with only `go_package` changed. chainstream-evm still publishes from its own copy. The two must stay wire-compatible until chainstream-evm imports this module.
