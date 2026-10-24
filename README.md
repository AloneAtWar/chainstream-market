# chainstream-market

Market data services built on the [chainstream-evm](https://github.com/AloneAtWar/chainstream-evm) block stream: trade orientation, USD pricing, candles and holders for ETH, BSC and Base.

## Layout

| Path | What it is |
| --- | --- |
| `proto/` | Separate Go module `github.com/AloneAtWar/chainstream-market/proto` holding the `stream.v1` wire format. It depends only on protobuf so producers can import it cheaply. |
| `cmd/market-engine` | Consumes one chainstream-evm topic per chain and writes derived market data. |
| `internal/engine` | Per-chain ordered apply loop: reverts, continuity checks and checkpoints. |
| `internal/market` | Per-chain processor: pool state, base/quote orientation, the USD price graph and candles. |
| `internal/source` | Kafka reader for partition zero of a chain topic. |
| `internal/storage` | Output models (oriented trades, token prices, candles), store interfaces and the in-memory backend. |
| `internal/conf` | YAML configuration. |

## market-engine

Each chain runs its own engine, which reads the chain topic in offset order without a consumer group. After each record the engine saves a checkpoint (offset, height, hash), so a restart resumes at the next unapplied record. If a checkpoint falls outside Kafka retention, the engine stops rather than skipping data.

- `BlockRevert` calls `Processor.Revert`, then `Output.Rewind`, and drops everything above `rollback_to`. A revert at or above the applied height is a no-op, so redelivery is safe.
- `BlockEvent` must extend the last applied block. Blocks at or below the applied height are treated as redeliveries from a retried publish and are skipped. A height gap or a parent hash mismatch without a preceding revert stops the engine.
- Stores must make `WriteBlock` replace a height, because a crash between the write and the checkpoint replays that block.

ClickHouse and Redis stores are not built yet, so `storage.driver` accepts only `memory`.

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

All state is journaled by height. A revert undoes it exactly, as long as the reorg stays above the finalized height and within `history_depth` blocks. A deeper reorg stops the engine. State lives in memory and is rebuilt by replaying the topic from offset zero. Snapshotting it alongside persistent stores is future work.

```sh
go run ./cmd/market-engine -conf configs/config.yaml
```

## Development

Requires Go 1.26.10 and Buf.

```sh
make generate   # regenerate proto/stream/v1 and tidy both modules
make verify     # buf lint, gofmt, golangci-lint, go test -race
```

`proto/stream/v1/event.proto` is copied from chainstream-evm's `api/stream/v1`, with only `go_package` changed. chainstream-evm still publishes from its own copy. The two must stay wire-compatible until chainstream-evm imports this module.
