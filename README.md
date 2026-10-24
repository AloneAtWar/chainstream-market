# chainstream-market

Market data services built on the [chainstream-evm](https://github.com/AloneAtWar/chainstream-evm) block stream: trade orientation, USD pricing, candles and holders for ETH, BSC and Base.

## Layout

| Path | What it is |
| --- | --- |
| `proto/` | Separate Go module `github.com/AloneAtWar/chainstream-market/proto` holding the `stream.v1` wire format. It depends only on protobuf so producers can import it cheaply. |
| `cmd/market-engine` | Consumes one chainstream-evm topic per chain and writes derived market data. |
| `internal/engine` | Per-chain ordered apply loop: reverts, continuity checks and checkpoints. |
| `internal/source` | Kafka reader for partition zero of a chain topic. |
| `internal/storage` | Output models (oriented trades, token prices, candles), store interfaces and the in-memory backend. |
| `internal/conf` | YAML configuration. |

## market-engine

Each chain runs its own engine, which reads the chain topic in offset order without a consumer group. After each record the engine saves a checkpoint (offset, height, hash), so a restart resumes at the next unapplied record. If a checkpoint falls outside Kafka retention, the engine stops rather than skipping data.

- `BlockRevert` calls `Processor.Revert`, then `Output.Rewind`, and drops everything above `rollback_to`. A revert at or above the applied height is a no-op, so redelivery is safe.
- `BlockEvent` must extend the last applied block. Blocks at or below the applied height are treated as redeliveries from a retried publish and are skipped. A height gap or a parent hash mismatch without a preceding revert stops the engine.
- Stores must make `WriteBlock` replace a height, because a crash between the write and the checkpoint replays that block.

The processor is a header-only placeholder for now. Orientation, the Chainlink-anchored price graph and liquidity-filtered candles come next. So do the ClickHouse and Redis stores; until they land, `storage.driver` accepts only `memory`.

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
