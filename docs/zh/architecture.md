# 架构说明

## 1. 组件

每个命令（`cmd/<命令>`）对应一个组件（`internal/app/<组件>`）：

| 命令 | 组件目录 | 读 | 写 | 作用 |
| --- | --- | --- | --- | --- |
| `market-engine` | `engine` | 链 topic | ClickHouse（trades、token_prices、candles）、PostgreSQL（状态，可选）、live topic | 交易定方向、USD 定价、K 线、集中流动性深度 |
| `market-api` | `api` | ClickHouse、PostgreSQL、live topic | — | HTTP API 与 WebSocket 实时推送 |
| `holder-indexer` | `holder` | 链 topic、EVM 节点 | PostgreSQL（holder_*） | ERC-20 与原生币持有人余额 |
| `ch-loader` | `loader` | 链 topic | ClickHouse（raw_*、asset_facts、pool_facts） | 原样保存原始事实，供查询与回填 |
| `market-registry` | `registry` | 链 topic | PostgreSQL（registry_*） | 代币与池子的最新元数据 |
| `market-rankings` | `rankings` | ClickHouse、PostgreSQL | PostgreSQL（token_rankings） | 定期重建代币排行榜 |

**组件之间互不 import**，`internal/pkg` 也不 import 任何组件（golangci-lint 的 depguard 规则保证）。一个组件写、另一个组件读
的表（例如 `token_prices`、`holder_counts`），其行模型和共享查询放在 `internal/pkg/chdb` 或 `internal/pkg/pgdb`。

## 2. 目录结构

```text
proto/                    独立 Go module，stream.v1 线格式（从 chainstream-evm 的 api/stream/v1 复制，仅改 go_package）
cmd/<命令>/               main.go（加载 -conf 配置）、wire.go / wire_gen.go（Wire 依赖注入）
internal/app/<组件>/
  conf/                   组件自己的 Bootstrap 配置（conf.proto）、默认值与校验
  biz/                    业务逻辑：Handler 或 Refresher，以及持久化接口
  data/                   ClickHouse / PostgreSQL 仓库实现，以及链 topic、live feed 等数据源
  service/                仅 market-api：HTTP 处理器
  server/                 Kratos 服务：每条链一个 Runner、market-api 的 HTTP 与 live hub、指标
internal/pkg/
  config/                 配置加载与共享配置段（common.proto）
  model/                  组件间交换的数据模型
  chdb/ pgdb/             共享数据库连接、全部 DDL、共享表的模型与查询
  chainsync/              每条链的顺序应用循环（revert、连续性检查、检查点）
  source/                 链 topic 读取：Kafka 分区 0 或跟读文件
  asset/                  代币地址目录（锚定资产、计价优先级、别名）与元数据解析
  rpc/                    通过 OMB EVM SDK 读合约状态
  chainlink/              解析 Chainlink feed proxy
  contract/               复制进仓库的 ABI 与 abigen 绑定
  live/                   实时推送：Publisher、Hub、Kafka/Redis 传输
  transport/              共享的 Kratos 服务（HTTP、Runner）
  metrics/                Prometheus 指标
web/                      React + TypeScript 前端
deploy/                   docker compose、Prometheus、Grafana
```

## 3. 顺序消费循环：chainsync

market-engine、holder-indexer、market-registry、ch-loader 都用 `internal/pkg/chainsync.Engine` 消费链 topic，它只负责"顺序"，
具体工作交给 `Handler`：

```go
type Handler interface {
    Restore(ctx, chainID) (Checkpoint, bool, error) // 启动时读取检查点
    Apply(ctx, *BlockEvent) error                   // 应用一个区块
    Revert(ctx, rollbackTo int64) error             // 回滚到某高度
    Commit(ctx, Checkpoint) error                   // 每条记录后提交检查点
}
```

处理规则：

- **offset ≤ 检查点**：已处理过，跳过。
- **BlockRevert**：回滚高度 ≥ 已应用高度时无事可做（revert 可能重复投递）；否则调用 `Handler.Revert`，检查点高度降到
  `rollback_to`。
- **BlockEvent**：必须紧接上一个已应用的块。高度 ≤ 已应用高度视为上游重试导致的重复投递，跳过；跳高度或 parent hash
  对不上（且之前没有 revert）说明数据异常，**引擎停止**，而不是带着错误数据继续。
- **每条记录之后**调用 `Handler.Commit`，保存检查点（offset、高度、哈希）以及该记录带来的全部变化。重启后从下一条未提交
  的记录继续。
- 检查点落在 Kafka 保留期之外时引擎停止，而不是跳过数据。

各组件的 Commit 策略：

| 组件 | 提交方式 |
| --- | --- |
| market-engine | `state.driver: postgres` 时状态增量与检查点同一事务提交；`memory` 时只有检查点，重启从 offset 0 重放并覆盖输出 |
| holder-indexer / market-registry | 余额或元数据变化 + 撤销记录 + 检查点，同一个 PostgreSQL 事务 |
| ch-loader | ClickHouse 无事务：先写事实后写检查点；重启后第一个块先删除其高度及以上的数据再插入，保证幂等 |

## 4. 数据源

`source.driver` 决定从哪里读链 topic：

- `kafka`：消费分区 0，不使用消费组，offset 由各组件的检查点管理。
- `file`：跟读 `<source.file.dir>/<topic>.jsonl`，每行一条 protobuf JSON 格式的 `StreamRecord`（即 chainstream-evm 以
  `output.type: file`、`output.file.format: protojson` 写出的文件）。记录的 offset 是它在非空行中的序号，所以检查点用法与 Kafka
  完全一致。只读取已写完换行符的行；未结束的行下次从行首重读，因为 chainstream-evm 在崩溃或写入失败后会截掉半行并写入另一条
  记录。文件比已读部分还短时报错停止。

用文件可以不依赖 Kafka 跑通整条链路，或者回放录制好的数据流。

## 5. 依赖注入与配置

所有命令沿用 chainstream-evm 的布局：`main.go` 用 Kratos config 把 `-conf`（默认 `configs/<命令>.yaml`）加载到组件的
`conf.Bootstrap`，`wire.go` 列出组件的 provider set，由 `wireApp` 组装成 `kratos.App`。

- 修改构造函数或 provider set 后运行 `make wire`；
- 修改组件的 `conf/conf.proto` 或 `internal/pkg/config/common.proto` 后运行 `make config`；
- CI 会运行 `make generate`，生成结果有变化就失败。

## 6. 数据库

- PostgreSQL 与 ClickHouse 都通过 GORM 访问，但表结构是 `internal/pkg/chdb/schema.go`、`internal/pkg/pgdb/schema.go` 中手写的 DDL，
  因为依赖部分索引、GIN 索引、ClickHouse 引擎、排序键和 TTL，这些无法用模型标签表达。
- 使用 PostgreSQL 的服务启动时会在一个以 schema 名为 key 的 advisory lock 下、一个事务内创建所有可能用到的表，因此多个服务可以
  同时在空库上启动，不会互相冲突。
- 链上调用统一经过 oh-my-blockchain 的 EVM SDK（`internal/pkg/rpc`），批量、重试并通过 multicall 聚合；calldata 只用 abigen 绑定
  编解码，ABI 原样放在 `internal/pkg/contract/<合约>/`。
