# 运行、配置与开发

## 本地完整环境

`make up` 构建镜像并用 docker compose 启动全部服务：Kafka、ClickHouse、PostgreSQL、Redis，扫描以太坊写入 `chainstream-eth` 的
chainstream-evm，本仓库的六个服务，Web 前端，以及 Prometheus 和 Grafana。

前提：

- Docker 与 Compose v2；
- 一个以太坊节点（`ETH_RPC_URL`）；
- 与本仓库并列的 chainstream-evm 代码目录（放在别处时用 `EVM_DIR=...` 指定）；
- 两个镜像都通过 SSH agent（`docker build --ssh default`）拉取私有的 oh-my-blockchain 模块，详见 chainstream-evm 的 README。

```sh
cp deploy/.env.example deploy/.env   # 设置 ETH_RPC_URL
make up      # 构建并启动；make logs 跟踪日志
make down    # 停止，保留数据；make clean 同时删除数据卷
```

| 端口 | 服务 |
| --- | --- |
| 8088 | Web 前端，把 `/v1` 代理到 market-api |
| 8080 | market-api，WebSocket 推送在 `/v1/stream` |
| 3000 | Grafana，匿名登录，默认打开 Chainstream 看板 |
| 9090 | Prometheus |
| 8000 | chainstream-evm 健康检查与 `/metrics` |
| 19092 | 给宿主机工具用的 Kafka（`localhost:19092`）；容器内用 `kafka:9092` |
| 8123、9000 | ClickHouse HTTP 与原生协议，用户 `default` 无密码，库 `market` |
| 5432 | PostgreSQL，`market:market@localhost:5432/market` |

节点要求与注意事项：

- 节点必须支持按区块哈希调用 `eth_call`（EIP-1898），且能提供链头附近的状态，供 holder-indexer 使用；还要能访问 market-engine 启动时解析的 Chainlink 代理合约。
- chainstream-evm 默认从链头开始扫描，设置 `ETH_START_HEIGHT` 可指定起点，之后游标保存在 Redis 中。
- 公共节点会限制请求大小：`ETH_CALL_BATCH_SIZE`（默认 10）是 chainstream-evm 每个 JSON-RPC 批量请求中的 `eth_call` 数量，
  publicnode 可以接受；drpc 免费档需要设为 3。
- chainstream-evm 的配置是 `deploy/evm/config.yaml`；每个服务读取自己的 `deploy/configs/<service>.yaml`。
- 无法启动的服务（例如节点不可达时的 market-engine）会不断重启直到成功。

topic 由 `kafka-init` 创建，单分区、不限保留期，因为每个消费者都从自己提交的 offset 恢复。
**新增一条链**：加一个使用独立配置和 topic 的 chainstream-evm 服务，把 topic 加到 `kafka-init`，再把这条链加到 `deploy/configs` 下的每个文件。

## 配置

- 每个命令默认读取 `configs/<command>.yaml`，用 `-conf` 指定其它文件；docker compose 环境使用 `deploy/configs/<service>.yaml`。
- 配置结构定义在各组件的 `internal/app/<component>/conf/conf.proto` 中（含默认值与校验）；多个组件共享的段落（如 `chains`、`clickhouse`）
  定义在 `internal/pkg/config/common.proto`。
- 配置文件可以读取环境变量：`${NAME}` 替换为变量值，`${NAME:default}` 在变量未设置时使用默认值；变量已设置但为空时按空值使用。
- 大多数组件直接配置唯一的数据库（`clickhouse:` 或 `postgres:`）。market-engine 有两个驱动：`storage.driver`（`memory` / `clickhouse`）用于输出，
  `state.driver`（`memory` / `postgres`）用于状态，所以它可以在没有任何数据库的情况下运行。
- `source.driver` 选择 `kafka` 或 `file`；`file` 模式读取 `<source.file.dir>/<topic>.jsonl`，可以完全不用 Kafka 跑通整条链路，或回放录制的数据流。

## 开发

需要 Go 1.26.10 和 Buf。

```sh
make generate   # 重新生成配置 proto、abigen 绑定、wire_gen.go 和 proto/stream/v1，并 tidy 两个模块
make verify     # buf lint、gofmt、golangci-lint、go test -race
```

- **Wire**：每个命令的 `main.go` 用 Kratos config 把 `-conf` 加载到组件的 `conf.Bootstrap`，`wire.go` 列出 `wireApp` 组装 `kratos.App` 所需的 provider set。
  修改构造函数或 provider set 后运行 `make wire`（或 `go generate ./cmd/...`）重写 `wire_gen.go`。
- **配置 proto**：修改组件的 `conf/conf.proto` 或 `internal/pkg/config/common.proto` 后运行 `make config`。
- CI 会运行 `make generate`，有任何文件被修改或新增就失败。

### 链上调用

所有节点读取都通过 [oh-my-blockchain](https://github.com/AloneAtWar/oh-my-blockchain) 的 EVM SDK（`internal/pkg/rpc`），由它负责批量、重试和通过 multicall 合约聚合调用。

- calldata 打包与返回值解码只用 abigen 绑定，不手写 selector。
- ABI 原样复制到 `internal/pkg/contract/<contract>/`，旁边的 `*_generate.go` 用 `go.mod` 中的 go-ethereum 版本运行 abigen；两者不一致时 `TestAbigenMatchesGoMod` 失败。
- 调用新合约：在那里加上 ABI 和 `*_generate.go`，然后运行 `make abi`。

oh-my-blockchain 是私有仓库。本地需要设置 `GOPRIVATE=github.com/AloneAtWar` 并让 git 能访问它，例如
`git config --global url."git@github.com:".insteadOf "https://github.com/"`。CI 需要仓库 secret `OMB_SSH_PRIVATE_KEY`（对 oh-my-blockchain 有读权限的 SSH key）。

### 依赖规则

- 组件之间互不导入，`internal/pkg` 不导入 `internal/app`；golangci-lint 的 depguard 规则会让违规构建失败。
- 一个组件写、另一个组件读的表（如 `token_prices`、`holder_counts`），其行模型和共享查询放在 `internal/pkg/chdb` 或 `internal/pkg/pgdb`。
- PostgreSQL 和 ClickHouse 通过 GORM 访问，但表结构是手写 DDL（`internal/pkg/chdb/schema.go`、`internal/pkg/pgdb/schema.go`），
  因为部分索引、GIN 索引、ClickHouse 引擎、排序键和 TTL 都无法用模型 tag 表达。
- `proto/stream/v1/event.proto` 复制自 chainstream-evm 的 `api/stream/v1`，只改了 `go_package`；在 chainstream-evm 改为导入本模块之前，两份必须保持线上兼容。

### 测试

```sh
make test
CLICKHOUSE_ADDR=127.0.0.1:9000 make test                                   # 打开 ClickHouse 测试（含 ch-loader 基于 kfake 的端到端测试）
POSTGRES_DSN=postgres://market:market@127.0.0.1:5432/market make test      # 打开 PostgreSQL 测试
```

market-engine 基于 kfake 的迟到交易测试同时需要两者。CI 会启动这两个服务。

## 指标

每个命令都在 `metrics.addr` 上提供 Prometheus `/metrics`，默认关闭；`-metrics-addr :9101` 可覆盖，方便多个命令共用一台主机。
所有指标前缀为 `chainstream_market_`，带 `chain` 标签；用 Prometheus 的 `job` 标签区分服务。

| 指标 | 含义 |
| --- | --- |
| `engine_consumer_lag_records` | 链 topic 中最后提交记录之后还有多少条记录 |
| `engine_block_lag_seconds` | 当前时间减去最后处理区块的时间 |
| `engine_height` | 最后处理的区块高度 |
| `engine_record_seconds{kind}` | 处理并提交一条记录的耗时，`block` 或 `revert` |
| `engine_records_total{kind}` | 已提交的记录数 |
| `engine_reorgs_total`、`engine_reorg_depth_blocks` | 撤销了已处理区块的回滚次数，以及每次撤销的块数 |
| `rpc_requests_total{result}`、`rpc_request_seconds` | holder-indexer 与 market-engine 喂价查询发出的 JSON-RPC 批量请求（每次尝试一条，SDK 最多重试三次） |
| `rpc_failures_total` | 失败的批量请求，以及单个调用中除 revert 外的节点错误 |
| `rankings_refresh_seconds`、`rankings_refresh_failures_total` | market-rankings 刷新 |
| `http_requests_total{route,code}`、`http_request_seconds{route}` | market-api 按路由模式统计的请求 |

`engine_*` 指标来自所有消费者：market-engine、ch-loader、holder-indexer 和 market-registry。chainstream-evm 的指标前缀为 `chainstream_evm_`。
`deploy/grafana/dashboards` 中的 Grafana 看板同时展示两者。

## Web 前端

`web/` 是 React + TypeScript 单页应用（Vite 构建），所有数据都来自 market-api：

| 页面 | 内容 |
| --- | --- |
| `/{chain}` | market-rankings 的代币列表，可按 24h 成交量、涨跌幅、流动性、持有人数或创建时间排序，支持流动性过滤和符号 / 地址搜索，每 15 秒刷新 |
| `/{chain}/token/{address}` | 价格、24h 统计、代币聚合 K 线与实时交易，持有人 / 池子 / 转账标签页，以及 registry 元数据和价格推导过程 |
| `/{chain}/pool/{pool}` | 池子自己的 K 线和实时交易、代币和元数据 |
| `/{chain}/holder/{address}` | 该地址跨代币的 ERC-20 转账 |

- K 线用 TradingView 的 lightweight-charts 绘制。
- 实时数据遵循[实时推送](services.md#实时推送live-push)的合并规则：先订阅，收到 `subscribed` 确认后再通过 REST 加载历史，然后应用期间收到的消息。
  每次确认（即每次重连后）和每次 revert 后都会重新加载历史，因为 REST 交易不带 `revision_height`；重载、revert 或 reset 之前发出的旧 K 线分页请求会被丢弃。
- 图表向左滚动会继续加载更早的 K 线；空桶按前一根收盘价画平。
- 页面请求同源的 `/v1`：开发时由 Vite 代理（包括 WebSocket）到 `MARKET_API`（默认 `http://localhost:8080`）；镜像中由 nginx 代理到 `MARKET_API`
  （默认 `http://market-api:8080`）。因此 market-api 看到的 origin 就是页面自己的 host，`live.allowed_origins` 不需要为它添加条目。

```sh
cd web
npm ci
npm run mock     # 可选：在 :8080 启动带生成数据和实时推送的假 market-api
npm run dev      # http://localhost:5173
npm test         # 单元测试：格式化、K 线与交易合并、WebSocket 客户端
npm run build    # 类型检查并构建到 dist/
make docker-web  # 在仓库根目录：构建 chainstream-web 镜像
```

`mock/server.mjs` 提供与 market-api 相同的路由和推送协议，每条链每秒生成一个区块并推送交易、K 线和链头，不需要启动整套环境就能开发 UI；它不模拟重组。
UI 提供的链和 K 线周期在 `web/src/lib/chains.ts` 中，必须与 market-api 的 `chains` 和 `api.intervals` 一致。
