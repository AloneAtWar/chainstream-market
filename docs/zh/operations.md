# 运行、配置、多链接入与运维排障手册

## 1. 本地完整环境与部署拓扑

`make up` 构建镜像并用 docker compose 启动全部服务：Kafka、ClickHouse、PostgreSQL、Redis，扫描以太坊写入 `chainstream-eth` 的 `chainstream-evm`，本仓库的八个服务，Web 前端，以及 Prometheus 和 Grafana。

![Docker Compose 部署拓扑、端口映射与 Prometheus / Grafana 监控体系](../../generated-images/deploy-observability-topology.png)

前提：

- Docker 与 Compose v2；
- 一个以太坊节点（`ETH_RPC_URL`）；
- 与本仓库并列的 `chainstream-evm` 代码目录（放在别处时用 `EVM_DIR=...` 指定）；
- 两个镜像都通过 SSH agent（`docker build --ssh default`）拉取私有的 `oh-my-blockchain` 模块，详见 `chainstream-evm` 的 README。

```sh
cp deploy/.env.example deploy/.env   # 设置 ETH_RPC_URL
make up      # 构建并启动；make logs 跟踪日志
make down    # 停止，保留数据；make clean 同时删除数据卷
```

| 端口 | 服务 | 说明 |
| --- | --- | --- |
| 8088 | Web 前端 | Nginx 托管 SPA 并把 `/v1`（含 WebSocket）反向代理到 `market-api:8080` |
| 8080 | `market-api` | REST API 与 WebSocket 实时推送（`/v1/stream`） |
| 3000 | Grafana | 匿名登录，默认打开 Chainstream 看板 |
| 9090 | Prometheus | 抓取 `chainstream-evm` 与 8 个 market 服务的 `/metrics` |
| 8000 | `chainstream-evm` | 健康检查与 `/metrics` |
| 19092 | Kafka | 给宿主机工具用的外部端口（`localhost:19092`）；容器网络内用 `kafka:9092` |
| 8123、9000 | ClickHouse | HTTP 与原生 TCP 协议，用户 `default` 无密码，库 `market` |
| 5432 | PostgreSQL | `market:market@localhost:5432/market` |

节点要求与注意事项：

- `holder-indexer` 默认（`state_reads: latest`）在 latest 读余额，普通全节点即可；`state_reads: exact` 时节点必须支持按区块哈希调用 `eth_call`（EIP-1898），并能提供正在应用的区块的状态。节点还要能访问 `market-engine` 启动时解析的 Chainlink 代理合约。
- `chainstream-evm` 默认从链头开始扫描，设置 `ETH_START_HEIGHT` 可指定起点，之后游标保存在 Redis 中。
- 公共节点会限制请求大小：`ETH_CALL_BATCH_SIZE`（默认 10）是 `chainstream-evm` 每个 JSON-RPC 批量请求中的 `eth_call` 数量，`publicnode` 可以接受；`drpc` 免费档需要设为 3。
- `chainstream-evm` 的配置是 `deploy/evm/config.yaml`；每个服务读取自己的 `deploy/configs/<service>.yaml`。
- 无法启动的服务（例如节点不可达时的 `market-engine`）会由 Docker Compose 自动重启直到成功。

---

## 2. 新增一条链完整接入清单（Multi-Chain Onboarding Checklist）

系统天然支持单进程内同时消费多条链（每条链一个独立的 `chainsync.Engine` 或 `Refresher` / `Follower` goroutine）。接入一条新链（例如新增 Arbitrum / Polygon，或启用 BSC / Base / Robinhood Chain）只需按以下 6 步核对：

| 步骤 | 涉及位置 | 操作内容与注意事项 |
| --- | --- | --- |
| **1. 资产目录与锚点** | [internal/pkg/asset/](../../internal/pkg/asset) | 确认该链的 `chain_id` 已在资产目录中登记：包括 **USD 稳定币锚点（Anchors）**、**Quote 计价优先级（Quote Preferences，如 USDT/USDC > WETH/WBNB）** 以及**原生币伪地址 `0xeeee...eeee` 到包装原生币（WETH/WBNB）的别名映射**。若缺少锚点，价格图将无法起价。 |
| **2. Multicall 与节点能力** | `internal/pkg/rpc` & `holder-indexer` 配置 | 若开启 `holders.native_balances: true`，确认该链有已知的 Multicall3（或 Uniswap Interface Multicall）地址。普通 EVM 链默认用 `state_reads: latest`（通过 EIP-1898 固定到最新链头哈希）；若为 **TRON**，必须使用 `state_reads: latest`（不支持 `exact`）。 |
| **3. 上游采集与 Kafka Topic** | `deploy/docker-compose.yml` & `deploy/evm/` | 1. 新增该链对应的 `chainstream-evm` 实例与配置文件，指定 `chain_id` 和输出 Topic（如 `chainstream-bsc`）；<br>2. 在 `kafka-init` 容器脚本中创建单分区、无限保留期的链 Topic（`--partitions 1 --config retention.ms=-1`）以及对应的实时 Topic `<topic>-live`。 |
| **4. Chainlink 喂价配置** | `configs/market-engine.yaml` / `deploy/configs/market-engine.yaml` | 在该链的 `chains[].feeds` 下配置各代币的 Chainlink **EACAggregatorProxy（代理合约）**地址、目标 `token` 地址及可选的 `description` / `decimals` 校验。`market-engine` 启动时会通过 `rpc_url` 自动拉取该代理的所有历史 Phase 聚合器。 |
| **5. 八大服务 `chains[]` 对齐** | `deploy/configs/*.yaml`（或 `configs/*.yaml`） | 在 8 个服务的配置文件 `chains:` 列表中加入该链条目：<br>- 5 个链 Topic 消费者（`engine`, `loader`, `holder`, `registry`, `launchpad`）：确保 `chain_id`、`name`、`topic` 一致；<br>- `market-engine` 与 `market-api`、`market-wallet`：确保 `live_topic`（默认 `<topic>-live`）一致；<br>- `market-rankings`：将该链加入刷新列表。 |
| **6. Web 前端链列表** | [web/src/lib/chains.ts](../../web/src/lib/chains.ts) | 将新链的 `id`、展示名称、原生币符号与区块浏览器 URL 前缀加入 `chains.ts`，并确保前端支持的 K 线周期与 `market-api` 的 `api.intervals` 一致。 |

---

## 3. 配置与 ClickHouse 数据生命周期管理

### 3.1 配置加载机制

- 每个命令默认读取 `configs/<command>.yaml`，用 `-conf` 指定其它文件；docker compose 环境使用 `deploy/configs/<service>.yaml`。
- 配置结构定义在各组件的 `internal/app/<component>/conf/conf.proto` 中（含默认值与校验）；多个组件共享的段落（如 `chains`、`clickhouse`）定义在 `internal/pkg/config/common.proto`。
- 配置文件支持读取环境变量：`${NAME}` 替换为变量值，`${NAME:default}` 在变量未设置时使用默认值；变量已设置但为空时按空值使用。
- `market-engine` 有两个可插拔驱动：`storage.driver`（`memory` / `clickhouse`）用于输出，`state.driver`（`memory` / `postgres`）用于状态，因此可以在无任何外部数据库的纯内存模式下运行单测或轻量回放。
- `source.driver` 选择 `kafka` 或 `file`；`file` 模式读取 `<source.file.dir>/<topic>.jsonl`，可完全不依赖 Kafka 跑通整条链路或回放录制流。

### 3.2 ClickHouse TTL 压缩、汇总与冷热分层规划

![ClickHouse 行情与原始事实表 TTL 汇总、压缩与冷热分层生命周期](../../generated-images/clickhouse-retention-lifecycle.png)

随着多链 7×24 小时运行，ClickHouse 中的原始事实与行情表会持续增长。系统通过 `storage.clickhouse.retention`（由 `market-engine` 设置行情表）和 `clickhouse.retention`（由 `ch-loader` 设置原始事实表）自动管理表的生命周期：

| 阶段 | 核心配置项 | 目标表 | 运维与容量规划要点 |
| --- | --- | --- | --- |
| **1. 热数据多版本窗口** | `candle_compact_days` | `candles` | 在此天数内，每次改动 K 线桶的区块都保留一行 `update_height` 版本。**此值必须大于可能发生的最深重组时间（建议 $\ge 2$ 天）**，否则回滚删掉新版本后找不到旧版本恢复。 |
| **2. 版本折叠与月度降采样** | `candle_compact_days`、`token_price_rollup_days` | `candles`、`token_prices` | 超过 `candle_compact_days` 的 K 线桶在后台 Merge 时只保留 `update_height` 最大的最终版本；超过 `token_price_rollup_days` 的 `token_prices` 按 `(chain_id, token, 月)` 折叠为每月最后一行，既释放 95%+ 的历史价格空间，又保证停牌老币仍有最后价格。 |
| **3. 冷盘自动迁移** | `storage_policy`、`cold_volume`、`cold_after_days` | 所有按月分区的事实表与行情表 | 需要在 ClickHouse 服务端 XML 配置包含 `default` 卷与 `<cold_volume>` 卷的 `storage_policy`（如 `hot_cold`）。超过 `cold_after_days` 的数据 Part 自动迁移到大容量机械盘/云盘。 |
| **4. 整分区到期淘汰** | `trades_days`、`candle_days`、`raw_*_days`、`wallet.trades_days` | `trades`、`candles`、`raw_transfers`、`raw_trades`、`oracle_updates`、`wallet_trades` | `trades` 与 `raw_*` 开启了 `ttl_only_drop_parts = 1`，即整个月度分区的最新一行都过期后才**整块删除 Part**，避免后台逐行重写造成的磁盘 I/O 抖动。注意：<ul><li>`rankings.candle_interval` 对应的 K 线周期在 `candle_days` 中**至少保留 2 天**；</li><li>`raw_trades_days` **必须覆盖 `market.backfill_blocks`**（默认约 7 天），否则迟到精度回填读不到原始交易；</li><li>`wallet.trades_days`（默认 120 天）**必须大于最大统计窗口（90 天）**。</li></ul> |

---

## 4. 开发与测试指南

需要 Go 1.26.10 和 Buf。

```sh
make generate   # 重新生成配置 proto、abigen 绑定、wire_gen.go 和 proto/stream/v1，并 tidy 两个模块
make verify     # buf lint、gofmt、golangci-lint、中文注释检查 commentlint、go test -race
```

- **Wire**：每个命令的 `main.go` 用 Kratos config 把 `-conf` 加载到组件的 `conf.Bootstrap`，`wire.go` 列出 `wireApp` 组装 `kratos.App` 所需的 provider set。修改构造函数或 provider set 后运行 `make wire`（或 `go generate ./cmd/...`）重写 `wire_gen.go`。
- **配置 proto**：修改组件的 `conf/conf.proto` 或 `internal/pkg/config/common.proto` 后运行 `make config`。
- **链上合约 ABI 绑定**：所有节点读取都通过 [oh-my-blockchain](https://github.com/AloneAtWar/oh-my-blockchain) 的 EVM SDK（`internal/pkg/rpc`），由它负责批量、重试和通过 multicall 合约聚合调用。ABI 原样复制到 `internal/pkg/contract/<contract>/`，旁边的 `*_generate.go` 用 `go.mod` 中的 `go-ethereum` 版本运行 `abigen`；新增合约后运行 `make abi`。
- **私有依赖访问**：`oh-my-blockchain` 是私有仓库，本地需设置 `GOPRIVATE=github.com/AloneAtWar` 并配置 Git SSH 访问（`git config --global url."git@github.com:".insteadOf "https://github.com/"`）。

### 测试命令

```sh
make test
CLICKHOUSE_ADDR=127.0.0.1:9000 make test                                   # 打开 ClickHouse 集成测试（含 ch-loader 基于 kfake 的端到端测试）
POSTGRES_DSN=postgres://market:market@127.0.0.1:5432/market make test      # 打开 PostgreSQL 集成测试
```

`market-engine` 基于 `kfake` 的迟到交易回填测试同时需要 ClickHouse 与 PostgreSQL。

---

## 5. 可观测性指标与运维排障手册（Troubleshooting Playbook）

### 5.1 核心 Prometheus 指标速查

每个命令都在 `metrics.addr` 上提供 Prometheus `/metrics`（默认关闭，可通过 `-metrics-addr :9101` 覆盖）。所有指标前缀为 `chainstream_market_`，带 `chain` 标签；用 Prometheus 的 `job` 标签区分服务。

| 指标 | 产出的服务 | 含义与告警关注点 |
| --- | --- | --- |
| `engine_consumer_lag_records` | 5 个链 Topic 消费者 | 链 Topic 最新高水位与最后提交 `kafka_offset` 的差值；持续增长说明消费速度慢于出块或卡在 RPC / 数据库 |
| `engine_block_lag_seconds` | 5 个链 Topic 消费者 | 当前墙钟时间减去最后处理区块的 `block_time` |
| `engine_height` | 5 个链 Topic 消费者 | 最后成功提交的区块高度 |
| `engine_record_seconds{kind}` | 5 个链 Topic 消费者 | 处理并提交单条记录（`block` 或 `revert`）的耗时直方图 |
| `engine_reorgs_total`、`engine_reorg_depth_blocks` | 5 个链 Topic 消费者 | 实际回滚了已应用区块的重组次数与每次回滚的区块深度 |
| `rpc_requests_total{result}`、`rpc_request_seconds` | `holder-indexer`、`market-engine` | 发往 EVM 节点的 JSON-RPC 批量请求计数与耗时（SDK 每次失败最多重试 3 次） |
| `rpc_failures_total` | `holder-indexer`、`market-engine` | 批次级 RPC 失败或单调用除合约 `revert` 外的节点错误；非零增长需排查节点限流或缺少状态 |
| `rankings_refresh_seconds`、`rankings_refresh_failures_total` | `market-rankings` | 代币排行榜单轮刷新耗时与失败次数 |
| `wallet_height`、`wallet_block_lag_seconds`、`wallet_trades_total`、`wallet_backfills_total` | `market-wallet` | 钱包账本最后提交的 `apply_height`、延迟秒数、入账流水笔数、因 live 缺口从 ClickHouse 补缺的次数 |
| `wallet_refresh_seconds`、`wallet_refresh_failures_total`、`wallet_labels{label}` | `market-wallet` | 钱包周期统计汇总耗时、失败次数，以及当前带 `smart_money` / `bot` 标签的钱包总数 |
| `http_requests_total{route,code}`、`http_request_seconds{route}` | `market-api` | 按路由模式与状态码统计的 HTTP 请求量与延迟分布 |

### 5.2 常见故障诊断与恢复步骤

| 故障现象 / 错误日志 | 根本原因分析 | 排查与恢复步骤 |
| --- | --- | --- |
| **`ErrReorgTooDeep`（进程报错退出）** | 收到 `BlockRevert{rollback_to: R}`，但 $R < \text{pruned\_through}$，所需的 `*_undo` 记录已被裁剪删除。通常是下游配置的 `history_depth` 小于上游 `chainstream-evm` 的 `scanner.cache_size`，或节点切换到了一条分叉深度超过 256 块的异常节点。 | 1. 检查节点是否切到了错误分叉；<br>2. 确保所有组件的 `history_depth >= chainstream-evm.scanner.cache_size`；<br>3. 若确属超深真实重组，需清空受影响组件在该链的业务表与 checkpoint（或对 `market-engine` 切回更早快照重放）。 |
| **`ErrHeightGap` 或 `ErrParentMismatch`** | 收到 `BlockEvent` 的高度不是 `checkpoint.Height + 1`，或 `parent_hash != checkpoint.Hash`，且前面没有收到对应的 `BlockRevert`。常见于手工清理了上游 Redis 游标但未清理 Kafka Topic，或两个不同起点的 `chainstream-evm` 写进了同一个 Topic。 | 1. 检查是否有多个 `chainstream-evm` 实例同时向同一 Topic 写入；<br>2. 若上游重置了扫描起点，需同步重建 Kafka Topic 并清理下游对应 `chain_id` 的表与 `*_checkpoints`。 |
| **`holder-indexer` 报错停止且 `rpc_failures_total` 飙升** | 1. 使用了 `state_reads: exact` 但节点不是 Archive 节点，追赶旧块时报错 `missing trie node`；<br>2. 公共 RPC 节点触发并发/批量限流（429 Too Many Requests）。 | 1. 非 Archive 节点务必将 `chains[].state_reads` 设为 `latest`（默认）；<br>2. 调小 `chains[].rpc_batch_size`（如从 100 调至 20）或更换高配 RPC 节点；注意普通合约 `balanceOf` 自身执行 revert 会被自动降级估算，不会导致进程停止。 |
| **`market-engine` 日志警告迟到回填等待 `ch-loader` 超时** | 新代币迟到 `decimals` 到达触发 ClickHouse `raw_trades` 回填时，等待 `backfill_wait`（默认 30s）后 `ch-loader` 的 `raw_checkpoints` 仍未追到目标高度。此时 engine 仅回放内存 `late` 挂起的交易。 | 1. 检查 `ch-loader` 是否存活、其 `engine_consumer_lag_records` 是否堆积；<br>2. 追块高峰期可适当调大 `market.backfill_wait`（如 `60s`）或 `market.late_trade_blocks`。 |
| **`market-wallet` 的 `wallet_backfills_total` 持续快速增长** | `market-wallet` 在 live topic 上频繁发现高度缺口而回退查 ClickHouse `trades`。常见于 `market-engine` 正在追赶历史块（落后超过 `live.max_lag` 的块不发往 live topic）。 | 追块期间属于**完全正常的预期行为**（依靠 ClickHouse 批量补齐比逐块走 Kafka live 更快）；若已追平链头后仍在增长，检查 `market-engine` 与 `market-wallet` 的 `live_topic` 配置是否一致。 |
| **前端 WebSocket 频繁断开（Close Code `1013`）** | 客户端消费 WebSocket 消息过慢，积压超过 `live.client_buffer`（默认 1024 条）。`market-api` 主动以 `1013 Try Again Later` 断开以防客户端漏掉 `revert` / `candle_reset`。 | 减少单连接同时订阅的高频频道数，或在服务端配置中适当调大 `live.client_buffer` 与 `live.candle_throttle`（如从 `250ms` 调至 `500ms`）。 |

---

## 6. Web 前端架构与本地联调

![Web 前端六大页面与 market-api 接口 / 实时频道映射](../../generated-images/web-ui-dataflow.png)

`web/` 是 React + TypeScript 单页应用（Vite 构建），所有数据都来自 `market-api`：

| 页面路由 | 核心功能与数据源 |
| --- | --- |
| `/{chain}` | `market-rankings` 的代币列表，可按 24h 成交量、涨跌幅、流动性、持有人数或创建时间排序，支持最低流动性过滤和符号 / 地址搜索，每 15 秒轮询刷新 |
| `/{chain}/launchpad` | `market-launchpad` 的三栏并排：新币（`new`）、按进度排序的发射中（`bonding`）、已迁移（`migrated`），可按平台（four.meme / Flap / Pons / Bankr / Clanker / Zora 等）筛选，每栏每 5 秒轮询 |
| `/{chain}/token/{address}` | 价格、24h 统计、代币聚合 K 线与实时交易，盈利榜（按已实现盈亏排序的交易者）/ 持有人 / 池子 / 转账标签页，以及 registry 元数据和价格推导过程；发射平台代币还会显示曲线进度、已募集和迁移信息 |
| `/{chain}/pool/{pool}` | 池子自己的 K 线和实时交易、代币和元数据；集中流动性池（Uniswap V3/V4、PancakeSwap V3/Infinity）另有**流动性分布图**：当前价附近各价格区间的活跃流动性，可缩放、切换计价方向，悬停看区间币量，附最近区间明细表，每 15 秒刷新 |
| `/{chain}/smart-money` | `market-wallet` 的钱包排行：7 / 30 / 90 天窗口，聪明钱或全部钱包，按已实现盈亏、ROI、胜率或成交额排序；旁边是聪明钱实时动向流（订阅 `trades:<chain>:label:smart_money`） |
| `/{chain}/wallet/{address}`（也可用 `/holder/`） | 钱包画像：标签、各窗口统计和代币收益分布（`>5x` / `2x~5x` / `0~2x` / `-50%~0` / `<-50%`）、当前持仓与已清仓（含未实现与已实现盈亏）、账本流水、实时成交和 ERC-20 转账 |

前端开发与独立 Mock 联调命令：

```sh
cd web
npm ci
npm run mock     # 可选：在 :8080 启动带生成数据和实时推送的假 market-api（无需启动后端与数据库即可开发 UI）
npm run dev      # http://localhost:5173
npm test         # 单元测试：格式化、K 线与交易合并、WebSocket 客户端状态机
npm run build    # 类型检查并构建到 dist/
make docker-web  # 在仓库根目录：构建 chainstream-web 镜像
```
