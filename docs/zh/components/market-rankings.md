# market-rankings：代币榜单读模型

`market-rankings` 不消费链 topic，而是定期把多个组件的数据汇总为 PostgreSQL 中的 `token_rankings`。它把 market-api 的代币列表从“实时关联多张历史大表”变成“一次有索引的当前表查询”。

| 项目 | 内容 |
| --- | --- |
| 启动入口 | `cmd/market-rankings`；默认配置 `configs/market-rankings.yaml` |
| 业务代码 | `internal/app/rankings/biz/rankings.go` |
| 数据代码 | `internal/app/rankings/data` |
| 读取 | ClickHouse `blocks`、`candles`、`token_prices`；PostgreSQL `holder_counts`、`registry_tokens` |
| 写入 | PostgreSQL `token_rankings`、`ranking_checkpoints` |
| 下游 | market-api 的代币列表，以及 launchpad / wallet 等需要最新价格的读取 |

## 1. 基本流程

每条链一个独立 refresher，启动后立即执行，随后每隔 `rankings.refresh_interval` 执行：

1. 从 ClickHouse `blocks` 最大块高取得链头时间；还没有块时本轮不做任何事。
2. 读取 `ranking_checkpoints.head_time` 作为上次价格扫描游标。
3. 计算最近价格：首次全量；后续只扫描 `上次链头 - reorg_window` 以来有价格的代币，并额外检查此窗口内已有排行的代币。
4. 从 token K 线计算 24h volume、trades 和 price change。
5. 在一个 PostgreSQL 事务里 upsert 价格和活跃度、清除失效数据、联读 holder counts 和 deployment、更新刷新游标。
6. 失败只记录指标与日志，下一周期重新构建同一读模型；不需要补消费 offset。

## 2. 核心计算

### 2.1 24h 窗口由链头决定

```text
head  = ClickHouse blocks 中最高区块的 block_time
since = (head - 24h) 向下取整到 rankings.candle_interval
```

只使用 `candles` 中 `subject='token'`、指定 interval、`open_time >= since` 的当前桶版本。由于起点对齐桶边界，实际覆盖窗口最多比 24h 多一个 interval；链头所在的未完结桶也会参与。

这与 launchpad 使用链时间的设计相同：追块或重放历史时，榜单对应数据流链头，而不是服务器当前日期。

### 2.2 每个桶先选版本，再聚合

`candles` 为同一逻辑桶保留多个 update height 版本。刷新先按 `(key, open_time)` 用 `argMax(..., update_height)` 得到当前 OHLCV，再按 token 汇总：

- `volume_24h_usd = sum(bucket.volume_usd)`；
- `trades_24h = sum(bucket.trades)`；
- `open = 最早桶的 open`，`close = 最晚桶的 close`；
- `price_change_24h = close / open - 1`，open 非正时为 0。

因为新桶从前一桶 close 开盘，若窗口前已有交易，首个桶 open 就是窗口起点的衔接价。结果为比例：`-0.25` 表示下跌 25%，不是 -25 美元，也不是百分数 -25。

### 2.3 最新价格与流动性

每个 token 从 `token_prices` 取最大 `block_height` 对应的价格、时间和来源池流动性：

- `price_usd` 可为空：代币在 24h 内交易过，但没有自己的可用 USD 价格时仍可入榜；
- `liquidity_usd` 是来源池对手方储备价值，不是全市场 TVL；
- `price_height` / `price_time` 描述这条价格的链上坐标，不是 rankings 刷新时间。

### 2.4 增量扫描也要能发现“价格被删了”

只查最近新增的 `token_prices` 不足以处理重组：某个代币的最近价格可能已被回滚删除。因此 refresher 还从当前 `token_rankings` 找出 `price_time >= since` 的代币，强制重新查询它们：

- 若还有更早价格，回退到那条价格；
- 若一条都不剩，将 price 字段置 `NULL`、liquidity 归零；
- 窗口外的老价格视为不会被当前允许重组改变，保留当前读模型。

因此 `reorg_window` 是**时间维度**的保护窗，必须覆盖业务允许的重组回溯时间；它和 engine 的块数 `history_depth` 不是同一个参数。

## 3. 表与字段

### 3.1 `token_rankings`

主键：`(chain_id, token)`。一行是一个代币最近一次刷新后的展示值。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `BIGINT` | 链 ID |
| `token` | `TEXT` | 代币地址；与 registry / holder / engine 的代币键一致 |
| `price_usd` | `NUMERIC NULL` | 最新一单位代币 USD 价；未知为 `NULL`，不能解释为 0 |
| `price_height` | `BIGINT NULL` | 最新价格对应的成交 / 定价块高；无价格为 `NULL`，也用于判断是否仍有价格 |
| `price_time` | `TIMESTAMPTZ NULL` | 最新价格的块时间；增量重组复查和游标过滤使用 |
| `liquidity_usd` | `NUMERIC` | 最新价格来源池的对手方储备 USD 价值；无价格时为 0 |
| `volume_24h_usd` | `NUMERIC` | 链头前 24h 窗口内代币聚合 K 线 USD 成交额；无成交为 0 |
| `trades_24h` | `BIGINT` | 同一窗口纳入代币 K 线的 swap 笔数；无成交为 0 |
| `price_change_24h` | `NUMERIC` | 窗口首桶 open 到末桶 close 的比例变化；无有效开盘价为 0 |
| `holders` | `BIGINT` | 刷新时从 `holder_counts` 复制的正余额地址数；不是本表实时累加 |
| `created_height` | `BIGINT NULL` | `registry_tokens.deployment.block_height`；未知部署为 `NULL` |
| `created_time` | `BIGINT NULL` | 部署块 Unix 秒；未知为 `NULL` |

价格相关列与活跃相关列可在不同轮次由不同来源更新。`volume_24h_usd=0` 与“无价格”是两种状态：后者通过 `price_usd IS NULL` 表示。

索引按 `chain_id + 排序列 + token` 建立，覆盖 volume、price change、liquidity、holders、created；created 是只含已知部署行的部分索引，另有 `price_time` 索引服务重组复查。

### 3.2 `ranking_checkpoints`

主键：`chain_id`。它不是链 topic checkpoint，而是**汇总读模型游标**。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `BIGINT` | 链 ID |
| `head_time` | `TIMESTAMPTZ` | 上次成功刷新时的链头块时间；下次价格扫描从这里减 reorg window |
| `refreshed_at` | `TIMESTAMPTZ` | 上次成功刷新完成的墙钟时间；API 返回 `refreshed_at` |

没有此行表示首次刷新，需要全量读取最新价格。它不含 Kafka offset，因为 rankings 没有流消费位置。

### 3.3 读取但不写入的上游表

| 表 | 使用字段 | 用途 |
| --- | --- | --- |
| `blocks` | `chain_id`、`block_height`、`block_time` | 确定链头时间 |
| `candles` | `subject`、`key`、`interval_seconds`、`open_time`、OHLCV、`trades`、`update_height` | 计算 24h 活跃度与涨跌幅 |
| `token_prices` | `token`、`block_height`、`block_time`、`price_usd`、`liquidity_usd` | 最新价与流动性 |
| `holder_counts` | `chain_id`、`token`、`holders` | 复制正余额人数 |
| `registry_tokens` | `address`、`deployment` | 补充创建高度和时间；API 另读 name / symbol / decimals |

这些上游字段的完整语义见对应组件文档：[market-engine](market-engine.md)、[holder-indexer](holder-indexer.md)、[market-registry](market-registry.md)。

## 4. 一次提交具体做什么

`CommitRankings` 在一个事务中：

1. 写入 / 更新本轮最新 quote；仅值实际变化时更新行。
2. 对扫描覆盖但没返回价格的代币清空价格与流动性。
3. 将本轮没有活跃度的旧排行代币的 volume / trades / change 归零。
4. upsert 本轮活跃度。
5. 删除既无价格又 `trades_24h=0` 的代币，防止榜单无限积累失效行。
6. 对所有当前排行行联读 `holder_counts`，无对应行则为 0。
7. 联读 registry deployment，更新或清空 created 字段。
8. 更新 `ranking_checkpoints`。

因此 API 不会看到同一事务内“价格已经刷新但 holders 尚未刷新”的中间状态。不过 ClickHouse 读取和 PostgreSQL 提交不是跨库快照，一轮刷新使用的是各源当时可见的数据，下一轮会再修正。

## 5. 设计巧思与边界

- **读模型与计算分离**：engine 专注逐块事实，rankings 用可失败重试的周期任务构建列表。
- **所有排序都有索引**：分页只读所需行，而不是在 API 请求里临时聚合 candles。
- **链头窗口 + 当前 K 线版本**：重组后，下一次刷新自然读到修正结果，无需自己的 undo 表。
- **有意归零与删除**：沉默代币不会永久保留旧 24h 数字；无价格且无成交的行会退出排行。
- **holders 来自增量维护表**：刷新从不扫描 `holder_balances`。
- **不是精确财务报表**：窗口会对齐 K 线桶，最新价、持有人数和元数据来自不同服务的各自进度；刷新之间也存在短暂不一致。

## 6. API 查询与配置

`GET /v1/chains/{chain}/tokens` 支持：

- `sort=volume_24h | price_change_24h | liquidity | holders | created`；
- `order=desc | asc`，同值时用 token 地址做同向稳定次序；
- `q`：完整地址精确查，否则按 registry symbol 前缀查；搜索还能返回无行情代币，`ranked=false`；
- `min_liquidity_usd`、`limit`、`offset`。

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `rankings.refresh_interval` | 1m | 每条链刷新周期；启动时立即刷新 |
| `rankings.candle_interval` | 5m | 必须是 engine 实际生成且至少保留 2 天的 token K 线周期 |
| `rankings.reorg_window` | 1h | 最近价格回看时间；须覆盖需要识别的重组范围 |

## 7. 代码索引

| 关注点 | 路径 |
| --- | --- |
| 刷新编排与链头游标 | `internal/app/rankings/biz/rankings.go` |
| ClickHouse 当前桶与最新价查询 | `internal/app/rankings/data/source.go` |
| PostgreSQL 事务、归零与删除 | `internal/app/rankings/data/rankings.go` |
| 表 DDL 与排序索引 | `internal/pkg/pgdb/schema.go` |
| API 列表与搜索 | `internal/app/api/data/rankings.go`、`service/rankings.go` |
