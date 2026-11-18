# ch-loader：原始事实归档与回填底座

`ch-loader` 是原始事实的 ClickHouse 写入器。它不做交易定方向、USD 定价或余额计算，而是尽量保持 chainstream-evm 发布的事实原貌，让转账查询、审计和 market-engine 的迟到精度回填不必重新消费整条 Kafka 流。

| 项目 | 内容 |
| --- | --- |
| 启动入口 | `cmd/ch-loader`；默认配置 `configs/ch-loader.yaml` |
| 业务代码 | `internal/app/loader/biz/loader.go` |
| 存储代码 | `internal/app/loader/data/raw.go`、`internal/pkg/chdb/raw.go` |
| 输入 | 每条链的原始 topic |
| 输出 | ClickHouse 五张事实表与 `raw_checkpoints` |
| 下游 | market-api 的转账查询；market-engine 的 raw trade 回填 |

## 1. 基本流程

1. `chainsync` 按 offset 读 `BlockEvent` / `BlockRevert`，并处理重复记录、高度连续性与 parent hash。
2. `model.NewRawBlock` 将块拆成 transfers、trades、oracle updates、asset facts、pool facts；地址转小写，其它事实保持上游语义。
3. 新块先进入内存 pending；接近链头时立即 flush，追块时按行数或时间攒批。
4. flush 对五张事实表分别执行一次批量 INSERT，**全部事实写完后才追加检查点**。
5. 回滚时先丢弃 pending 中回滚点以上的块，再删除 ClickHouse 中 `block_height` 高于回滚点的事实，最后推进检查点。
6. 正常停止时写出尚未 flush 的缓冲区。

它不需要 PostgreSQL：事实和检查点位于同一个 ClickHouse 数据库，备份、删除或恢复数据库时二者一起移动。

## 2. 核心流程：无跨表事务下怎样保证可重放

ClickHouse 不提供这五张表与检查点之间的原子事务，所以采用“**事实先行，检查点最后**”：

```text
WriteRaw(pending) → SaveRawCheckpoint(latest)
```

崩溃只允许出现“事实已写、检查点未写”，不会出现“检查点宣称完成、事实却没写”。恢复时：

- 启动后第一条 block：先删除 `block_height >= 当前块高` 的旧事实，再插入当前块；
- 启动后第一条 revert：先删除 `block_height > rollback_to` 的旧事实；
- 某次 flush 部分失败后直接重试：先删除 pending 第一个块高度及以上的事实，再重写整批。

因此重复消费不会让同一高度留下两套事实。代价是恢复清理依赖按高度删除，且过程中读取方可能看到尚未被检查点覆盖的新事实；这与系统整体的未 finalized 数据可见策略一致。

## 3. 涉及哪些表

| 表 | 内容 | 主要读取方 |
| --- | --- | --- |
| `raw_transfers` | 每条 ERC-20 Transfer 日志，包括零金额 | market-api |
| `raw_trades` | 尚未定方向的池操作及交易后池状态 | market-engine backfill |
| `oracle_updates` | 任意合约的 Chainlink 风格 `AnswerUpdated` 日志 | 原始事实查询 / 审计 |
| `asset_facts` | 每次资产观察或创建，不做“最新值”折叠 | 原始事实查询 / 审计 |
| `pool_facts` | 每次池观察或创建，保留历史 | 原始事实查询 / 审计 |
| `raw_checkpoints` | 每次 flush 的流位置；最高 offset 是当前位置 | ch-loader 自身；engine 等待回填时读取 |

五张事实表都按 `block_time` 月分区，排序键以 `(chain_id, block_height, …)` 开头。这使回滚、重放清理和按时间保留都围绕同一个高度维度。

### 3.1 `raw_transfers`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `UInt64` | 链 ID |
| `block_height` | `Int64` | 日志所在块高；回滚与重放清理按它执行 |
| `block_time` | `DateTime('UTC')` | 日志所在块时间；月分区依据 |
| `transaction_hash` | `String` | 产生日志的交易哈希 |
| `transaction_index` | `Int64` | 交易在块内位置 |
| `log_index` | `UInt32` | Transfer 日志在块内的序号 |
| `token` | `String` | 发出 Transfer 的代币合约地址 |
| `from_address` | `String` | 事件中的发送方；mint 时通常是零地址 |
| `to_address` | `String` | 事件中的接收方；burn 时通常是零地址 |
| `amount` | `UInt256` | 原始最小单位数量；零金额也保存，不换算 decimals |

`token` 和 `[from_address, to_address]` 上有 bloom filter 跳数索引。后者是一个数组索引，使“from = 地址 OR to = 地址”能用 `has` 一次过滤，而不是让两个独立索引分别处理 OR。

### 3.2 `raw_trades`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `UInt64` | 链 ID |
| `block_height` / `block_time` | `Int64` / `DateTime` | 操作所在块高和 UTC 时间 |
| `transaction_hash` | `String` | 操作所属交易哈希 |
| `transaction_index` | `Int64` | 交易在块内位置 |
| `log_index` | `UInt32` | 触发操作的日志序号 |
| `operation_index` | `UInt32` | 同一日志包含多个操作时的子序号 |
| `protocol` | `LowCardinality(String)` | 协议名 |
| `dex_address` | `String` | 上游给出的 DEX 合约地址 |
| `pool_id` | `String` | 池 ID；保留协议特定格式 |
| `action` | `LowCardinality(String)` | `swap`、`liquidity_added`、`liquidity_removed` 或 `unspecified` |
| `sender` | `String` | 上游事件的 sender，不一定等于交易发起方 |
| `trader` | `String` | 交易发起方，供后续钱包归属使用 |
| `recipient` | `String` | 事件中的接收方 |
| `amount_assets` | `Array(String)` | 各金额腿的资产地址 |
| `amount_raw` | `Array(String)` | 各腿的原始单位有符号整数，池子视角；流出为负、流入为正 |
| `amount_values` | `Array(String)` | 已按 decimals 换算的人类单位值；未知时为空字符串 |
| `amount_decimals` | `Array(UInt32)` | 各腿 decimals；仅在对应 known 为真时有效 |
| `amount_decimals_known` | `Array(Bool)` | 对应下标的 decimals 是否确实已知 |
| `reserves_after` | `Array(String)` | 操作后的池储备原始整数，顺序与池代币顺序对应；未提供时为空数组 |
| `sqrt_price_x96` | `String` | 集中流动性池操作后的 Q64.96 价格；未提供时为空 |
| `liquidity` | `String` | 操作后的 active liquidity 原始整数；未提供时为空 |
| `tick` | `String` | 操作后的当前 tick；未提供时为空 |
| `active_bin_id` | `Nullable(UInt32)` | Liquidity Book 等协议的 active bin；不适用为 `NULL` |
| `curve_progress` | `String` | 交易后 bonding curve 进度原始整数，1e18 = 100%；非曲线为空 |
| `curve_base_remaining` | `String` | 曲线剩余可售 base 原始整数 |
| `curve_quote_raised` | `String` | 曲线已募集 quote 原始整数 |
| `curve_price` | `String` | 每 1e18 最小单位 base 对应的 quote 最小单位价格 |

五个 `amount_*` 数组**按下标对齐**。例如第 `i` 个资产、raw、value、decimals、known 描述同一条腿，不能分别排序。`amount_assets` 的 bloom filter 专门支持 market-engine 按代币找回历史 raw trades。

### 3.3 `oracle_updates`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `UInt64` | 链 ID |
| `block_height` / `block_time` | `Int64` / `DateTime` | 日志所在块高和 UTC 时间 |
| `transaction_hash` | `String` | 交易哈希 |
| `transaction_index` | `Int64` | 交易在块内位置 |
| `log_index` | `UInt32` | `AnswerUpdated` 日志序号 |
| `feed` | `String` | 实际发出日志的合约地址；可能是 aggregator，不一定是配置中的 proxy |
| `answer` | `Int256` | 原始有符号答案；小数位由具体 feed 决定 |
| `round_id` | `UInt256` | Chainlink round ID |
| `updated_at` | `Int64` | 事件中报告的链上更新时间，Unix 秒；不是 loader 写库时间 |

这里保存**所有**此类日志，不只保存 engine 配置信任的 feed；是否信任由 market-engine 的 feed 配置和 proxy 解析决定。

### 3.4 `asset_facts`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `UInt64` | 链 ID |
| `block_height` / `block_time` | `Int64` / `DateTime` | 发布该事实的区块高度和时间 |
| `address` | `String` | 被观察资产地址 |
| `standard` | `LowCardinality(String)` | 上游探测的资产标准，如 ERC-20 |
| `recognition` | `LowCardinality(String)` | 对该标准的识别结果 |
| `metadata_complete` | `Bool` | 上游是否认为元数据完整 |
| `type` | `LowCardinality(String)` | `observed`、`created` 或 `unspecified` |
| `source_height` | `Int64` | 元数据实际读取 / 观察高度；可与发布块高不同 |
| `deployment_height` | `Int64` | 合约部署块高；未知时为零值 |
| `deployment_time` | `Int64` | 部署块时间，Unix 秒 |
| `deployment_tx_index` | `Int64` | 部署交易在块内位置 |
| `deployment_tx_hash` | `String` | 部署交易哈希 |
| `deployment_trace_id` | `String` | 部署 trace 的标识，用于定位创建操作 |
| `metadata_json` | `String` | 上游发布的原始资产元数据 JSON 文本；loader 不清洗、不展开 |

### 3.5 `pool_facts`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `UInt64` | 链 ID |
| `block_height` / `block_time` | `Int64` / `DateTime` | 发布该事实的块高和时间 |
| `pool_id` | `String` | 池 ID |
| `protocol` | `LowCardinality(String)` | 池所属协议 |
| `dex_address` | `String` | DEX 合约地址 |
| `token_addresses` | `Array(String)` | 池内代币地址，保留上游顺序 |
| `source_height` | `Int64` | 池元数据实际来源高度 |
| `source_time` | `Int64` | 池元数据来源时间，Unix 秒 |
| `created` | `Bool` | 该事实是否表示池创建 |
| `metadata_json` | `String` | 协议特定的原始元数据 JSON 文本 |

### 3.6 `raw_checkpoints`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `UInt64` | 链 ID；ReplacingMergeTree 排序键 |
| `kafka_offset` | `Int64` | 本次 flush 最后提交的流 offset；文件源时为记录序号；也是合并版本值 |
| `block_height` | `Int64` | 最后应用高度 |
| `block_hash` | `String` | 最后应用块哈希 |
| `updated_at` | `DateTime64(3,'UTC')` | flush 提交时间 |

读取时按 `kafka_offset DESC LIMIT 1` 选当前位置，不依赖后台 merge 是否已发生。它没有 `pruned_through`，因为原始事实可以按高度直接删除，不需要有限深度的 undo 历史。

## 4. 设计巧思

- **派生可重算，原始事实先行**：raw trades 让迟到精度回填成为数据库读取，而不是跨服务重放协调。
- **并列数组保留协议结构**：多币池和池视角符号不丢失，同时避免为每个协议创建一张表。
- **检查点作为追加事实**：无需 PostgreSQL，也无需给 ClickHouse 假设一个跨表事务。
- **热头与追块两种提交策略**：新块即时可见；历史块批量写，减少每块一个 part 的写放大。
- **无 undo 的任意深度回滚**：原始事实直接按高度删除即可，不需要保存旧行镜像；但仍受 Kafka / 文件是否保留所需历史影响。
- **保留事实历史而非只留最新**：`asset_facts` / `pool_facts` 与 registry 的当前行互补，可审计元数据何时、从哪个高度出现。

## 5. 保留期、依赖与边界

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `loader.batch_lag` | 5m | 比它旧的块进入批量模式；0 表示每块提交 |
| `loader.flush_rows` | 100000 | 一个批次达到的事实总行数阈值 |
| `loader.flush_interval` | 2s | 追块批次最长等待时间 |
| `clickhouse.retention.raw_transfers_days` | 90 | 原始转账保留天数，按块时间 |
| `clickhouse.retention.raw_trades_days` | 90 | 原始交易保留天数；会限制 engine 可回填历史 |
| `clickhouse.retention.oracle_updates_days` | 0 | 0 表示永久保留 |
| `clickhouse.retention.storage_policy` 等 | 见配置 | 可选冷热分层，须与 ClickHouse 服务端策略匹配 |

`asset_facts` 与 `pool_facts` 永久保留。事实表使用 `ttl_only_drop_parts`，按月整个 part 过期，因此实际数据可能比配置天数多留最多约一个分区周期。TTL 改变会触发后台重写，不应由只读服务重复设置。

ch-loader 不负责校验 JSON 的业务合法性，也不把 asset / pool 事实合并成最新值；这些属于 market-registry。它保存的是事件事实，不保证 Transfer 能完整解释余额，也不提供 holder 计算。

## 6. 代码索引

| 关注点 | 路径 |
| --- | --- |
| 缓冲、flush、恢复清理 | `internal/app/loader/biz/loader.go` |
| 按高度删除事实 | `internal/app/loader/data/raw.go` |
| 块到各表行的转换 | `internal/pkg/model/rawblock.go` |
| 批量 INSERT 与行模型 | `internal/pkg/chdb/raw.go` |
| DDL、排序键、跳数索引 | `internal/pkg/chdb/schema.go` |
| TTL 与冷热分层 | `internal/pkg/chdb/retention.go` |
