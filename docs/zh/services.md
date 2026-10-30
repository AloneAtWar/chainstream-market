# 其它服务

本页介绍 market-engine 之外的五个组件：market-api（含实时推送）、holder-indexer、market-registry、market-rankings 和 ch-loader。
除 market-api 和 market-rankings 外，它们都通过 [chainsync](architecture.md) 各自按 offset 顺序读链 topic、维护自己的检查点。

## market-api

```sh
go run ./cmd/market-api -conf configs/market-api.yaml   # 需要 clickhouse.addr 和 postgres.dsn
```

代码：`internal/app/api`（`service/api.go` 是 HTTP 处理，`data` 是 ClickHouse / PostgreSQL 查询）。
查询直接打到 ClickHouse，本地规模下足够快；只有延迟确实不够时才考虑加 Redis 热视图。

| 接口 | 返回 |
| --- | --- |
| `GET /v1/chains/{chain}/tokens/{token}/price` | 最新 USD 价格、来源、跳数和流动性 |
| `GET /v1/chains/{chain}/tokens/{token}/candles?interval=1m` | 代币聚合 K 线（已按流动性过滤） |
| `GET /v1/chains/{chain}/pools/{pool}/candles?interval=1m` | 池子 K 线 |
| `GET /v1/chains/{chain}/tokens/{token}/trades` | 以该代币为 base 的最近交易，新的在前 |
| `GET /v1/chains/{chain}/pools/{pool}/trades` | 池子最近的交易 |
| `GET /v1/chains/{chain}/tokens/{token}/holders` | 持有人数和最大持有人；`balance_raw` 为原始单位，registry 知道小数位时附带 `decimals` 和换算后的 `balance` |
| `GET /v1/chains/{chain}/tokens/{token}` | registry 中的代币元数据：标准、识别状态、名称、符号、小数位、部署信息、原始元数据 JSON |
| `GET /v1/chains/{chain}/tokens/{token}/pools` | 交易该代币的池子，最近观察到的在前，支持 `limit` |
| `GET /v1/chains/{chain}/pools/{pool}` | 池子元数据：协议、DEX、代币、原始元数据 JSON |
| `GET /v1/chains/{chain}/pools/{pool}/liquidity` | 集中流动性池在当前价下各价格区间的币量、合计及 `band_bps` 内的深度（来自 engine 的 tick 表）；表过期或 hook 可能在表外结算时 `usable` 为 false；engine 没有该池的表时返回 404 |
| `GET /v1/chains/{chain}/tokens?sort=volume_24h` | market-rankings 生成的代币列表 |
| `GET /v1/chains/{chain}/tokens/{token}/transfers` | 代币的 ERC-20 转账（来自 ch-loader），`holder` 可限定某一方 |
| `GET /v1/chains/{chain}/holders/{holder}/transfers` | 某地址跨代币的转入转出，`token` 可限定代币 |
| `GET /v1/stream` | 实时交易与 K 线的 WebSocket，需开启 `live.enabled` |
| `GET /healthz` | 进程存活时返回 204 |

约定：

- `{chain}` 是数字链 ID。价格、K 线、交易接口中，原生币伪地址是包装代币的别名；持有人和 registry 接口按原样使用地址。
- K 线支持 `from`、`to`（unix 秒，`from <= open_time < to`）和 `limit`；`interval` 必须在 `api.intervals` 中，
  而 `api.intervals` 又必须是 engine `market.intervals` 的子集。没有交易的桶不返回，每个返回的桶都以前一根的收盘价开盘。
- 交易支持 `limit` 和 `before_height` 向前翻页。
- 转账支持 `limit`、`before_height`、`before_log_index`；整页返回时带 `next: {before_height, before_log_index}`，原样传回即可继续，
  不会跳过同一块剩余的转账。金额为原始单位 `amount_raw`；只存 ERC-20 转账，所以原生币伪地址不做映射。
- 所有小数值以字符串返回。

## 实时推送（Live push）

开启 `live.enabled: true` 后，market-engine 把每块派生出的数据发布到每条链的 live topic，market-api 再通过 `GET /v1/stream`
推给 WebSocket 客户端。未最终确定的数据立即推送，重组后再纠正，和存储的处理方式一致。

```text
链 topic -> market-engine -> {topic}-live（Kafka 或 Redis） -> market-api hub -> WebSocket 客户端
```

代码：`internal/pkg/live`（`message.go` 消息格式，`publisher.go` engine 侧发布，`hub.go` api 侧分发，`transport.go` Kafka / Redis 传输）。

**传输**：`live.driver` 为 `kafka` 时写 `{topic}-live` 的 0 号分区；为 `redis` 时发布到 `live.redis` 上同名的 Pub/Sub 频道。
两者对客户端表现一致，因为 hub 只跟随它运行期间发布的内容。Redis 不需要建 topic，适合不用 Kafka 的部署（例如 `source.driver: file`）。

**发布**：区块输出写入存储之后、检查点提交之前，engine 把该块定好方向的交易和所有改动过的 K 线桶写到 `chains[].live_topic`
（默认 `<topic>-live`）。回滚时写出回滚到的高度，以及被回滚区块触及的每个 K 线序列的重置消息。

- 写入失败会像存储失败一样让 engine 停止，所以每条记录**至少发布一次**；重放会重发相同的值，客户端直接覆盖即可。
- 比 `live.max_lag` 更旧的区块不发布，避免追块时把历史推给客户端；回滚消息总是发布。

**分发**：market-api 不使用消费组，从末尾读取每个 live topic，所以每个实例服务自己的客户端。

- 交易在区块到达时立即发出。
- K 线桶和链头每隔 `live.candle_throttle` 合并发送一次，每个桶只发最新值。
- 客户端落后超过 `live.client_buffer` 条消息会被断开（关闭码 1013），而不是悄悄跳过纠正消息；客户端应重连并重新加载历史。

### 协议

客户端发送 JSON 请求：

```json
{"op": "subscribe", "channels": ["candles:1:token:0xc02a...:1m", "trades:1:pool:0x88e6..."]}
{"op": "unsubscribe", "channels": ["trades:1:pool:0x88e6..."]}
```

| 频道 | 内容 |
| --- | --- |
| `trades:<chain>:pool:<pool>` | 池子的交易，同 `/pools/{pool}/trades` |
| `trades:<chain>:token:<token>` | 以该代币为 base 的交易，同 `/tokens/{token}/trades` |
| `candles:<chain>:pool:<pool>:<interval>` | 池子 K 线，`interval` 取自 `api.intervals` |
| `candles:<chain>:token:<token>:<interval>` | 代币聚合 K 线 |

地址统一小写，代币别名（如原生币伪地址）映射为包装代币。`subscribed` 回复列出服务端规范化后的频道名，之后的消息都用这个写法。
每个连接最多订阅 `live.max_subscriptions` 个频道；非法请求收到 `error` 消息，且不改变任何订阅。

| 消息 | 含义 |
| --- | --- |
| `{"type":"trades","channel","height","trades":[...]}` | 新交易。字段同 REST，另带 `revision_height`（发出它的区块）。迟到回放的交易保留自己的 `block_height`，但 `revision_height` 更晚 |
| `{"type":"candle","channel","candle":{...}}` | 某个桶的完整当前值（带 `update_height`），按 `open_time` 替换 |
| `{"type":"head","chain_id","height","finalized_height"}` | 链上最新推送的区块；`finalized_height` 及以下的区块永远不会被回滚 |
| `{"type":"revert","chain_id","rollback_to"}` | 发生重组：丢弃该链上 `revision_height > rollback_to` 的所有交易；发给订阅了该链任一频道的连接 |
| `{"type":"candle_reset","channel","candle":{...} 或 null}` | 紧跟在 revert 之后，每个受影响序列一条。candle 是该序列在 `rollback_to` 时的最新桶：替换它并丢弃更新的桶；`null` 表示当时没有桶，全部丢弃 |

### 客户端如何合并历史与实时数据

按顺序应用消息，最终结果与存储一致：回滚前 hub 会先把已合并的内容发出，所以 reset 一定在最后。

为了无缝衔接历史与实时数据：**先订阅，再通过 REST 加载历史，最后应用期间收到的消息**。
K 线按 `open_time` 覆盖、交易按 `(block_height, transaction_hash, log_index, operation_index)` 覆盖，重叠部分无害。
空桶不会推送，像 REST K 线一样按前一根收盘价画平即可。

reset 之所以精确：序列只会向后新增桶，迟到交易也只会给原本没有桶的序列建桶；因此 `rollback_to` 时的当前桶是被回滚区块唯一可能改动过的旧桶，
engine 从回滚所用的同一份撤销历史中读出它。

其它来源的浏览器需要列在 `live.allowed_origins` 中（例如 `localhost:5173`）。该流**没有鉴权**。

## holder-indexer

```sh
go run ./cmd/holder-indexer -conf configs/holder-indexer.yaml   # 需要 postgres.dsn 和 chains[].rpc_url
```

代码：`internal/app/holder`。维护见过的每一笔 ERC-20 持仓，可选维护原生币余额。

- **初始化（seeding）**：某个 `(token, holder)` 第一次出现时，用该块哈希调用 `balanceOf` 读取余额，所以初值已包含本块转账和 indexer 启动前的历史。
  节点必须能提供该块的状态（全节点在链头附近都可以）；拿不到状态时 indexer 停止而不是猜测。
  `balanceOf` 本身 revert 时，用本块净转账作为初值，并标记为未验证。
- **累加**：之后每块按持仓累加净转账。mint / burn 使用零地址，零地址不算持有人；零金额转账忽略。
- **对账（reconcile）**：每 `holders.reconcile_every` 块，挑 `reconcile_batch` 个最久未验证的持仓用 `balanceOf` 校正。
  这能纠正 rebase 代币和收税代币——它们的余额变化没有对应的 `Transfer`。
- **原生币**：开启 `holders.native_balances` 后，ETH / BNB 余额存在同一组表里，代币地址为 `0xeeee…eeee`。
  gas、出块奖励、L2 费用都会改变余额却没有转账，所以不做累加：对本块 `native_touched` 中的每个地址（发送方、原生转账双方、出块收益地址），
  在该块哈希上用 Multicall3 的 `getEthBalance`（与 `balanceOf` 一起批量）读取，把与存储值的差作为已验证的增量。
  余额为零的地址不保存。chainstream-evm 未开 `need_internal` 时内部转账的接收方、信标链提款不会被触及，靠对账纠正。
  没有已知 multicall 合约的链（如本地开发链）记录警告并跳过原生币。
- **重组**：每次变化都以可撤销增量保存，回滚时减去被回滚区块加上的量，并删除它们新建的持仓。

| 表 | 内容 |
| --- | --- |
| `holder_checkpoints` | 每条链一行：Kafka offset、高度、哈希、已裁剪高度 |
| `holder_balances` | `(chain_id, token, holder) -> balance NUMERIC(78,0)`，以及 `balanceOf` 最后确认的高度 |
| `holder_undo` | 每块每个持仓加上的增量或是否新建，以及之前的验证高度；裁剪方式同 `engine_state_undo` |
| `holder_counts` | `(chain_id, token) -> holders`，正余额数量；与余额在同一事务中增减，回滚时同样回退 |

余额只存在 PostgreSQL 中，内存不会随持有人数量增长。

## market-registry

```sh
go run ./cmd/market-registry -conf configs/market-registry.yaml   # 需要 postgres.dsn
```

代码：`internal/app/registry`。保存见过的每个资产（`AssetFact`）和池子（`PoolFact`）的最新元数据。

- **代币**：每个地址一行，包括标准、识别状态、`metadata_complete`、发布的 `asset.Info` JSON，以及从中读出的名称、符号、小数位。
  ERC-20 字段只有被 chainstream-evm 标记为已知时才采用。`created` 事实或任何带部署信息的事实会设置 `created` 并保留部署信息。
- **池子**：每个池子 ID 一行，包括协议、DEX 地址、代币、来源高度和时间、协议元数据 JSON。不带元数据的事实保留已存储的元数据。
  `tokens` 上的 GIN 索引用于查询代币的池子。
- **新的覆盖旧的**：除非已存储的元数据读自更高的来源高度，否则新事实覆盖旧元数据，迟到的旧观察会被忽略。`created` 和部署信息始终保留。
- **重组**：区块第一次修改某行之前，把该行（或"不存在"）存入 `registry_undo`；回滚时恢复 `rollback_to` 之上最早保存的镜像。
  撤销历史按最终确定高度和 `registry.history_depth` 裁剪。
- **清洗**：不是合法 JSON 的元数据丢弃；去掉 PostgreSQL 不接受、而代币名里可能出现的 NUL 字符。

| 表 | 内容 |
| --- | --- |
| `registry_checkpoints` | 每条链一行：Kafka offset、高度、哈希、已裁剪高度 |
| `registry_tokens` | `(chain_id, address)` 及上述字段，外加首次存储和最后更新的高度 |
| `registry_pools` | `(chain_id, pool_id)` 及上述字段，外加首次和最后更新的高度 |
| `registry_undo` | 每块每行修改前的值（JSONB），该块新建时为 NULL |

## market-rankings

```sh
go run ./cmd/market-rankings -conf configs/market-rankings.yaml   # 需要 clickhouse.addr 和 postgres.dsn
```

代码：`internal/app/rankings`。它不消费 topic，而是每隔 `rankings.refresh_interval` 重建每条链的 `token_rankings`，
在一个事务里提交，market-api 的代币列表用一条带索引的查询读取。

- **24h 活跃度**：成交量、成交笔数和涨跌幅来自 `rankings.candle_interval` 周期的代币聚合 K 线，汇总链头（ClickHouse 中最新区块，而非墙上时钟）
  之前 24 小时。窗口从桶边界开始，所以最多比 24h 长一个周期。每个桶以前一根收盘价开盘，第一个桶的开盘价就是窗口前的价格，
  `price_change_24h = close / open - 1`（`-0.25` 表示跌 25%）。窗口内没有交易的代币归零。
- **价格与流动性**：最新一行 `token_prices`。只重新读取自上次刷新链头（减去 `rankings.reorg_window`）以来有定价的代币，首次刷新读取全部。
  最近价格被重组删除的代币回落到更早的价格或失去价格。流动性取价格来源池的流动性。
- **持有人数**：读 holder-indexer 维护的 `holder_counts`，刷新时从不统计 `holder_balances`。
- **创建时间**：market-registry 记录的部署高度和时间（已知时）。

代币有价格或在窗口内有交易就会进入排行。活跃度、持有人数和最近价格每次刷新都重算，所以排行在一个刷新周期内跟上重组。

| 表 | 内容 |
| --- | --- |
| `token_rankings` | `(chain_id, token)` 及价格、流动性、24h 成交量 / 笔数 / 涨跌幅、持有人数、创建高度，每个排序列一个索引 |
| `ranking_checkpoints` | 每条链一行：上次刷新的链头时间（下次价格扫描的起点）和刷新时间 |

`GET /v1/chains/{chain}/tokens` 参数：

- `sort`：`volume_24h`（默认）、`price_change_24h`、`liquidity`、`holders`、`created`；`created` 只列出已知部署信息的代币。
- `order`：`desc`（默认）或 `asc`，并列时按地址同向排序。
- `q`：完整地址精确查找；其它内容按符号前缀（不区分大小写）匹配。搜索覆盖 registry 中所有代币，包括没有市场数据的（`"ranked": false`）。
  符号完全匹配的排最前，其次是已排名的代币，再按 `sort`。
- `min_liquidity_usd`、`limit`（默认 200，最多 1000）、`offset`（最多 10000）。

名称、符号、小数位来自 market-registry；响应中的 `refreshed_at` 是上次刷新的 unix 时间。

## ch-loader

```sh
go run ./cmd/ch-loader -conf configs/ch-loader.yaml   # 需要 clickhouse.addr
```

代码：`internal/app/loader`。把 chainstream-evm 发布的事实原样存入 ClickHouse，不做任何派生；地址统一小写。

| 表 | 内容 |
| --- | --- |
| `raw_transfers` | 所有 ERC-20 转账（含零金额），`amount UInt256`；`token` 和 `[from_address, to_address]` 上有 bloom filter 索引 |
| `raw_trades` | 定方向之前的池子操作：动作、sender、trader、recipient、按池子视角带符号的金额（并列数组）、交易后的池子状态 |
| `oracle_updates` | 任意合约发出的所有 `AnswerUpdated` 日志，`answer Int256` |
| `asset_facts` | 代币观察与创建：标准、识别状态、部署信息、`metadata_json` |
| `pool_facts` | 池子观察与创建：协议、代币、来源高度、`metadata_json` |
| `raw_checkpoints` | 每次 flush 一行：Kafka offset、高度、哈希；最新 offset 即检查点 |

事实表按 `(chain_id, block_height, ...)` 排序，`block_height` 是发布该事实的区块。

- **检查点存在 ClickHouse**：ClickHouse 没有事务，所以 flush 先写事实、再写检查点。检查点与事实在同一个库，删除或恢复库时二者一起变化，ch-loader 不需要 PostgreSQL。
- **重放**：crash 可能留下检查点之上的事实。重启后第一个区块或回滚会先删除检查点下一高度及以上的所有数据再插入，所以重放后只有一份。
  部分失败的 flush 在重试前也会这样清理。
- **重组**：回滚丢弃缓冲中 `rollback_to` 之上的区块，并用轻量级 `DELETE` 删除已存储的事实。原始事实不需要撤销历史，任意深度的重组都能回滚。
- **批量**：比 `loader.batch_lag` 更旧的区块（追块中）先缓冲，累计到 `flush_rows` 条或 `flush_interval` 时间后一起写入，每张表一次插入；
  更新的区块在各自提交时立即写入，API 马上可见。正常停止时会写出缓冲区。
- **保留期**：`clickhouse.retention` 中的 `raw_transfers_days`、`raw_trades_days`、`oracle_updates_days` 由 ch-loader 在启动时设置；
  `asset_facts` 和 `pool_facts` 永久保留。注意 market-engine 的[回填](market-engine.md#回填backfill)依赖 `raw_trades`。
