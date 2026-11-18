# 组件文档

本目录按 `cmd/` 下的八个可执行组件逐一说明：输入与输出、基本流程、核心算法、涉及的表与逐字段含义、重组和重启语义，以及实现中的关键取舍。

所有页面都依据当前代码与 DDL 编写。更深入的设计专题仍保留在上一级目录，组件页会链接过去，避免同一内容出现多个互相矛盾的版本。

## 阅读顺序

1. 先读[整体架构](../architecture.md)，理解链 topic、共享顺序消费循环和存储边界。
2. 按关注的数据选择组件：行情看 `market-engine`，原始事实看 `ch-loader`，接口和推送看 `market-api`。
3. 排查重组或重启问题时，重点看各页的“一致性与边界”以及 `*_checkpoints`、`*_undo` 字段。

## 八个组件

| 组件 | 文档 | 核心职责 | 主要写入 |
| --- | --- | --- | --- |
| `market-engine` | [market-engine.md](market-engine.md) | 池状态、USD 价格图、交易定方向、K 线、实时消息 | ClickHouse 行情表；可选 PostgreSQL 状态表 |
| `ch-loader` | [ch-loader.md](ch-loader.md) | 不派生、不遗漏地保存原始链上事实 | ClickHouse `raw_*`、事实表和检查点 |
| `holder-indexer` | [holder-indexer.md](holder-indexer.md) | ERC-20 / TRC-20 与可选原生币余额、持有人数 | PostgreSQL `holder_*` |
| `market-registry` | [market-registry.md](market-registry.md) | 最新代币与池子元数据 | PostgreSQL `registry_*` |
| `market-launchpad` | [market-launchpad.md](market-launchpad.md) | 曲线与直发代币的生命周期、进度和三栏列表数据 | PostgreSQL `launchpad_*` |
| `market-rankings` | [market-rankings.md](market-rankings.md) | 将行情、持有人数和部署信息汇总成代币榜单 | PostgreSQL `token_rankings`、`ranking_checkpoints` |
| `market-wallet` | [market-wallet.md](market-wallet.md) | 钱包成本账本、盈亏统计、聪明钱和机器人标签 | PostgreSQL 与 ClickHouse 的 `wallet_*` |
| `market-api` | [market-api.md](market-api.md) | 聚合只读查询、REST API、WebSocket 扇出 | 业务上只读；启动时确保表结构存在 |

## 通用术语

| 术语 | 含义 |
| --- | --- |
| 链 topic | chainstream-evm 发布的 `BlockEvent` / `BlockRevert` 有序流；可来自 Kafka 分区 0 或 protojson 行文件 |
| live topic | market-engine 发布的派生行情流；market-api 和 market-wallet 使用，和原始链 topic 不是一回事 |
| 当前状态表 | 每个业务键只保留当前值，例如 `holder_balances`、`registry_tokens`、`wallet_positions` |
| 事实 / 流水表 | 保存每次发生的事件，例如 `raw_transfers`、`trades`、`wallet_trades` |
| undo | 为重组保存的可逆信息；有的是旧行镜像，有的是增量，不能把它们都理解成快照 |
| checkpoint | 最后完整提交的流位置；字段名叫 `kafka_offset` 时，文件源也用它保存记录序号 |
| `block_height` | 事实或成交实际发生的区块高度 |
| `revision_height` | market-engine 实际写出这条交易或价格的区块高度；迟到回填时可大于 `block_height` |
| `apply_height` | market-wallet 实际把流水记入账本的区块高度，对应上游的 revision 高度 |
| `update_height` | 某个 K 线桶当前版本最后一次由哪个区块更新；也是回滚 K 线的依据 |

## 表字段的权威来源

- PostgreSQL：`internal/pkg/pgdb/schema.go`、`internal/pkg/pgdb/wallet.go`。
- ClickHouse 行情与原始事实：`internal/pkg/chdb/schema.go`。
- ClickHouse 钱包表：`internal/pkg/chdb/wallet.go`。

组件文档中的字段表用于解释语义；新增列、索引或枚举时，应同步修改对应组件页。具体 SQL 类型、默认值和索引仍以这些 DDL 文件为准。
