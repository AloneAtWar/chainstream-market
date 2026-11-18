# market-registry：代币与池子的最新元数据

`market-registry` 将链 topic 中反复出现的 `AssetFact` 和 `PoolFact` 折叠成每个资产、每个池子的当前元数据。它相当于“最新登记表”，而 ch-loader 的 `asset_facts` / `pool_facts` 保留完整事实历史。

| 项目 | 内容 |
| --- | --- |
| 启动入口 | `cmd/market-registry`；默认配置 `configs/market-registry.yaml` |
| 业务代码 | `internal/app/registry/biz/registry.go` |
| 存储代码 | `internal/app/registry/data/registry.go` |
| 输入 | 原始链 topic 的 `assets` 与 `pools` |
| 输出 | PostgreSQL `registry_tokens`、`registry_pools`、`registry_undo`、`registry_checkpoints` |
| 下游 | market-api、market-rankings、market-launchpad 的元数据补充 |

## 1. 基本流程

1. `chainsync` 按每条链自己的 offset 顺序提供区块和回滚。
2. 一个块内先按“地址 + 标准”合并资产事实，按 pool ID 合并池事实，避免同一键反复写。
3. 地址去空格并小写；`source_height <= 0` 时用当前块高兜底。
4. 清洗元数据：空值和 `null` 变为无元数据，非法 JSON 丢弃，递归删除 JSON 字符串及对象键中的 NUL。
5. 从合法 `asset.Info` JSON 提取 name、symbol、decimals 为独立可查询列。
6. 存储层先为每个首次被本块修改的行保存 undo，再按 source height 和识别规则 upsert。
7. 当前行、undo、裁剪和检查点在同一个 PostgreSQL 事务中提交。

## 2. 核心规则：不是简单的“后到的覆盖先到的”

### 2.1 元数据来源高度

事实有 `source_height`，表示元数据真正读取 / 观察的位置。一个新到的消息可能携带更旧的状态，所以只有以下来源可替换当前元数据：

```text
new.source_height >= stored.source_height
```

资产的 `created` 和 deployment 是单调事实：即使某条较旧事实不能覆盖元数据，它带来的创建证据仍会被保留。池子的 `created` 也做 OR 合并；池子的空 metadata 不会擦除已有 metadata。

### 2.2 同一地址可能探测多个标准

chainstream-evm 可以对一个地址分别报告多个 standard。registry 最终每个地址只存一行，选择顺序是：

1. `recognized` 的事实优先；
2. 否则优先与数据库中已有 standard 相同的事实，使这个标准自己的后续负结果可以撤销旧识别；
3. 再比较 source height 和块内出现顺序。

更新时还有一道保护：未识别的新 standard 不能覆盖另一个 standard 已识别出的行。这个设计避免一次失败的 ERC-20 探测把有效名称和 decimals 清空。

### 2.3 首次高度与更新高度的语义

`first_height` 是 registry 第一次存储该行的流区块，不等于合约部署高度；部署高度在 `deployment.block_height` 中。`updated_height` 是最后一次通过更新规则替换元数据的应用高度，也不一定等于元数据的 `source_height`。

## 3. 表与字段

### 3.1 `registry_tokens`

主键：`(chain_id, address)`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `BIGINT` | 链 ID |
| `address` | `TEXT` | 资产合约地址，规范化小写 |
| `standard` | `TEXT` | 当前选中的资产标准 |
| `recognition` | `TEXT` | 当前标准的识别状态；`recognized` 表示通过对应探测 |
| `metadata_complete` | `BOOLEAN` | 上游是否认为当前元数据完整 |
| `name` | `TEXT NULL` | 从 metadata 提取的名称；未知为 `NULL` |
| `symbol` | `TEXT NULL` | 从 metadata 提取的符号；用于前缀搜索 |
| `decimals` | `SMALLINT NULL` | 从已知 ERC-20 元数据提取的小数位；未知为 `NULL` |
| `metadata` | `JSONB NULL` | 清洗后的 `asset.Info` 原始 JSON；保留其它协议字段 |
| `created` | `BOOLEAN` | 是否曾收到创建事实或部署证据；一旦为真不会被后续观察取消 |
| `deployment` | `JSONB NULL` | 部署信息对象，见下表 |
| `source_height` | `BIGINT` | 当前元数据的实际来源高度，用于拒绝迟到的旧观察 |
| `first_height` | `BIGINT` | registry 第一次存储该地址的应用块高 |
| `updated_height` | `BIGINT` | 最近一次实际替换元数据的应用块高 |

`deployment` JSONB 的键：

| 键 | 含义 |
| --- | --- |
| `block_height` | 合约部署块高；rankings 的 `created_height` 来自这里 |
| `block_time` | 部署块 Unix 秒；rankings 的 `created_time` 来自这里 |
| `transaction_index` | 部署交易在块内位置 |
| `transaction_hash` | 部署交易哈希 |
| `trace_id` | 创建操作 trace ID |

`registry_tokens_symbol` 支持按小写 symbol 查找；`registry_tokens_symbol_prefix` 使用 `text_pattern_ops` 支持非 C collation 下的符号前缀搜索。

### 3.2 `registry_pools`

主键：`(chain_id, pool_id)`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `BIGINT` | 链 ID |
| `pool_id` | `TEXT` | 池 ID；可能是地址，也可能是协议特定作用域 ID |
| `protocol` | `TEXT` | 协议名 |
| `dex_address` | `TEXT` | DEX 合约地址 |
| `tokens` | `TEXT[]` | 池内代币地址，保留顺序；GIN 索引支持“包含某代币”查询 |
| `created` | `BOOLEAN` | 是否曾观察到池创建；按 OR 保留 |
| `source_height` | `BIGINT` | 当前池观察 / 元数据的来源高度 |
| `source_time` | `BIGINT` | 来源时间的 Unix 秒；token pools 按它展示最近观察 |
| `metadata` | `JSONB NULL` | 协议特定池元数据；新事实没带时保留旧值 |
| `first_height` | `BIGINT` | registry 第一次存此池的应用块高 |
| `updated_height` | `BIGINT` | 最近一次按 source height 规则更新此池的应用块高 |

多币池的 `tokens` 可以超过两个。pool ID 不应在调用方擅自拆成代币地址；具体结构由协议和 chainstream-evm 决定。

### 3.3 `registry_undo`

主键：`(chain_id, block_height, kind, key)`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `BIGINT` | 链 ID |
| `block_height` | `BIGINT` | 第一次修改目标行的应用块高 |
| `kind` | `TEXT` | `token` 或 `pool`，决定恢复到哪张当前表 |
| `key` | `TEXT` | token 的 address 或 pool 的 pool ID |
| `prev` | `JSONB NULL` | 该块修改前的整行 JSON；`NULL` 表示该行当时不存在 |

同一行在一个块内可以经过多次合并，但只保存块前第一次旧镜像。回滚时先删除所有被涉及行，再恢复每个键在回滚区间中**最早**的一条非空 undo；没有旧镜像即保持删除。

### 3.4 `registry_checkpoints`

主键：`chain_id`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `chain_id` | `BIGINT` | 链 ID |
| `kafka_offset` | `BIGINT` | 最后提交的原始流 offset；文件源时为记录序号 |
| `block_height` | `BIGINT` | 最后应用高度 |
| `block_hash` | `TEXT` | 最后应用块哈希；有效回滚后清空 |
| `pruned_through` | `BIGINT` | undo 已裁剪到的高度，只增不减 |
| `updated_at` | `TIMESTAMPTZ` | 最后提交时间 |

### 3.5 与原始事实表的关系

| 表 | 写入者 | 与 registry 的区别 |
| --- | --- | --- |
| `asset_facts` | ch-loader | 每次资产观察都一行，保留 source height、完整部署列和未清洗 JSON |
| `pool_facts` | ch-loader | 每次池观察都一行，不覆盖历史 |
| `registry_tokens` / `registry_pools` | market-registry | 每个键只留当前值，负责清洗、冲突选择和可查询列 |

## 4. 设计巧思

- **读取高度与到达顺序分离**：避免“网络里后到”被误当成“链上更新”。
- **创建证据单调保留**：元数据可以替换，部署事实不能因一次旧观察或空观察而丢失。
- **完整行镜像 undo**：当前表结构复杂且更新规则多，回滚恢复整行比为每种字段设计差分更可靠。
- **JSON 清洗与索引列并存**：保留原始协议信息，同时把常用 name / symbol / decimals 提升为可索引列。
- **懒于下游、勤于写入**：API 不需要在请求时解析 metadata JSON 或扫描事实历史。

## 5. 重组、重启与边界

- 回滚只恢复 `rollback_to` 以上 undo 涉及的行；其它行不动。
- undo 裁剪到 `max(finalized_height, height - registry.history_depth)`；回滚低于裁剪高度会报 `ErrReorgTooDeep` 并停止。
- 所有当前行、undo 和 checkpoint 在一个 PostgreSQL 事务内，因此重启无需跨表对账。
- registry 提供“已见过的最新元数据”，不是链上完整资产清单。上游没发布过的资产不会被发现。
- name / symbol / decimals 依赖上游识别结果；`metadata_complete=false` 或字段未知时必须保留 `NULL`，不能当作空字符串或 0。
- 地址统一小写的行为面向当前支持的地址形式；它不承担 TRON base58 的展示规范化，后者主要由 holder 流程专门处理。

## 6. 配置与代码索引

`registry.history_depth` 默认 256，应至少覆盖 chainstream-evm 的 `scanner.cache_size`。`source`、`kafka`、`postgres` 和 `metrics` 使用共享配置段。

| 关注点 | 路径 |
| --- | --- |
| 块内合并、source height、JSON 清洗 | `internal/app/registry/biz/registry.go` |
| 多标准选择、upsert 与 undo | `internal/app/registry/data/registry.go` |
| 元数据字段提取 | `internal/pkg/asset/metadata.go` |
| 表 DDL 与索引 | `internal/pkg/pgdb/schema.go` |
| API 元数据读取 | `internal/app/api/data/registry.go` |
