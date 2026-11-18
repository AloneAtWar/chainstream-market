# 持有人余额：初始化、增量、对账、回滚与链上读取

本文解释 `chainstream-market` 当前怎样维护**持有人余额**，并结合上游 `chainstream-evm` 和 `oh-my-blockchain`（下文简称 OMB）说明为什么这么设计。

这里的“余额”特指 `holder-indexer` 维护的：

- ERC-20 / TRC-20 余额：按 `(chain_id, token, holder)` 保存；
- 可选的原生币余额：ETH、BNB 等也放在同一组表里，用 `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee` 作为伪代币地址。

本文**不讨论**另外三类容易混淆的数据：`market-wallet` 的成本账本 `wallet_positions`、Curve/Balancer 池的余额快照、ClickHouse 的原始转账表 `raw_transfers`。它们不是这里所说的持有人余额，也不参与 `holder-indexer` 的余额计算。

代码基线：

- `chainstream-market`：`3e79001ad957c7556eaae1fa39a284f20afab93b`；
- `chainstream-evm`：`955cdea4e2a1d75a5a2d2ba97ca652ada7a7f5a4`；
- OMB：两个项目的 `go.mod` 均指向 `cac108d91357ae1be7d15c51c11db291393d9aa5`。

## 1. 先看整条链路：为什么不能只把转账加起来

```text
EVM 节点
   │
   │ 区块、收据、trace
   ▼
OMB chainscanner：按 parent hash 识别分叉，给出 RollbackTo
   │
   ▼
chainstream-evm：生成 BlockEvent / BlockRevert
   │  BlockEvent.transfers       ERC-20 转账事实
   │  BlockEvent.native_touched  本块原生币余额可能变化的地址
   │  BlockEvent.block_hash      当前块的哈希
   │  BlockRevert.rollback_to    回滚到公共祖先高度
   ▼
Kafka 分区 0 或 protojson 文件（按 offset 有序，至少一次投递）
   │
   ▼
holder-indexer ───────────────► EVM 节点
   │                              balanceOf / getEthBalance
   │  PostgreSQL 事务              exact：按块哈希
   │  ├─ holder_balances           latest：至少当前块高的链头
   │  ├─ holder_counts
   │  ├─ holder_undo
   │  └─ holder_checkpoints
   ▼
market-api / market-rankings
```

只累加 `Transfer` 会遇到三类问题：

1. **索引器不是从创世块启动**：一个地址在启动前已经有 500 个币，启动后只出现一笔 1000 个币的 mint；如果从零开始加，得到的只有 1000，而链上实际是 1500。
2. **并非所有余额变化都有对应的 `Transfer`**：rebase、某些收税代币、gas、出块奖励、L2 费用和信标链提款都会造成偏差。
3. **最近区块可能重组**：已经展示或入库的余额可能来自随后被孤立的分支。

所以当前实现采用混合模型：

> **首次出现时读链上全量余额，之后累加转账；周期性读链上全量余额纠偏；每块留下可撤销记录，重组时回滚。**

其中最重要的一个小巧思是：不仅记录“余额是多少”，还记录“这份余额已经包含到哪一块”。没有这条信息，默认的 `latest` 读取很容易重复计算转账，详见第 3、4 节。

## 2. 余额存储涉及哪些表

### 2.1 四张核心表的分工

| 表 | 一句话职责 | 没有它会怎样 |
| --- | --- | --- |
| `holder_balances` | 当前余额，以及余额包含到哪个块、最近何时校验 | 没有可查询的当前状态 |
| `holder_undo` | 每块对每个持仓施加了什么变化，以及变化前的两个高度 | 无法精确撤销重组块和对账修正 |
| `holder_counts` | 每个代币的正余额持仓数 | 每次查询人数都要扫描全部余额 |
| `holder_checkpoints` | 每条链消费到哪条流记录、哪个块，撤销历史裁剪到哪里 | 重启无法安全续传，也无法限制过深回滚 |

一次区块或回滚的提交会在**同一个 PostgreSQL 事务**里更新这些表。不会出现“余额已经改了，但检查点没改”或“余额回滚了，但人数没回滚”的持久化中间状态。

### 2.2 完整 DDL

以下是 `internal/pkg/pgdb/schema.go` 中 holder 相关的原始语句，包含后续补列、索引和 `holder_counts` 的一次性历史回填。各服务启动时先创建并选定 PostgreSQL schema，再在按 schema 命名的 advisory lock 下执行这些语句。

```sql
CREATE TABLE IF NOT EXISTS holder_checkpoints (
    chain_id       BIGINT PRIMARY KEY,
    kafka_offset   BIGINT NOT NULL,
    block_height   BIGINT NOT NULL,
    block_hash     TEXT NOT NULL,
    pruned_through BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS holder_balances (
    chain_id        BIGINT NOT NULL,
    token           TEXT NOT NULL,
    holder          TEXT NOT NULL,
    balance         NUMERIC(78, 0) NOT NULL,
    verified_height BIGINT,
    PRIMARY KEY (chain_id, token, holder)
);

CREATE INDEX IF NOT EXISTS holder_balances_top
    ON holder_balances (chain_id, token, balance DESC) WHERE balance > 0;

CREATE INDEX IF NOT EXISTS holder_balances_verified
    ON holder_balances (chain_id, verified_height NULLS FIRST);

ALTER TABLE holder_balances
    ADD COLUMN IF NOT EXISTS as_of_height BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS holder_undo (
    chain_id      BIGINT NOT NULL,
    block_height  BIGINT NOT NULL,
    token         TEXT NOT NULL,
    holder        TEXT NOT NULL,
    delta         NUMERIC(78, 0) NOT NULL,
    created       BOOLEAN NOT NULL,
    prev_verified BIGINT,
    PRIMARY KEY (chain_id, block_height, token, holder)
);

ALTER TABLE holder_undo
    ADD COLUMN IF NOT EXISTS prev_as_of BIGINT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF to_regclass('holder_counts') IS NULL THEN
        CREATE TABLE holder_counts (
            chain_id BIGINT NOT NULL,
            token    TEXT NOT NULL,
            holders  BIGINT NOT NULL,
            PRIMARY KEY (chain_id, token)
        );
        INSERT INTO holder_counts (chain_id, token, holders)
        SELECT chain_id, token, count(*)
        FROM holder_balances
        WHERE balance > 0
        GROUP BY chain_id, token;
    END IF;
END $$;
```

`NUMERIC(78, 0)` 存的是最小单位的十进制整数，不是浮点数，也不是按代币 `decimals` 换算后的数量。78 位十进制整数足以容纳 `uint256`。数据库没有 `balance >= 0` 的约束；异常代币或估算数据可能暂时产生非正数，查询持有人时只取 `balance > 0`。

### 2.3 `holder_balances`：当前状态和两条时间线

| 字段 | 含义与注意事项 |
| --- | --- |
| `chain_id` | 数字链 ID，与代币、地址共同构成主键。 |
| `token` | 代币合约地址；原生币使用 `0xeeee…eeee` 伪地址。EVM 十六进制地址统一小写；TRON base58 地址保留大小写。 |
| `holder` | 持有人地址，地址规范化规则同上。零地址不作为持有人。 |
| `balance` | 当前保存的原始单位余额。它可能是链上读取值，也可能是读取失败时按转账估算的值。 |
| `verified_height` | 最近一次把“已验证”变化提交进库的**索引器应用块高**，并不总是 RPC 真正执行的块高。成功 seed / 重读 / 对账时更新；新建估算行或普通转账累加不更新；重组失效时清为 `NULL`。失败的对账调用也会推进它，见第 5 节。 |
| `as_of_height` | **余额高水位**：这份 `balance` 已包含该块及之前的转账，之后不得重复叠加。成功链上读取时是实际执行块高；读取失败、用本块净转账估算时是当前应用块高；`-1` 表示读取点可能已被重组，必须重读。旧数据迁移时默认 `0`。 |

为什么要把 `verified_height` 和 `as_of_height` 分开？看一个默认 `latest` 模式的例子：

```text
holder-indexer 正在应用块 104
节点链头已经到块 105
balanceOf(Alice) 实际在块 105 执行，返回 1015

入库：
verified_height = 104   -- 这次验证是在应用块 104 时提交的
as_of_height    = 105   -- 1015 已包含块 105 的转账
balance         = 1015
```

如果只存一个“校验高度”，就无法同时回答“对账调度多久没检查它”和“未来哪些转账已经包含在余额里”这两件事。

还要注意：`as_of_height` 是**去重高水位**，不是 finalized 标志，也没有配套保存该高度的块哈希。

### 2.4 `holder_undo`：不是旧余额快照，而是可逆操作

| 字段 | 含义 |
| --- | --- |
| `chain_id` | 链 ID。 |
| `block_height` | 变化所属的**流中应用块高**。latest 读取实际发生在更高块时，undo 仍记在当前应用块。 |
| `token`、`holder` | 被修改的持仓；与应用块高、链 ID 共同构成主键，所以每块每个持仓最多一条 undo。 |
| `delta` | 这一块加到旧余额上的有符号量。回滚时从当前余额减去它；对账误差也记录为 delta。新建行的该值为 0。 |
| `created` | 这一块是否新建了持仓。为真时回滚直接删除整行，而不是尝试恢复一个“不存在的旧余额”。 |
| `prev_verified` | 本块修改前的 `verified_height`，回滚时恢复。 |
| `prev_as_of` | 本块修改前的 `as_of_height`，回滚时恢复。 |

为什么对账也要写 undo？假设块 104 对账发现 rebase 多了 10 个币，当前余额从 1000 修成 1010。若块 104 随后被孤立，这 10 个币的修正也必须一起撤销，否则旧分支的对账结果会污染新分支。

### 2.5 `holder_counts`：只统计正余额

| 字段 | 含义 |
| --- | --- |
| `chain_id` | 链 ID。 |
| `token` | 代币地址或原生币伪地址。 |
| `holders` | 当前 `balance > 0` 的持仓行数。 |

每次写入都根据 `旧余额是否 > 0` 和 `新余额是否 > 0` 计算 `-1 / 0 / +1`，与余额同事务更新。零余额行可以保留在 `holder_balances`，但不计入人数；新建的原生币零余额行则直接不保存。

首次增加这张表时，DDL 会从已有 `holder_balances` 一次性回填，避免升级后人数从零开始。

### 2.6 `holder_checkpoints`：消费进度和撤销边界

| 字段 | 含义 |
| --- | --- |
| `chain_id` | 每条链一行。 |
| `kafka_offset` | 最后完整提交的流记录 offset；文件源中实际上是记录行序号，沿用同一字段名。重启从 `offset + 1` 读取。 |
| `block_height` | 最后应用块高。 |
| `block_hash` | 最后应用块哈希，用于下一块的 parent hash 连续性检查。执行有效回滚后清空，因为此时该高度的规范块身份交给下一条流记录重新建立。 |
| `pruned_through` | 已删除到哪个高度的 undo 历史；只允许单调增大。低于此高度的回滚会被拒绝。 |
| `updated_at` | 最后提交时间。 |

### 2.7 相关但不是余额事实源的表

- `token_rankings.holders` 是 `market-rankings` 定期从 `holder_counts` 复制的排行榜字段，不是实时余额表。
- `raw_transfers` 由 `ch-loader` 独立写入 ClickHouse；`holder-indexer` **不会反查它来计算余额**，而是直接消费 `BlockEvent.transfers`。
- `wallet_positions.quantity` 服务于成本与盈亏账本，不等同于链上实际余额。

## 3. 没有数据时，余额如何初始化

“没有数据”有两层：服务没有检查点，以及某个具体 `(token, holder)` 没有余额行。二者不能混为一谈。

### 3.1 服务冷启动：不是扫描代币合约的全部存储

没有 `holder_checkpoints` 时，消费引擎使用 `offset = -1`，从流的开头读取。这个“开头”是 Kafka 保留的 topic 或文件的开头，不保证是链的创世块；`chainstream-evm` 自身也可能从配置的某个高度或链头开始发布。

当前实现**不会**枚举 ERC-20 合约的全部 holder，也不会在启动时给所有地址批量预填余额。它采用按需发现：

> 地址第一次在本块转账中出现时，才把“数据库没有这一行”转成一个 `balanceOf` 查询。

因此，“没有行”表示**尚未见过、尚未初始化**，不能直接解释成链上余额为零。若数据流没有覆盖代币早期历史，那些后来从未再转账的老 holder 也不会凭空出现。

### 3.2 新持仓的 seed：为什么读全量，而不是从零加本块

假设索引器启动前 Alice 已持有 500，启动后的第一个相关块 100 又 mint 了 1000：

```text
链上块 100 后余额 = 1500
本块 Transfer 增量 = +1000
```

当前实现调用块 100 后的 `balanceOf(Alice)`，得到 1500，直接作为初值，**不再加本块的 +1000**。因为 `balanceOf` 是块后状态，已经包含触发本次初始化的那笔转账。

如果采用“默认零 + 本块净转账”，结果会是 1000，永久丢掉启动前的 500；如果采用“读 1500 后再加 1000”，又会重复成 2500。seed 的核心就是避免这两种错误。

在 `exact` 模式下，读取固定在块 100 的 `block_hash`；在 `latest` 模式下，可能读到更高的块 H，此时初值还包含 H 之前的后续变化，并通过 `as_of_height = H` 防止后面重复累加。

### 3.3 哪些 key 会触发初始化

对每一笔正数 ERC-20 转账，发送方减少、接收方增加；同一 `(token, holder)` 在一个块内先求净额。以下地址不建行：

- 空地址；
- EVM 零地址 `0x0000…0000`；
- TRON 零地址 `T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb`。

因此 mint 只初始化接收方，burn 只处理发送方。零金额转账被忽略，既因为没有余额变化，也避免“向海量地址发零金额事件”的垃圾交易制造大量数据库行。

原生币初始化不同：`native_touched` 中的新地址也会读余额，但只有读到的值大于零才建行。发送方和手续费接收方几乎每块都会出现，把大量零余额地址永久落库没有价值。

### 3.4 读取失败时，哪些情况停止，哪些情况降级

先区分“合约调用失败”和“节点无法提供可信状态”：

| 情况 | 当前处理 | 原因 |
| --- | --- | --- |
| `balanceOf` 执行 revert、地址无代码、返回空或无法解码为 `uint256` | 新 ERC-20 持仓用本块净转账作为估算初值，`verified_height = NULL` | 某些代币不标准，不能让一个坏合约卡死整条链 |
| 已存在且因重组需要重读的 ERC-20 持仓，本次 `balanceOf` 失败 | 加本块非零净转账，`as_of_height` 仍为 `-1`，以后仍需重读 | 暂时保留可用估算，但不伪装成已恢复可信 |
| 原生币读取失败 | 新地址不建行；已有地址不生成修正 | 没有可靠的逐笔原生币总额可替代，尤其还有 gas 和奖励 |
| 节点没有指定块/状态、请求超时、RPC 错误、latest 低于当前应用块、无法证明实际执行高度 | 整次读取报错，当前块不提交，索引器停止或重试 | 此时继续猜会永久错账 |

OMB 对失败的批次默认最多尝试三次。合约自身 revert 与“节点缺少历史状态”不会被混为一谈：前者可以降级，后者必须阻止提交。

## 4. 增量余额变更如何处理

### 4.1 先按块、按持仓求净额

对块 N 中的某个 key `k = (token, holder)`：

```text
delta(N, k) = 本块所有转入 amount 之和 - 本块所有转出 amount 之和
```

地址会先规范化：十六进制地址去空格并转小写；TRON base58 地址只去空格，不错误地转小写。

同一块内有多笔转账时只生成一条净额变化，正好对应 `holder_undo` 的主键“一块、一持仓一条”。自转账的转入和转出相互抵消。

### 4.2 已初始化的 ERC-20：用 `as_of_height` 决定是否累加

设当前保存余额为 B，高水位为 A，正在应用块 N：

| 条件 | 处理 |
| --- | --- |
| `A >= 0` 且 `A < N`，本块净额非零 | `B' = B + delta(N, k)`，不调用 RPC |
| `A >= N` | 保持 B，不叠加本块转账，因为读取时已经包含它 |
| `A = -1` | 不信任当前高水位，先重读；成功后用“读到的值 − 旧值”作为修正 delta |
| key 不存在 | 按第 3 节 seed |

这就是 `latest` 模式最关键的去重规则。

#### 实际场景：读到了“未来”，怎样避免加两遍

```text
正在应用块 100：mint 给 Alice 1000
节点 latest 已到块 103，此时 Alice 的余额为 1300
块 102 还有一笔 Alice 收到 300 的转账
```

处理过程：

1. 应用块 100 时读到 1300，保存 `balance = 1300, as_of_height = 103`；
2. 应用块 101、102、103 时，都不再加 Alice 的转账，因为这些块不高于 103；
3. 应用块 104 时，如果 Alice 又收到 50，才变成 1350。

如果没有 `as_of_height`，块 102 会再加 300，余额变成 1600。这个小字段让系统可以使用普通全节点的 latest 状态，同时保持增量记账不重复。

它还有一个容易忽略的好处：假设 Bob 在块 102 转出 300，但读取点块 103 上 Bob 已经是 0，那么 seed 直接保存 0，而不是错误地从“未知零值”减成 -300。

### 4.3 链上重读不是直接覆盖，而是转成 delta

已有持仓需要重读时，假设旧值为 B，链上读到 Q，实际执行高度为 H：

```text
correction = Q - B
新余额      = B + correction = Q
新 as_of    = H
```

之所以数据库里保存 `correction`，而不是只保存“覆盖成 Q”，是因为 `holder_undo` 可以用减法统一撤销普通转账、原生币刷新和对账修正。

### 4.4 原生币：每一笔都重读 touched 地址，不累加转账

原生币也使用上述表和 delta 机制，但**发现变化和读取策略不同**。

`chainstream-evm` 的 `buildNative` 为每块生成 `native_touched`：

- 所有交易发送方，包括失败交易发送方，因为失败也要付 gas；
- 成功的交易级原生币转账双方；
- 开启内部调用 tracing（`need_internal`）时，成功的价值转移内部调用双方；`delegatecall`、`staticcall`、`callcode` 不当作价值移动；
- 出块者 / fee recipient。

`holder-indexer` 对这些地址去重、排除零地址，然后**每块全部重读**，调用 multicall 合约的 `getEthBalance(address)`。已有地址用差值更新，新地址只保存正余额。

为什么不直接累加 `native_transfers`？例如 Alice 给 Bob 转 1 ETH，同时支付 0.001 ETH gas：

```text
转账事实只能解释：Alice -1，Bob +1
链上实际还包括：Alice -0.001 gas，可能还有 fee recipient 的收入
```

只累加转账必然漏掉 gas。重读 touched 地址则能一次性覆盖转账、gas 和奖励等多种变化。

边界也很明确：未开 `need_internal` 时，内部转账接收方可能没进入 `native_touched`；信标链提款始终不在列表中。已有这些地址的余额行可以靠周期对账补正，但一个从未入表、只收到提款的全新地址不会被对账自动发现。

### 4.5 持仓人数随零 crossing 同步变化

每次 seed、转账或对账修正后，数据库比较旧值和新值：

- 从 `<= 0` 变成 `> 0`：人数加一；
- 从 `> 0` 变成 `<= 0`：人数减一；
- 符号区间没变：人数不变。

这使 `holder_counts` 不需要定期全表 `count(*)`，回滚时也能用同样的 crossing 规则反向修正。

## 5. 余额对账如何处理

### 5.1 对账解决的是“转账事实不完备”

即使 seed 和普通增量都正确，以下情况仍会造成漂移：

- rebase 直接改变账户余额；
- fee-on-transfer 等代币的实际到账与事件金额不完全一致；
- 非标准合约的事件语义不完整；
- 原生币存在 `native_touched` 没覆盖的变化；
- 某些合约调用失败时，初始化曾退回到转账估算。

因此系统定期重新读取链上真值，而不是假设转账之和永远等于余额。

### 5.2 什么时候对账，选哪些地址

仓库 `configs/holder-indexer.yaml` 的关键配置为（其中原生币维护是可选项，不是 Go 配置零值的隐式默认）：

```yaml
holders:
  reconcile_every: 100
  reconcile_batch: 200
  history_depth: 256
  native_balances: true
```

当应用块高满足 `height % reconcile_every == 0` 时：

1. 先处理本块正常转账触发的 seed / 增量 / 重读；
2. 从数据库按 `verified_height NULLS FIRST` 选择最久未验证的持仓；
3. 排除本块已经触碰的 key，避免同块重复读取；
4. 最多保留 `reconcile_batch` 个，批量调用链上余额接口；
5. 成功后用 `actual - stored` 生成修正 delta，推进 `as_of_height` 和 `verified_height`。

查询会多取 `reconcile_batch + 本块 touched 数量` 条再排除 touched，避免“最旧的一批恰好都被本块处理过”导致实际对账数量不足。`verified_height NULLS FIRST` 还让从未验证和刚被重组失效的持仓优先得到检查。

若关闭 `native_balances`，历史遗留的原生币行会被排除在对账批次之外，防止这些不再更新的行长期占满最旧队列。

`reconcile_every` 配置为负数可关闭对账；业务层中的 0 也表示关闭，但配置加载会把未显式设置的 0 填成默认值 100。

### 5.3 一个 rebase 修正及其回滚

```text
块 101：Alice seed 为 1000
块 104：没有 Alice 的 Transfer，但链上 balanceOf = 1010

对账修正：
delta      = +10
balance    = 1010
undo 记录  = block 104 / delta +10 / 修改前的两个高度
```

若之后回滚到块 103，数据库减去这 10，恢复为 1000。对账结果和普通转账一样属于特定分支，不能例外保留。

### 5.4 对账调用失败时的一个细节

周期对账中，如果某个 `balanceOf` 返回链上失败，当前代码：

- 不修改余额；
- 不推进 `as_of_height`；
- 仍生成 `Verified = true, Delta = 0` 的变化，因此会推进 `verified_height`。

所以 `verified_height` 更准确地说是“最近一次验证处理被提交的应用高度”，不是“每次都成功拿到链上值”的严格证明。这个细节也意味着：重组后 `as_of_height = -1` 的行，即使某次对账失败，失效状态仍保留，但它不再一定排在 `NULLS FIRST` 队列的最前面。

### 5.5 对账是有界吞吐，不是全量即时校验

假设共有 1,000,000 个持仓，每 100 块检查 200 个，则平均速度是每块 2 个，理论上一轮要约 500,000 块。实际还受队列并列顺序、失败和本块排除项影响。

所以对账是**最终纠偏机制**，不是每次读取前的强一致保证，也不是 `sum(holder_balances) == totalSupply` 的全局审计。当前没有对 totalSupply 做这种全局核对；税费、销毁、未见过地址和精度语义也会让这种等式并不总成立。

## 6. 余额回滚如何处理

### 6.1 上游先保证“会收到 revert，而且顺序正确”

OMB `chainscanner` 用最近区块的 `(height, hash, parentHash)` 锚点检测分叉，找到公共祖先 R，并给出 `RollbackTo = R`。`chainstream-evm` 在替换块之前发布：

```text
BlockRevert { rollback_to: R }
BlockEvent  { block_height: R + 1, ... }
BlockEvent  { block_height: R + 2, ... }
```

记录按分区 0 或文件顺序排列。发布与上游 Redis 游标不能放进一个事务，因此选择“先发布、后提交游标”，并接受重复块和重复 revert；下游用检查点和幂等回滚消化它们。上游的 pending revert 还会保证发布中途崩溃或“回滚之后又回滚”时，不会漏发必要的 revert。

`holder-indexer` 的 `chainsync` 再做一层防御：

- offset 不超过检查点的记录跳过；
- 高度不超过已应用高度的块视为重复并跳过；
- 高度跳跃或 parent hash 不匹配而没有先收到 revert，停止而不是继续错账；
- 有效 revert 后检查点高度降到 R，哈希清空。

### 6.2 一次回滚在 PostgreSQL 中做什么

回滚到 R 时，`CommitHolders` 在一个事务中依次执行：

1. **删除新建持仓**：删除 `holder_undo` 中 `block_height > R AND created = true` 对应的余额行，并修正人数；
2. **撤销已有持仓的增量**：按持仓汇总 `sum(delta)`，从当前余额中减去；同时取这些块中最早的 `prev_verified`、`prev_as_of` 恢复；
3. **删除已经消费的 undo**：删除 `block_height > R` 的记录；
4. **失效可能来自旧分支的读取**：把其余 `as_of_height > R` 的行设为 `as_of_height = -1, verified_height = NULL`；
5. 按裁剪边界清理过旧 undo，并保存新的检查点。

先删 created 行再做 delta update 是有意的：同一持仓若先在被回滚块中创建、后又在更高块修改，整行都应消失，后面的 delta 不应再作用到它身上。

### 6.3 普通增量怎样精确撤销

沿用前面的例子：

```text
块 100：Alice seed = 1500（created = true）
块 101：Alice 向 Bob 转 300，Alice delta = -300
块 102：Alice 向 Carol 转 100，Alice delta = -100
当前 Alice = 1100

回滚到 101：
删除 Carol 在 102 新建的行
Alice = 1100 - (-100) = 1200
删除 102 的 undo
```

因为记录的是每块实际施加的净变化，不需要重新扫描旧块，也不需要保存每个地址每块的完整余额快照。

### 6.4 latest 读取为什么还需要额外的“失效重读”

假设应用块 100 时，余额实际读自 latest 块 103：

```text
balance = 1300
as_of_height = 103
holder checkpoint 的应用高度 = 100
```

之后链重组到公共祖先 101，旧分支的 102、103 被孤立。仅撤销“应用块 102、103 写入的 delta”不够，因为 1300 这个基数本身可能来自旧分支的块 103。

因此第 4 步把所有 `as_of_height > R` 的余额标成 `-1`：

- 下次该持仓被转账触碰时，先重读再更新；
- 没被触碰的持仓，因为 `verified_height = NULL`，会在后续对账中优先检查；
- 重读产生的修正也写 undo，若新分支再次回滚，可以恢复此前的失效状态。

这里的设计取舍是**先标记不可信，再按需修复**，而不是收到 revert 后立即对全库所有受影响地址发起 RPC。后者在热门代币的大量 holder 上会造成不可控的重组尖峰。

### 6.5 撤销历史能保留多深

每应用一个高度为 H、携带 finalized 高度 F 的块，计算：

```text
pruned_through = max(F, H - history_depth)
```

该值只增不减，删除 `block_height <= pruned_through` 的 undo。收到回滚 R 时，如果 `R < pruned_through`，返回 `ErrReorgTooDeep` 并停止，而不是猜一个旧余额。

例如 `H = 100, history_depth = 4`：

- finalized 未知（0）：裁剪到 96，允许回滚到 96；
- finalized 为 98：裁剪到 98，允许回滚到 98；
- finalized 为 90：仍按深度裁剪到 96。

配置要求 `holders.history_depth >= chainstream-evm.scanner.cache_size`。否则上游仍能检测并发出某个深度的 revert，下游却已经删掉了对应的 undo。

### 6.6 当前实现的两个可见边界

#### 边界一：标记失效不等于 API 立即不可见

`market-api` 只按 `balance > 0` 查询，不检查 `as_of_height = -1`，响应里也不返回 `as_of_height` / `verified_height`。因此在“revert 已提交、该地址尚未重读”的窗口里，旧数值仍可能出现在持有人列表中。

如果该地址之后一直没有被触碰，且对账被关闭，这个状态可能长期存在。当前设计提供的是最终修复，不是重组瞬间的强一致展示。

#### 边界二：等于当前检查点的 revert 可能漏掉 latest 高水位失效

这是从当前分支条件可以直接推演出的特殊场景：

```text
1. holder-indexer 已应用并提交到块 100。
2. 它在处理块 100 时从 latest 块 101 的 A 分支读到 Alice 余额，保存 as_of_height = 101。
3. A101 被孤立，上游发出 BlockRevert{rollback_to: 100}。
4. 此时 holder-indexer 的应用检查点仍是 100。
```

通用 `chainsync.revert` 把 `rollback_to >= checkpoint.Height` 当作“没有已应用块需要撤销”，不会调用 `Handler.Revert`。因此第 6.2 节的 `as_of_height > rollback_to` 失效逻辑也不会运行。

随后替换块 B101 到来时，该持仓已有 `as_of_height = 101`，普通增量规则会认为“B101 的转账已经包含在余额里”，既不重读也不累加。旧 A101 的基数可能继续保留：

- 开启对账时，之后的周期对账仍能纠正；
- 关闭对账时，若没有其他事件迫使它重读，偏差可能长期保留。

现有测试覆盖了“回滚低于已应用高度后重读”，但没有覆盖这个“latest 读取领先一块、revert 正好到检查点”的组合。若要让该路径也即时自愈，合理修正是：**即使无需撤销已应用块，也应对每条有效 revert 执行 `as_of_height > rollback_to` 的失效处理**，并增加上述时间线的端到端测试。这是改进建议，不是当前代码已有的行为。

## 7. 当前如何获取余额

“获取余额”有两条完全不同的路径：

1. `holder-indexer` 为写库而向节点读取链上状态；
2. `market-api` 为对外服务而读取 PostgreSQL，不会在 HTTP 请求里临时调用节点。

先看第一条，因为它决定写入的余额属于哪个块、哪个分支。

### 7.1 每块到底读取哪些地址

并不是每块把所有 holder 全部读一遍。当前块 N 的读取集合来自以下几类：

| 触发原因 | 地址集合 | 是否读链上 |
| --- | --- | --- |
| 新 ERC-20 持仓 | 本块非零转账双方中数据库没有行的 `(token, holder)`，排除零地址 | 是，seed |
| 普通已有 ERC-20 持仓 | 本块转账双方中已有行且 `as_of_height >= 0` | 通常否；只按第 4 节条件累加 |
| 重组后失效的 ERC-20 持仓 | 本块再次触碰且 `as_of_height = -1` 的已有行 | 是，重读并修正 |
| 原生币 | 开启 `native_balances` 时，本块 `native_touched` 的全部地址 | 是，每块都读 |
| 周期对账 | 最久未验证的最多 `reconcile_batch` 个持仓，排除本块 touched key | 是 |

这些查询会合并成批量调用。`rpc_batch_size` 默认 100，限制 JSON-RPC 调度批次大小；OMB 还会按链配置把多个 `eth_call` 聚合成 multicall。ERC-20 和原生币余额可以在同一批中处理。

### 7.2 业务层方法与参数

`holder-indexer` 的接口是：

```go
Balances(
    ctx context.Context,
    block rpc.Block,                 // {Height: 当前应用块高, Hash: 当前应用块哈希}
    queries []rpc.BalanceQuery,      // 每项 {Token: 代币地址, Holder: 地址}
) ([]rpc.Balance, error)
```

返回的每项包含：

```go
type Balance struct {
    Amount *big.Int // 链上调用失败时为 nil
    Height int64    // 实际状态包含到的块高
}
```

对于普通 ERC-20：

```text
to       = query.Token
calldata = balanceOf(query.Holder)
```

calldata 使用 abigen 生成的 ERC-20 绑定打包，不手写 selector。TRON holder 的 base58 地址会先转换为 ABI 所需的 20 字节 hex 形式。

对于原生币：

```text
to       = 该链的 Multicall3 地址；没有时退到 Uniswap interface multicall
calldata = getEthBalance(query.Holder)
```

如果一条链没有可用的 multicall 合约，原生币余额功能记录警告并关闭，ERC-20 仍可继续工作。

### 7.3 `exact`：指定当前块，而且指定块哈希

配置：

```yaml
chains:
  - name: eth
    chain_id: 1
    rpc_url: http://127.0.0.1:8545
    state_reads: exact
```

每个请求构造成：

```go
evmsdk.RPCCallReq{
    Address:   tokenOrMulticall,
    Calldata:  calldata,
    BlockHash: block.Hash,
}
```

OMB 使用 EIP-1898 的区块参数调用 `eth_call`。它内部以 `BlockNumberOrHashWithHash(hash, false)` 构造参数；`requireCanonical=false` 在 go-ethereum 序列化时省略，因此实际发送的 JSON 为：

```json
{
  "blockHash": "0x...当前 BlockEvent.block_hash..."
}
```

所以这里**不是仅按高度读取，而是直接按指定哈希执行**。即使同一个高度出现两个分叉块，只要节点仍保存目标块及其状态，返回的就是目标哈希对应的状态，不会悄悄换成同高度的另一个哈希。返回的 `Balance.Height` 使用当前应用块高。

需要准确理解 `requireCanonical = false`：

- 它是“允许节点按哈希读取一个目前已不在规范链上的块”，不是“额外检查它仍是 canonical”；
- 重组后，节点若仍认识旧块，可以继续返回旧分支状态；若旧块或状态已不存在，调用报错；
- 因此不能指望 exact 调用在重组后必然自动失败。旧分支数据最终靠流中的 revert 和 undo 撤销。

另外，当前调用信任上游 `BlockEvent` 中 `(block_height, block_hash)` 的配对，没有再独立下载该哈希的区块头来核对高度。正常连续性由上游分叉检测和下游 parent hash 检查保证。

**代价**：若索引器落后很多块，普通全节点通常不保留那么旧的状态，`exact` 需要 archive 节点。TRON 节点只支持 latest，配置 exact 会在启动时被拒绝。

### 7.4 `latest`：不是“恰好当前块”，而是“至少当前块高”

默认配置：

```yaml
state_reads: latest
```

每个请求构造成：

```go
evmsdk.RPCCallReq{
    Address:  tokenOrMulticall,
    Calldata: calldata,
    Read: evmsdk.StateRead{
        Requirement:   evmsdk.ReadAtLeast,
        QueryAt:       evmsdk.LatestBlockNumber,
        MinimumHeight: block.Height,
    },
}
```

参数含义：

- `QueryAt = latest`：希望读节点当前链头；
- `MinimumHeight = 当前应用块高 N`：实际执行高度必须 `H >= N`；
- `ReadAtLeast`：允许 H 大于 N，不要求 `H == N`。

普通 EVM 节点上的过程是：

1. OMB 用 `eth_getBlockByNumber("latest", ...)` 把 latest 解析成具体高度 H；
2. 若 `H < N`，返回 `ErrStateReadUnstable`，不拿旧状态冒充当前块状态；
3. 在 H 上执行直接或聚合后的 `eth_call`；
4. 从具体请求高度或 multicall 返回的 `blockNumber` 取得执行证据；
5. 再次校验 `H >= N`，把 H 交给 `holder-indexer` 写入 `as_of_height`。

不同批次可能把 latest 解析成不同的 H；每个结果都携带自己的执行高度，业务层不会假设所有持仓共享同一个全局快照。

TRON 只能把 `eth_call` 的区块参数写成 `"latest"`。OMB 通过 multicall 报告的块高确认执行位置；无法从合约得到块高时，会比较执行前后的 latest 高度，只有二者不变才接受，否则报不稳定读取并重试。这里证明的仍是高度，不是哈希。

**代价与收益**：不需要 archive 状态，落后很多块也能追数据；但追块期间的余额和人数可能“超前”于当前应用高度，而且每个持仓的高水位可以不同。

### 7.5 latest 是否验证指定块哈希

**没有。**

虽然 OMB 的通用 `StateRead` 已支持 `ExpectedHash`，multicall 也有 `BlockInfoHash` 模式，但 `chainstream-market` 当前这条 latest 路径：

- 没有设置 `ExpectedHash`；
- 默认 multicall 只要求返回块高（`BlockInfoNumber`）；
- `internal/pkg/rpc` 只读取 `Execution.BlockNumber`，不保存或校验 `Execution.BlockHash`；
- `holder_balances` 也没有 `as_of_hash` 字段。

因此下面两件事都可能发生：

1. latest 在执行前后发生同高度重组，余额来自另一条分支，但高度仍满足 `H >= N`；
2. 即使读取当下完全正确，H 是未 finalized 块，之后仍可能正常发生重组。

**块高合法不代表分支合法，分支合法也不代表永远 finalized。** 这是三个不同层次的问题。

### 7.6 是否可能获取到“之后会被回滚的余额”

**会，两种模式都会。**

| 模式 | 读取绑定 | 是否可能读到未 finalized 状态 | 当前主要修复方式 |
| --- | --- | --- | --- |
| `exact` | 当前应用块的哈希 | 会；当前应用块本身就可能尚未 finalized | revert + undo 精确撤销 |
| `latest` | 高度至少为 N 的当前链头，未绑定哈希 | 会，而且可能比当前应用块更靠前 | revert + undo + `as_of_height = -1` + 重读 / 对账 |

当前系统的定位是“低延迟跟随链头，重组后纠正”，不是“只展示永不变更的余额”。这与 `chainstream-evm` 不等待 finalized 就发布事实的整体取舍一致。

现有缓解手段包括：

- exact 用块哈希防止同高度误读另一条分支；
- latest 用实际执行高水位避免重复累加；
- 所有变更进入 undo；
- 对可能来自旧分支的 latest 余额标 `-1`；
- 用周期对账修正残余漂移。

但第 6.6 节说明的两个边界仍然存在，所以不能把它描述成“任何时刻、任何重组形态下，API 都立即返回新规范链余额”。

### 7.7 如果业务完全不能接受会回滚的余额，怎么解决

需要根据业务要求增加更强的读取或展示层，不能只把 `latest` 换成 `exact`：

1. **只读 finalized 块**：先取得 finalized 高度及对应块哈希，再按该哈希做 EIP-1898 `balanceOf` / `getEthBalance`。读取的是已最终确定的状态，而不是当前 latest。离 finalized 很远时仍需要能服务该状态的节点。
2. **建立 finalized 余额视图**：持续消费流，但只对外暴露已经 finalized 的版本；链头视图和 finalized 视图分开，避免用一份可提前量的当前表同时承担两种语义。
3. **加强 latest 的执行证据**：在 OMB 层解析 latest 时保留其哈希，配合 `ExpectedHash` 或 multicall 的块哈希证据检查实际执行分支，并在余额行保存 `as_of_hash`。这能发现“解析 latest 与执行之间换分支”，但仍不能把未 finalized 块变成 finalized。
4. **补齐失效传播和 API 语义**：对每条 revert 都失效高于 R 的读取高水位；API 至少返回 `as_of_height`、`verified_height`、`stale`，或者过滤 `as_of_height = -1` 的行。这样调用方不必把待重读值误认为可信现值。

以上是为了严格一致性可做的改造方向，**不是当前已上线的完整能力**。

### 7.8 对外 API 怎样读取余额

当前接口：

```http
GET /v1/chains/{chain}/tokens/{token}/holders?limit=...
```

它执行的核心查询等价于：

```sql
SELECT token, holder, balance::text AS balance
FROM holder_balances
WHERE chain_id = ? AND token = ? AND balance > 0
ORDER BY balance DESC, holder
LIMIT ?;
```

人数直接读取 `holder_counts`。响应包含：

- `balance_raw`：原始单位整数字符串；
- `decimals` 与换算后的 `balance`：ERC-20 精度来自 `market-registry`，原生币固定按 18 位处理；
- `holder_count`：正余额持仓数；
- 可选的钱包 `labels`。

当前没有在这个接口上提供“指定块高 / 指定块哈希查询历史余额”的参数，也不返回每行的实际读取高度、读取哈希或失效状态。它展示的是数据库当前值，不是一次新的链上调用，也不是全局同一区块快照。

## 8. 把当前保证与不能保证的事情分开

| 问题 | 当前答案 |
| --- | --- |
| 没见过某地址时，是否直接当余额为 0？ | 否。第一次在非零转账中出现时，通常用链上全量余额 seed；原生币新地址仅在正值时保留。 |
| 是否每块重读所有 holder？ | 否。只读新持仓、失效后再次触碰的持仓、native_touched 和周期对账批次。 |
| 已知 ERC-20 是否主要靠 RPC 更新？ | 否。主要靠本块净转账增量，RPC 用于 seed、失效重读和对账。 |
| latest 读到未来块后是否会重复加转账？ | 正常路径不会，`as_of_height` 会挡住不高于读取高度的转账。 |
| 对账修正是否可回滚？ | 是，修正同样是带 undo 的 delta。 |
| 回滚是否与余额人数、检查点原子提交？ | 是，在同一个 PostgreSQL 事务内。 |
| 是否支持任意深度回滚？ | 否。低于已裁剪历史会停止；history_depth 至少要覆盖上游 scanner.cache_size。 |
| exact 是否验证块哈希？ | 是，调用直接按 EIP-1898 块哈希执行；但它不是 canonical/finality 证明。 |
| latest 是否验证块哈希？ | 否，只证明实际执行高度满足下界。 |
| 是否可能返回之后会重组的余额？ | 会。当前采用低延迟 + 事后撤销 / 重读 / 对账。 |
| revert 后 API 是否立即隐藏待重读余额？ | 否，API 不过滤也不暴露 `as_of_height = -1`。 |
| 是否已有严格的 finalized-only 持有人余额 API？ | 没有。需要额外视图或按 finalized 块哈希查询。 |

一句话概括当前设计：

> 用链上读取解决“起点不为零”，用净转账解决“不能每块读所有人”，用对账解决“事件解释不了全部变化”，用 undo 解决“分支可以撤销”，再用 `as_of_height` 把 latest 的未来读取和增量账本接起来；代价是它是分持仓、分高度的最终一致视图，而不是一个始终 finalized 的全局快照。

## 9. 关键配置与代码索引

### 9.1 配置检查清单

```yaml
holders:
  # 至少覆盖 chainstream-evm 的 scanner.cache_size
  history_depth: 256
  # 负数关闭；默认每 100 块一次
  reconcile_every: 100
  reconcile_batch: 200
  rpc_batch_size: 100
  rpc_timeout: 30s
  native_balances: true

chains:
  - name: eth
    chain_id: 1
    topic: chainstream-eth
    rpc_url: http://127.0.0.1:8545
    # latest：普通全节点可追块，但可能超前且未绑定哈希
    # exact：按块哈希，深历史需要 archive 节点
    state_reads: latest
```

### 9.2 代码位置

| 关注点 | 仓库与位置 |
| --- | --- |
| 四张表完整 DDL | `chainstream-market/internal/pkg/pgdb/schema.go:39-91` |
| 每块 key 选择、seed、增量、重读、对账 | `chainstream-market/internal/app/holder/biz/indexer.go:147-316` |
| 净转账、零地址、地址规范化 | `chainstream-market/internal/app/holder/biz/indexer.go:336-388` |
| 回滚深度检查 | `chainstream-market/internal/app/holder/biz/indexer.go:318-334` |
| 余额、人数、undo、检查点的事务提交与回滚 SQL | `chainstream-market/internal/app/holder/data/holder.go:73-177` |
| `as_of_height` 的模型语义 | `chainstream-market/internal/pkg/model/holder.go:14-32` |
| RPC 请求参数、EIP-1898 / ReadAtLeast、返回值解码 | `chainstream-market/internal/pkg/rpc/client.go:104-188` |
| 节点错误与合约执行失败的分类 | `chainstream-market/internal/pkg/rpc/client.go:223-261` |
| 下游重复块、parent hash、revert 处理 | `chainstream-market/internal/pkg/chainsync/engine.go:123-213` |
| undo 裁剪公式 | `chainstream-market/internal/pkg/chainsync/prune.go:1-19` |
| API 的 Top Holders 和人数查询 | `chainstream-market/internal/app/api/data/holders.go:22-42` |
| API 响应与精度换算 | `chainstream-market/internal/app/api/service/holders.go:38-90` |
| 上游块、revert、native_touched 的线协议 | `chainstream-evm/api/stream/v1/event.proto:8-79` |
| 上游 native_touched 的构造 | `chainstream-evm/internal/biz/stream/native.go:14-76` |
| 上游回滚发布和游标提交顺序 | `chainstream-evm/internal/biz/chain/consumer.go:60-143` |
| OMB `StateRead`、`ExpectedHash` 定义 | `oh-my-blockchain/chain/evm/sdk/state_read.go:11-76` |
| OMB latest 解析、执行高度校验、ExpectedHash 校验 | `oh-my-blockchain/chain/evm/sdk/impl/state_read.go:198-383` |
| OMB TRON latest 执行高度确认 | `oh-my-blockchain/chain/evm/sdk/impl/state_read_latest.go:72-122` |
| OMB EIP-1898 `requireCanonical=false` 的实际调用参数 | `oh-my-blockchain/chain/evm/sdk/impl/basic_block_hash.go:12-18` |
| OMB multicall 默认块信息级别 | `oh-my-blockchain/chain/evm/sdk/multicall/option.go:3-36` |
| latest 去重、回滚重读、对账高水位的现有测试 | `chainstream-market/internal/app/holder/data/indexer_test.go:337-401` |
