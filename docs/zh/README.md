# chainstream-market 中文文档

`chainstream-market` 是建立在 [chainstream-evm](https://github.com/AloneAtWar/chainstream-evm) 区块流之上的一组
市场数据服务：给交易定方向（base/quote）、USD 定价、K 线、持有人余额、原始转账、代币排行榜，以及一个 Web 前端。
目前覆盖 ETH、BSC 和 Base。

chainstream-evm 只发布链上"事实"（每块的交易、转账、资产、池子、预言机日志……），本仓库负责所有"派生"计算。

## 文档目录

| 文档 | 内容 |
| --- | --- |
| [architecture.md](architecture.md) | 整体数据流、六个组件的职责、目录结构与分层约定、共享的顺序消费循环（chainsync） |
| [market-engine.md](market-engine.md) | 定价与 K 线：池子状态、价格图、交易定方向、集中流动性 tick 表、Chainlink、异常交易、迟到的代币小数位与回填、存储与状态 |
| [services.md](services.md) | market-api 接口、实时推送协议、holder-indexer、market-registry、market-rankings、ch-loader |
| [operations.md](operations.md) | 本地完整环境、配置、开发命令、指标、Web 前端 |

英文版完整说明见仓库根目录 [README.md](../../README.md)；两者不一致时以英文 README 和代码为准。

## 一张图看懂

```text
                      chainstream-evm（每条链一个进程）
                                │  stream.v1.StreamRecord（BlockEvent / BlockRevert）
                                ▼
                 Kafka topic（单分区）或 protojson 行文件
        ┌──────────────┬───────────────┼────────────────┬──────────────────┐
        ▼              ▼               ▼                ▼                  │
  market-engine   holder-indexer   market-registry    ch-loader            │
  定方向/定价/K线  持有人余额        代币与池子元数据    原始事实入库          │
        │              │               │                │                  │
        ▼              ▼               ▼                ▼                  │
   ClickHouse      PostgreSQL       PostgreSQL       ClickHouse            │
  trades/prices/   holder_*        registry_*        raw_*                 │
  candles                                                                  │
        │  live topic（Kafka / Redis）        market-rankings（定期汇总 → token_rankings）
        ▼                                                │
     market-api（REST + WebSocket /v1/stream） ◄─────────┘
        │
        ▼
     web 前端（React + Vite）
```

每个消费组件都**各自**按 offset 顺序读同一条链 topic、各自维护检查点，互不依赖；任何一个落后或重启都不影响其它组件。

## 快速开始

```sh
cp deploy/.env.example deploy/.env   # 设置 ETH_RPC_URL
make up                              # 用 docker compose 启动全部服务
open http://localhost:8088           # Web 前端
```

需要 Docker Compose v2、一个 ETH 节点，以及与本仓库并列的 chainstream-evm 代码目录。详见 [operations.md](operations.md)。
