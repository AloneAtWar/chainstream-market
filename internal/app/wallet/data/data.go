// Package data 是 market-wallet 的数据层：要跟读的 live topic、要补缺的 market-engine
// trades 表（ClickHouse），以及账本表（PostgreSQL 的持仓、undo、检查点、标签，ClickHouse
// 的流水、日汇总和统计）。
package data

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/wallet/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/wallet/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/google/wire"
	"gorm.io/gorm"
)

// ProviderSet 是数据层的依赖注入集合。
var ProviderSet = wire.NewSet(NewData, NewWalletRepo, NewTradeSource, NewStatsRepo, NewLabelRepo, NewOpeners)

// Data 持有 market-wallet 的两个数据库。
type Data struct {
	// market 是 ClickHouse：market-engine 的 trades 和 market-wallet 的流水、汇总表。
	market *gorm.DB
	// state 是 PostgreSQL：持仓、undo、检查点和标签。
	state *gorm.DB
}

// NewData 连接两个数据库，建好需要的表，并给 wallet_trades 设置保留期。
func NewData(ctx context.Context, clickhouse *config.ClickHouse, postgres *config.Postgres, wallet *conf.Wallet, logger *slog.Logger) (*Data, func(), error) {
	market, closeMarket, err := chdb.OpenConfig(ctx, clickhouse, logger)
	if err != nil {
		return nil, nil, err
	}
	for _, schema := range [][]string{chdb.MarketSchema, chdb.WalletSchema} {
		if err = chdb.Migrate(ctx, market, schema); err != nil {
			closeMarket()
			return nil, nil, fmt.Errorf("clickhouse: migrate: %w", err)
		}
	}
	if err = chdb.SetWalletTradesTTL(ctx, market, int(wallet.GetTradesDays())); err != nil {
		closeMarket()
		return nil, nil, err
	}
	state, closeState, err := pgdb.OpenConfig(ctx, postgres, logger)
	if err != nil {
		closeMarket()
		return nil, nil, err
	}
	return &Data{market: market, state: state}, func() {
		closeState()
		closeMarket()
	}, nil
}

// Openers 按链 ID 给出各链 live topic 的打开函数。
type Openers map[uint64]biz.Opener

// NewOpeners 为每条链打开 kafka 上的 live topic：有检查点时从下一条 offset 继续，首次
// 启动从末尾开始。
func NewOpeners(kafka *config.Kafka, chains []*config.Chain) Openers {
	openers := make(Openers, len(chains))
	for _, chain := range chains {
		options := source.KafkaOptions{
			Brokers: kafka.GetBrokers(), ClientID: kafka.GetClientId(), Topic: chain.GetLiveTopic(),
			FetchMaxBytes: kafka.GetFetchMaxBytes(), FetchMaxWait: kafka.GetFetchMaxWait().AsDuration(),
		}
		openers[chain.GetChainId()] = func(start int64) (source.Source, error) {
			if start < 0 {
				return source.NewKafkaAtEnd(options)
			}
			return source.NewKafka(options, start)
		}
	}
	return openers
}
