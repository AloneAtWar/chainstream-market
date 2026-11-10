// Package server 在每条链上把 market-wallet 的账本跟读和统计汇总各作为一个 Kratos
// server 运行，旁边是 metrics 端点。
package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/wallet/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/wallet/conf"
	"github.com/AloneAtWar/chainstream-market/internal/app/wallet/data"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/transport"
	kratostransport "github.com/go-kratos/kratos/v2/transport"
	"github.com/google/wire"
)

// ProviderSet 是 server 层的依赖注入集合。
var ProviderSet = wire.NewSet(NewServers)

// NewServers 提供 metrics，并为每条链运行一个 Follower（跟读 live topic）和一个
// Refresher（定时汇总统计、重算标签）。
func NewServers(
	chains []*config.Chain,
	wallet *conf.Wallet,
	metricsConfig *config.Metrics,
	openers data.Openers,
	repo biz.WalletRepo,
	trades biz.TradeSource,
	stats biz.StatsRepo,
	labels biz.LabelRepo,
	logger *slog.Logger,
) ([]kratostransport.Server, error) {
	servers := metrics.Servers(metricsConfig, logger)
	statsConfig := wallet.GetStats()
	windows := make([]int, len(statsConfig.GetWindows()))
	for index, window := range statsConfig.GetWindows() {
		windows[index] = int(window)
	}
	smart, bot := statsConfig.GetSmartMoney(), statsConfig.GetBot()
	for _, chain := range chains {
		chainLogger := logger.With("chain", chain.GetName())
		follower, err := biz.New(biz.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Open: openers[chain.GetChainId()],
			Repo: repo, Trades: trades,
			HistoryDepth: wallet.GetHistoryDepth(), History: wallet.GetHistory().AsDuration(),
			BackfillHeights: wallet.GetBackfillHeights(), BatchLag: wallet.GetBatchLag().AsDuration(),
			FlushRows: int(wallet.GetFlushRows()), FlushInterval: wallet.GetFlushInterval().AsDuration(),
			CloseRatio: wallet.GetCloseRatio(), Logger: chainLogger,
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		refresher, err := biz.NewRefresher(biz.StatsOptions{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Repo: stats, Labels: labels,
			Windows: windows, FastCloseSeconds: statsConfig.GetFastCloseSeconds(),
			SmartMoney: biz.SmartMoneyRule{
				WindowDays: int(smart.GetWindowDays()), MinRealizedUSD: smart.GetMinRealizedUsd(),
				MinWinRate: smart.GetMinWinRate(), MinTokens: smart.GetMinTokens(),
				MaxLossShare: smart.GetMaxLossShare(), MaxWallets: int(smart.GetMaxWallets()),
			},
			Bot: biz.BotRule{
				WindowDays: int(bot.GetWindowDays()), MinTrades: bot.GetMinTrades(),
				MinCloses: bot.GetMinCloses(), MinFastCloseShare: bot.GetMinFastCloseShare(),
			},
			Logger: chainLogger,
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		period := statsConfig.GetRefreshInterval().AsDuration()
		servers = append(servers,
			transport.NewRunner("ledger "+chain.GetName(), follower.Run),
			transport.NewRunner("stats "+chain.GetName(), func(ctx context.Context) error {
				refresher.Run(ctx, period)
				return nil
			}),
		)
	}
	return servers, nil
}
