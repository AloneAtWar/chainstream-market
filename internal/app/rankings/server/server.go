// Package server runs market-rankings' refresher on every chain, each as a
// Kratos server, beside the metrics endpoint.
package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/rankings/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/rankings/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/transport"
	kratostransport "github.com/go-kratos/kratos/v2/transport"
	"github.com/google/wire"
)

// ProviderSet is the server provider set.
var ProviderSet = wire.NewSet(NewServers)

// NewServers serves metrics and refreshes each chain's rankings every
// rankings.refresh_interval.
func NewServers(
	chains []*config.Chain,
	rankings *conf.Rankings,
	metricsConfig *config.Metrics,
	source biz.RankingSource,
	repo biz.RankingRepo,
	logger *slog.Logger,
) ([]kratostransport.Server, error) {
	period := rankings.GetRefreshInterval().AsDuration()
	logger.Info("refreshing rankings", "every", period, "candle_interval", rankings.GetCandleInterval().AsDuration())
	servers := metrics.Servers(metricsConfig, logger)
	for _, chain := range chains {
		refresher, err := biz.New(biz.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Source: source, Repo: repo,
			Interval: rankings.GetCandleInterval().AsDuration(), ReorgWindow: rankings.GetReorgWindow().AsDuration(),
			Logger: logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		servers = append(servers, transport.NewRunner("rankings "+chain.GetName(), func(ctx context.Context) error {
			refresher.Run(ctx, period)
			return nil
		}))
	}
	return servers, nil
}
