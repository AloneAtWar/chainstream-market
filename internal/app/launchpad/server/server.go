// Package server runs market-launchpad's handler on every chain, each as a
// Kratos server, beside the metrics endpoint.
//
// 为每条链启动一个 market-launchpad handler（各自作为 Kratos server），并提供 metrics 端点。
package server

import (
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/launchpad/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/launchpad/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/transport"
	kratostransport "github.com/go-kratos/kratos/v2/transport"
	"github.com/google/wire"
)

// ProviderSet is the server provider set.
//
// ProviderSet 是 server 层的 provider 集合。
var ProviderSet = wire.NewSet(NewServers)

// NewServers serves metrics and follows launchpads on each chain's topic.
//
// NewServers 提供 metrics 端点，并为每条链启动一个 launchpad handler。
func NewServers(
	chains []*config.Chain,
	launchpad *conf.Launchpad,
	metricsConfig *config.Metrics,
	sources *source.Sources,
	repo biz.LaunchpadRepo,
	logger *slog.Logger,
) ([]kratostransport.Server, error) {
	servers := metrics.Servers(metricsConfig, logger)
	for _, chain := range chains {
		handler, err := biz.New(biz.Options{
			ChainID: chain.GetChainId(), Repo: repo, Protocols: launchpad.GetProtocols(),
			HistoryDepth: launchpad.GetHistoryDepth(), Logger: logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		engine, err := chainsync.New(chainsync.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Handler: handler, Logger: logger,
			Open: sources.Opener(chain.GetTopic()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		servers = append(servers, transport.NewRunner("chain "+chain.GetName(), engine.Run))
	}
	return servers, nil
}
