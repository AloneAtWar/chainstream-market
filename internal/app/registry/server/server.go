// Package server runs market-registry's handler on every chain, each as a
// Kratos server, beside the metrics endpoint.
package server

import (
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/registry/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/registry/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/transport"
	kratostransport "github.com/go-kratos/kratos/v2/transport"
	"github.com/google/wire"
)

// ProviderSet is the server provider set.
var ProviderSet = wire.NewSet(NewServers)

// NewServers serves metrics and runs the registry on each chain's topic.
func NewServers(
	chains []*config.Chain,
	registry *conf.Registry,
	metricsConfig *config.Metrics,
	sources *source.Sources,
	repo biz.RegistryRepo,
	logger *slog.Logger,
) ([]kratostransport.Server, error) {
	servers := metrics.Servers(metricsConfig, logger)
	for _, chain := range chains {
		handler, err := biz.New(biz.Options{
			ChainID: chain.GetChainId(), Repo: repo, HistoryDepth: registry.GetHistoryDepth(),
			Logger: logger.With("chain", chain.GetName()),
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
