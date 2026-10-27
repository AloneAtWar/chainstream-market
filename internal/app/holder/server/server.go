// Package server runs holder-indexer's indexer on every chain, each as a
// Kratos server, beside the metrics endpoint.
package server

import (
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/holder/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/holder/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/transport"
	kratostransport "github.com/go-kratos/kratos/v2/transport"
	"github.com/google/wire"
)

// ProviderSet is the server provider set.
var ProviderSet = wire.NewSet(NewServers)

// NewServers serves metrics and runs the indexer on each chain's topic,
// reading balanceOf over the chain's rpc_url.
func NewServers(
	chains []*config.Chain,
	holders *conf.Holders,
	metricsConfig *config.Metrics,
	sources *source.Sources,
	repo biz.HolderRepo,
	logger *slog.Logger,
) ([]kratostransport.Server, error) {
	servers := metrics.Servers(metricsConfig, logger)
	for _, chain := range chains {
		client, err := rpc.New(rpc.Options{
			URL: chain.GetRpcUrl(), ChainID: chain.GetChainId(), Chain: chain.GetName(),
			BatchSize: int(holders.GetRpcBatchSize()), Timeout: holders.GetRpcTimeout().AsDuration(),
			Logger: logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		native := holders.GetNativeBalances()
		if native && !client.NativeBalances() {
			logger.Warn("chain has no multicall contract, native balances are off", "chain", chain.GetName())
			native = false
		}
		indexer, err := biz.New(biz.Options{
			ChainID: chain.GetChainId(), Repo: repo, Balances: client,
			HistoryDepth: holders.GetHistoryDepth(), ReconcileEvery: max(holders.GetReconcileEvery(), 0),
			ReconcileBatch: int(holders.GetReconcileBatch()), NativeBalances: native,
			Logger: logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		engine, err := chainsync.New(chainsync.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Handler: indexer, Logger: logger,
			Open: sources.Opener(chain.GetTopic()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		servers = append(servers, transport.NewRunner("chain "+chain.GetName(), engine.Run))
	}
	return servers, nil
}
