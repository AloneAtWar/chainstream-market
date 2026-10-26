// Package server runs ch-loader's loader on every chain, each as a Kratos
// server, beside the metrics endpoint.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/loader/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/loader/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/transport"
	kratostransport "github.com/go-kratos/kratos/v2/transport"
	"github.com/google/wire"
)

// flushTimeout bounds the write of a stopping chain's buffered batch.
const flushTimeout = 30 * time.Second

// ProviderSet is the server provider set.
var ProviderSet = wire.NewSet(NewServers)

// NewServers serves metrics and runs the loader on each chain's topic. A
// chain that stops cleanly writes its buffered batch, so a restart replays
// less.
func NewServers(
	chains []*config.Chain,
	loaderConfig *conf.Loader,
	metricsConfig *config.Metrics,
	sources *source.Sources,
	repo biz.RawRepo,
	logger *slog.Logger,
) ([]kratostransport.Server, error) {
	servers := metrics.Servers(metricsConfig, logger)
	for _, chain := range chains {
		loader, err := biz.New(biz.Options{
			ChainID: chain.GetChainId(), Repo: repo, BatchLag: loaderConfig.GetBatchLag().AsDuration(),
			FlushRows: int(loaderConfig.GetFlushRows()), FlushInterval: loaderConfig.GetFlushInterval().AsDuration(),
			Logger: logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		engine, err := chainsync.New(chainsync.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Handler: loader, Logger: logger,
			Open: sources.Opener(chain.GetTopic()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		name := chain.GetName()
		servers = append(servers, transport.NewRunner("chain "+name, func(ctx context.Context) error {
			if err := engine.Run(ctx); err != nil {
				return err
			}
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
			defer cancel()
			if err := loader.Flush(flushCtx); err != nil {
				logger.Error("flush on stop", "chain", name, "error", err)
			}
			return nil
		}))
	}
	return servers, nil
}
