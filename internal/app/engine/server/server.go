// Package server runs market-engine's processor on every chain, each as a
// Kratos server, beside the metrics endpoint.
package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/engine/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/transport"
	kratostransport "github.com/go-kratos/kratos/v2/transport"
	"github.com/google/wire"
	"github.com/shopspring/decimal"
)

// ProviderSet is the server provider set.
var ProviderSet = wire.NewSet(NewServers)

// NewServers serves metrics and runs a processor on each chain's topic. Its
// feeds are resolved over the chain's rpc_url first. Without a state store,
// checkpoints are kept in memory.
func NewServers(
	ctx context.Context,
	chains []*config.Chain,
	market *conf.Market,
	metricsConfig *config.Metrics,
	sources *source.Sources,
	output biz.Output,
	state biz.StateStore,
	history biz.History,
	notifier biz.Notifier,
	logger *slog.Logger,
) ([]kratostransport.Server, error) {
	servers := metrics.Servers(metricsConfig, logger)
	checkpoints := biz.NewMemory()
	for _, chain := range chains {
		feeds, err := ResolveFeeds(ctx, chain, logger)
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		processor, err := biz.NewProcessor(biz.Options{
			Catalog:               asset.DefaultCatalog(chain.GetChainId()),
			MinLiquidityUSD:       decimal.NewFromFloat(market.GetMinLiquidityUsd()),
			CandleMinLiquidityUSD: decimal.NewFromFloat(market.GetCandleMinLiquidityUsd()),
			MaxHops:               uint8(market.GetMaxHops()),
			Intervals:             market.IntervalDurations(),
			HistoryDepth:          market.GetHistoryDepth(),
			Feeds:                 feeds,
			OracleDeviation:       decimal.NewFromFloat(market.GetOracleDeviation()),
			OutlierDeviation:      decimal.NewFromFloat(max(market.GetOutlierDeviation(), 0)),
			LateTradeBlocks:       max(market.GetLateTradeBlocks(), 0),
			LateTradesPerToken:    int(market.GetLateTradesPerToken()),
			History:               history,
			BackfillBlocks:        max(market.GetBackfillBlocks(), 0),
			DepthBands:            market.DepthBandList(),
			DynamicFeeDepthBPS:    int(market.GetDynamicFeeDepthBps()),
			Logger:                logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		handler, err := biz.NewHandler(biz.HandlerOptions{
			ChainID: chain.GetChainId(), Processor: processor, Output: output, Checkpoints: checkpoints, State: state,
			Notifier: notifier, Logger: logger.With("chain", chain.GetName(), "chain_id", chain.GetChainId()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		engine, err := chainsync.New(chainsync.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Open: sources.Opener(chain.GetTopic()),
			Handler: handler, Logger: logger,
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		servers = append(servers, transport.NewRunner("chain "+chain.GetName(), engine.Run))
	}
	return servers, nil
}
