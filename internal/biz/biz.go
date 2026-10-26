// Package biz assembles each command's per-chain work: the engines that
// apply chain topics through a market, holder, registry or ch-loader
// handler, and the rankings refreshers. Each command wires one of the sets
// below; they all produce Runners.
package biz

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/chloader"
	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/data"
	"github.com/AloneAtWar/chainstream-market/internal/holders"
	"github.com/AloneAtWar/chainstream-market/internal/market"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainlink"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc"
	"github.com/AloneAtWar/chainstream-market/internal/rankings"
	"github.com/AloneAtWar/chainstream-market/internal/registry"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/google/wire"
	"github.com/shopspring/decimal"
)

// The provider sets of the commands that consume chain topics.
var (
	MarketSet   = wire.NewSet(NewMarketEngines)
	HolderSet   = wire.NewSet(NewHolderEngines)
	RegistrySet = wire.NewSet(NewRegistryEngines)
	LoaderSet   = wire.NewSet(NewLoaderEngines)
	RankingSet  = wire.NewSet(NewRefreshers)
)

// Runner is one unit of a command's work, such as one chain's engine. Run
// returns when ctx ends, or with the error that stopped it.
type Runner struct {
	Name string
	Run  func(ctx context.Context) error
}

// Runners are a command's units of work. The first to fail stops them all,
// so the process restarts from consistent checkpoints.
type Runners []Runner

func engineRunner(chain *conf.Chain, chainEngine *chainsync.Engine) Runner {
	return Runner{Name: "chain " + chain.GetName(), Run: chainEngine.Run}
}

// NewMarketEngines builds market-engine's processor and engine per chain.
func NewMarketEngines(
	ctx context.Context,
	chains []*conf.Chain,
	marketConfig *conf.Market,
	sources *data.Sources,
	output storage.Output,
	memory *storage.Memory,
	state storage.StateStore,
	notifier market.Notifier,
	history storage.History,
	logger *slog.Logger,
) (Runners, error) {
	runners := make(Runners, 0, len(chains))
	for _, chain := range chains {
		feeds, err := ResolveFeeds(ctx, chain, logger)
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		processor, err := market.NewProcessor(market.Options{
			Catalog:               asset.DefaultCatalog(chain.GetChainId()),
			MinLiquidityUSD:       decimal.NewFromFloat(marketConfig.GetMinLiquidityUsd()),
			CandleMinLiquidityUSD: decimal.NewFromFloat(marketConfig.GetCandleMinLiquidityUsd()),
			MaxHops:               uint8(marketConfig.GetMaxHops()),
			Intervals:             marketConfig.IntervalDurations(),
			HistoryDepth:          marketConfig.GetHistoryDepth(),
			Feeds:                 feeds,
			OracleDeviation:       decimal.NewFromFloat(marketConfig.GetOracleDeviation()),
			OutlierDeviation:      decimal.NewFromFloat(max(marketConfig.GetOutlierDeviation(), 0)),
			LateTradeBlocks:       max(marketConfig.GetLateTradeBlocks(), 0),
			LateTradesPerToken:    int(marketConfig.GetLateTradesPerToken()),
			History:               history,
			BackfillBlocks:        max(marketConfig.GetBackfillBlocks(), 0),
			Logger:                logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		handler, err := market.NewHandler(market.HandlerOptions{
			ChainID: chain.GetChainId(), Processor: processor, Output: output, Checkpoints: memory, State: state,
			Notifier: notifier, Logger: logger.With("chain", chain.GetName(), "chain_id", chain.GetChainId()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		chainEngine, err := chainsync.New(chainsync.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Open: sources.Opener(chain.GetTopic()),
			Handler: handler, Logger: logger,
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		runners = append(runners, engineRunner(chain, chainEngine))
	}
	return runners, nil
}

// ResolveFeeds turns a chain's configured feeds into trusted aggregators.
// A proxy is read over the chain's rpc_url for the aggregator of every phase,
// and its decimals and description are checked against the config.
func ResolveFeeds(ctx context.Context, chain *conf.Chain, logger *slog.Logger) ([]market.Feed, error) {
	var client *rpc.Client
	proxies := make(map[string]chainlink.Proxy)
	var feeds []market.Feed
	for _, feed := range chain.GetFeeds() {
		decimals := uint8(feed.GetDecimals())
		if feed.GetProxy() == "" {
			feeds = append(feeds, market.Feed{Address: feed.GetAddress(), Token: feed.GetToken(), Decimals: decimals})
			continue
		}
		address := strings.ToLower(strings.TrimSpace(feed.GetProxy()))
		proxy, ok := proxies[address]
		if !ok {
			var err error
			if client == nil {
				if client, err = rpc.New(rpc.Options{
					URL: chain.GetRpcUrl(), ChainID: chain.GetChainId(), Chain: chain.GetName(), Logger: logger,
				}); err != nil {
					return nil, err
				}
			}
			if proxy, err = chainlink.Resolve(ctx, client, address); err != nil {
				return nil, err
			}
			proxies[address] = proxy
			logger.Info("feed proxy resolved", "chain", chain.GetName(), "proxy", address,
				"description", proxy.Description, "decimals", proxy.Decimals, "aggregators", proxy.Aggregators)
		}
		if decimals != 0 && decimals != proxy.Decimals {
			return nil, fmt.Errorf("feed proxy %s has %d decimals, config says %d", address, proxy.Decimals, decimals)
		}
		if want := strings.TrimSpace(feed.GetDescription()); want != "" && !strings.EqualFold(want, strings.TrimSpace(proxy.Description)) {
			return nil, fmt.Errorf("feed proxy %s is %q, config says %q", address, proxy.Description, want)
		}
		for _, aggregator := range proxy.Aggregators {
			feeds = append(feeds, market.Feed{Address: aggregator, Token: feed.GetToken(), Decimals: proxy.Decimals})
		}
	}
	return feeds, nil
}

// NewHolderEngines builds holder-indexer's balanceOf client, indexer and
// engine per chain.
func NewHolderEngines(chains []*conf.Chain, holderConfig *conf.Holders, sources *data.Sources, store storage.HolderStore, logger *slog.Logger) (Runners, error) {
	runners := make(Runners, 0, len(chains))
	for _, chain := range chains {
		if strings.TrimSpace(chain.GetRpcUrl()) == "" {
			return nil, fmt.Errorf("chain %s: rpc_url is required", chain.GetName())
		}
		client, err := rpc.New(rpc.Options{
			URL: chain.GetRpcUrl(), ChainID: chain.GetChainId(), Chain: chain.GetName(),
			BatchSize: int(holderConfig.GetRpcBatchSize()), Timeout: holderConfig.GetRpcTimeout().AsDuration(),
			Logger: logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		indexer, err := holders.New(holders.Options{
			ChainID: chain.GetChainId(), Store: store, Balances: client,
			HistoryDepth: holderConfig.GetHistoryDepth(), ReconcileEvery: max(holderConfig.GetReconcileEvery(), 0),
			ReconcileBatch: int(holderConfig.GetReconcileBatch()),
			Logger:         logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		chainEngine, err := chainsync.New(chainsync.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Handler: indexer, Logger: logger,
			Open: sources.Opener(chain.GetTopic()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		runners = append(runners, engineRunner(chain, chainEngine))
	}
	return runners, nil
}

// NewRegistryEngines builds market-registry's handler and engine per chain.
func NewRegistryEngines(chains []*conf.Chain, registryConfig *conf.Registry, sources *data.Sources, store storage.RegistryStore, logger *slog.Logger) (Runners, error) {
	runners := make(Runners, 0, len(chains))
	for _, chain := range chains {
		handler, err := registry.New(registry.Options{
			ChainID: chain.GetChainId(), Store: store, HistoryDepth: registryConfig.GetHistoryDepth(),
			Logger: logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		chainEngine, err := chainsync.New(chainsync.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Handler: handler, Logger: logger,
			Open: sources.Opener(chain.GetTopic()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		runners = append(runners, engineRunner(chain, chainEngine))
	}
	return runners, nil
}

// NewLoaderEngines builds ch-loader's loader and engine per chain. A chain that stops cleanly writes its buffered batch, so
// a restart replays less.
func NewLoaderEngines(
	chains []*conf.Chain,
	loaderConfig *conf.ChLoader,
	sources *data.Sources,
	store storage.RawStore,
	logger *slog.Logger,
) (Runners, error) {
	runners := make(Runners, 0, len(chains))
	for _, chain := range chains {
		loader, err := chloader.New(chloader.Options{
			ChainID: chain.GetChainId(), Store: store, BatchLag: loaderConfig.GetBatchLag().AsDuration(),
			FlushRows: int(loaderConfig.GetFlushRows()), FlushInterval: loaderConfig.GetFlushInterval().AsDuration(),
			Logger: logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		chainEngine, err := chainsync.New(chainsync.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Handler: loader, Logger: logger,
			Open: sources.Opener(chain.GetTopic()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		name := chain.GetName()
		runners = append(runners, Runner{Name: "chain " + name, Run: func(ctx context.Context) error {
			if err := chainEngine.Run(ctx); err != nil {
				return err
			}
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if err := loader.Flush(flushCtx); err != nil {
				logger.Error("flush on stop", "chain", name, "error", err)
			}
			return nil
		}})
	}
	return runners, nil
}

// NewRefreshers builds market-rankings' refresher per chain.
func NewRefreshers(chains []*conf.Chain, rankingConfig *conf.Rankings, source storage.RankingSource, store storage.RankingStore, logger *slog.Logger) (Runners, error) {
	period := rankingConfig.GetRefreshInterval().AsDuration()
	logger.Info("refreshing rankings", "every", period, "candle_interval", rankingConfig.GetCandleInterval().AsDuration())
	runners := make(Runners, 0, len(chains))
	for _, chain := range chains {
		refresher, err := rankings.New(rankings.Options{
			ChainID: chain.GetChainId(), Chain: chain.GetName(), Source: source, Store: store,
			Interval: rankingConfig.GetCandleInterval().AsDuration(), ReorgWindow: rankingConfig.GetReorgWindow().AsDuration(),
			Logger: logger.With("chain", chain.GetName()),
		})
		if err != nil {
			return nil, fmt.Errorf("chain %s: %w", chain.GetName(), err)
		}
		runners = append(runners, Runner{Name: "rankings " + chain.GetName(), Run: func(ctx context.Context) error {
			refresher.Run(ctx, period)
			return nil
		}})
	}
	return runners, nil
}
