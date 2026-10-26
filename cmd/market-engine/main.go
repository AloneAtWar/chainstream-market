// Command market-engine consumes chainstream-evm chain topics and derives
// orientation, prices and candles.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/AloneAtWar/chainstream-market/internal/metrics"

	"github.com/AloneAtWar/chainstream-market/internal/chainlink"
	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/live"
	"github.com/AloneAtWar/chainstream-market/internal/market"
	"github.com/AloneAtWar/chainstream-market/internal/rpc"
	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chraw"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
	"github.com/shopspring/decimal"
)

// Version is set at build time.
var Version = "dev"

// metricsAddr, when set, overrides metrics.addr.
var metricsAddr string

func main() {
	configPath := flag.String("conf", "configs/config.yaml", "config file path")
	flag.StringVar(&metricsAddr, "metrics-addr", "", "address to serve /metrics on, overriding metrics.addr")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "market-engine", "version", Version)
	if err := run(*configPath, logger); err != nil {
		logger.Error("market-engine stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	config, err := conf.Load(configPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = metrics.Start(ctx, cmp.Or(metricsAddr, config.Metrics.Addr), logger); err != nil {
		return err
	}

	memory := storage.NewMemory()
	output, closeOutput, err := openOutput(ctx, config.Storage, memory)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := closeOutput(); closeErr != nil {
			logger.Error("close storage", "error", closeErr)
		}
	}()
	state, closeState, err := openState(ctx, config.State)
	if err != nil {
		return err
	}
	defer closeState()
	notifier, closeNotifier, err := openNotifier(config)
	if err != nil {
		return err
	}
	defer closeNotifier()
	history, closeHistory, err := openHistory(ctx, config)
	if err != nil {
		return err
	}
	defer closeHistory()
	engines := make([]*engine.Engine, 0, len(config.Chains))
	for _, chain := range config.Chains {
		options := source.KafkaOptions{
			Brokers: config.Kafka.Brokers, ClientID: config.Kafka.ClientID, Topic: chain.Topic,
			FetchMaxBytes: config.Kafka.FetchMaxBytes, FetchMaxWait: config.Kafka.FetchMaxWait,
		}
		feeds, feedsErr := resolveFeeds(ctx, chain, logger)
		if feedsErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, feedsErr)
		}
		processor, processorErr := market.NewProcessor(market.Options{
			Catalog:               market.DefaultCatalog(chain.ChainID),
			MinLiquidityUSD:       decimal.NewFromFloat(config.Market.MinLiquidityUSD),
			CandleMinLiquidityUSD: decimal.NewFromFloat(config.Market.CandleMinLiquidityUSD),
			MaxHops:               config.Market.MaxHops,
			Intervals:             config.Market.Intervals,
			HistoryDepth:          config.Market.HistoryDepth,
			Feeds:                 feeds,
			OracleDeviation:       decimal.NewFromFloat(config.Market.OracleDeviation),
			OutlierDeviation:      decimal.NewFromFloat(max(config.Market.OutlierDeviation, 0)),
			LateTradeBlocks:       max(config.Market.LateTradeBlocks, 0),
			LateTradesPerToken:    config.Market.LateTradesPerToken,
			History:               history,
			BackfillBlocks:        max(config.Market.BackfillBlocks, 0),
			Logger:                logger.With("chain", chain.Name),
		})
		if processorErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, processorErr)
		}
		chainEngine, newErr := engine.New(engine.Options{
			ChainID: chain.ChainID, Chain: chain.Name,
			Open: func(start int64) (source.Source, error) {
				return source.NewKafka(options, start)
			},
			Processor: processor, Output: output, Checkpoints: memory, State: state,
			Notifier: notifier, Logger: logger,
		})
		if newErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, newErr)
		}
		engines = append(engines, chainEngine)
	}

	// Chains are independent; the first failure stops them all so the
	// process restarts from consistent checkpoints.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	for index, chainEngine := range engines {
		name := config.Chains[index].Name
		wg.Go(func() {
			if runErr := chainEngine.Run(ctx); runErr != nil {
				cancel(fmt.Errorf("chain %s: %w", name, runErr))
			}
		})
	}
	wg.Wait()
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return nil
}

// resolveFeeds turns a chain's configured feeds into trusted aggregators.
// A proxy is read over the chain's rpc_url for the aggregator of every phase,
// and its decimals and description are checked against the config.
func resolveFeeds(ctx context.Context, chain conf.Chain, logger *slog.Logger) ([]market.Feed, error) {
	var client *rpc.Client
	proxies := make(map[string]chainlink.Proxy)
	var feeds []market.Feed
	for _, feed := range chain.Feeds {
		if feed.Proxy == "" {
			feeds = append(feeds, market.Feed{Address: feed.Address, Token: feed.Token, Decimals: feed.Decimals})
			continue
		}
		address := strings.ToLower(strings.TrimSpace(feed.Proxy))
		proxy, ok := proxies[address]
		if !ok {
			var err error
			if client == nil {
				if client, err = rpc.New(rpc.Options{URL: chain.RPCURL, Chain: chain.Name}); err != nil {
					return nil, err
				}
			}
			if proxy, err = chainlink.Resolve(ctx, client, address); err != nil {
				return nil, err
			}
			proxies[address] = proxy
			logger.Info("feed proxy resolved", "chain", chain.Name, "proxy", address,
				"description", proxy.Description, "decimals", proxy.Decimals, "aggregators", proxy.Aggregators)
		}
		if feed.Decimals != 0 && feed.Decimals != proxy.Decimals {
			return nil, fmt.Errorf("feed proxy %s has %d decimals, config says %d", address, proxy.Decimals, feed.Decimals)
		}
		if want := strings.TrimSpace(feed.Description); want != "" && !strings.EqualFold(want, strings.TrimSpace(proxy.Description)) {
			return nil, fmt.Errorf("feed proxy %s is %q, config says %q", address, proxy.Description, want)
		}
		for _, aggregator := range proxy.Aggregators {
			feeds = append(feeds, market.Feed{Address: aggregator, Token: feed.Token, Decimals: proxy.Decimals})
		}
	}
	return feeds, nil
}

// openNotifier returns the live publisher, or nil when live push is off.
func openNotifier(config *conf.Config) (engine.Notifier, func(), error) {
	if !config.Live.Enabled {
		return nil, func() {}, nil
	}
	publisher, err := live.NewPublisher(live.PublisherOptions{
		Brokers: config.Kafka.Brokers, ClientID: config.Kafka.ClientID,
		Topics: config.LiveTopics(), MaxLag: config.Live.MaxLag,
	})
	if err != nil {
		return nil, nil, err
	}
	return publisher, publisher.Close, nil
}

// openHistory returns ch-loader's raw trades for backfills, or nil when
// backfills are off or the output is not in ClickHouse.
func openHistory(ctx context.Context, config *conf.Config) (storage.History, func(), error) {
	if config.Storage.Driver != conf.StorageClickHouse || config.Market.BackfillBlocks <= 0 {
		return nil, func() {}, nil
	}
	clickhouse := config.Storage.ClickHouse
	store, err := chraw.Open(ctx, chstore.Options{
		Addr: clickhouse.Addr, Database: clickhouse.Database,
		Username: clickhouse.Username, Password: clickhouse.Password, DialTimeout: clickhouse.DialTimeout,
	})
	if err != nil {
		return nil, nil, err
	}
	return chraw.NewHistory(store, config.Market.BackfillWait), func() { _ = store.Close() }, nil
}

// openState returns the persistent state store, or nil when checkpoints stay
// in memory.
func openState(ctx context.Context, config conf.State) (storage.StateStore, func(), error) {
	if config.Driver != conf.StatePostgres {
		return nil, func() {}, nil
	}
	store, err := pgstore.Open(ctx, pgstore.Options{DSN: config.Postgres.DSN, Schema: config.Postgres.Schema})
	if err != nil {
		return nil, nil, err
	}
	return store, store.Close, nil
}

// openOutput returns the configured output store and its closer.
func openOutput(ctx context.Context, config conf.Storage, memory *storage.Memory) (storage.Output, func() error, error) {
	switch config.Driver {
	case conf.StorageClickHouse:
		store, err := chstore.Open(ctx, chstore.Options{
			Addr: config.ClickHouse.Addr, Database: config.ClickHouse.Database,
			Username: config.ClickHouse.Username, Password: config.ClickHouse.Password,
			DialTimeout: config.ClickHouse.DialTimeout,
		})
		if err != nil {
			return nil, nil, err
		}
		if err := store.ApplyRetention(ctx, chstore.Retention(config.ClickHouse.Retention)); err != nil {
			return nil, nil, errors.Join(err, store.Close())
		}
		return store, store.Close, nil
	default:
		return memory, func() error { return nil }, nil
	}
}
