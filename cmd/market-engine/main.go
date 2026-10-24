// Command market-engine consumes chainstream-evm chain topics and derives
// orientation, prices and candles.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/market"
	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/shopspring/decimal"
)

// Version is set at build time.
var Version = "dev"

func main() {
	configPath := flag.String("conf", "configs/config.yaml", "config file path")
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

	memory := storage.NewMemory()
	engines := make([]*engine.Engine, 0, len(config.Chains))
	for _, chain := range config.Chains {
		options := source.KafkaOptions{
			Brokers: config.Kafka.Brokers, ClientID: config.Kafka.ClientID, Topic: chain.Topic,
			FetchMaxBytes: config.Kafka.FetchMaxBytes, FetchMaxWait: config.Kafka.FetchMaxWait,
		}
		processor, processorErr := market.NewProcessor(market.Options{
			Catalog:               market.DefaultCatalog(chain.ChainID),
			MinLiquidityUSD:       decimal.NewFromFloat(config.Market.MinLiquidityUSD),
			CandleMinLiquidityUSD: decimal.NewFromFloat(config.Market.CandleMinLiquidityUSD),
			MaxHops:               config.Market.MaxHops,
			Intervals:             config.Market.Intervals,
			HistoryDepth:          config.Market.HistoryDepth,
		})
		if processorErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, processorErr)
		}
		chainEngine, newErr := engine.New(engine.Options{
			ChainID: chain.ChainID, Chain: chain.Name,
			Open: func(start int64) (source.Source, error) {
				return source.NewKafka(options, start)
			},
			Processor: processor, Output: memory, Checkpoints: memory,
			Logger: logger,
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
