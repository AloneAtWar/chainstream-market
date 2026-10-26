// Command ch-loader consumes chainstream-evm chain topics and copies their
// raw facts, every ERC-20 transfer among them, into ClickHouse.
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
	"sync"
	"syscall"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/metrics"

	"github.com/AloneAtWar/chainstream-market/internal/chloader"
	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chraw"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
)

// Version is set at build time.
var Version = "dev"

// metricsAddr, when set, overrides metrics.addr.
var metricsAddr string

func main() {
	configPath := flag.String("conf", "configs/config.yaml", "config file path")
	flag.StringVar(&metricsAddr, "metrics-addr", "", "address to serve /metrics on, overriding metrics.addr")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "ch-loader", "version", Version)
	if err := run(*configPath, logger); err != nil {
		logger.Error("ch-loader stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	config, err := conf.Load(configPath)
	if err != nil {
		return err
	}
	if config.Storage.Driver != conf.StorageClickHouse {
		return fmt.Errorf("ch-loader needs storage.driver %q, got %q", conf.StorageClickHouse, config.Storage.Driver)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = metrics.Start(ctx, cmp.Or(metricsAddr, config.Metrics.Addr), logger); err != nil {
		return err
	}

	store, err := chraw.Open(ctx, chstore.Options{
		Addr: config.Storage.ClickHouse.Addr, Database: config.Storage.ClickHouse.Database,
		Username: config.Storage.ClickHouse.Username, Password: config.Storage.ClickHouse.Password,
		DialTimeout: config.Storage.ClickHouse.DialTimeout,
	})
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			logger.Error("close storage", "error", closeErr)
		}
	}()
	if err := store.ApplyRetention(ctx, chstore.Retention(config.Storage.ClickHouse.Retention)); err != nil {
		return err
	}

	loaders := make([]*chloader.Loader, 0, len(config.Chains))
	engines := make([]*engine.Engine, 0, len(config.Chains))
	for _, chain := range config.Chains {
		loader, loaderErr := chloader.New(chloader.Options{
			ChainID: chain.ChainID, Store: store, BatchLag: config.ChLoader.BatchLag,
			FlushRows: config.ChLoader.FlushRows, FlushInterval: config.ChLoader.FlushInterval,
			Logger: logger.With("chain", chain.Name),
		})
		if loaderErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, loaderErr)
		}
		options := source.KafkaOptions{
			Brokers: config.Kafka.Brokers, ClientID: config.Kafka.ClientID, Topic: chain.Topic,
			FetchMaxBytes: config.Kafka.FetchMaxBytes, FetchMaxWait: config.Kafka.FetchMaxWait,
		}
		chainEngine, newErr := engine.New(engine.Options{
			ChainID: chain.ChainID, Chain: chain.Name, Handler: loader, Logger: logger,
			Open: func(start int64) (source.Source, error) {
				return source.NewKafka(options, start)
			},
		})
		if newErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, newErr)
		}
		loaders = append(loaders, loader)
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
				return
			}
			// A clean stop writes the buffered batch so a restart replays less.
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer flushCancel()
			if flushErr := loaders[index].Flush(flushCtx); flushErr != nil {
				logger.Error("flush on stop", "chain", name, "error", flushErr)
			}
		})
	}
	wg.Wait()
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return nil
}
