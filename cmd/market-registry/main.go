// Command market-registry consumes chainstream-evm chain topics and keeps
// the latest token and pool metadata in PostgreSQL.
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

	"github.com/AloneAtWar/chainstream-market/internal/metrics"

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/registry"
	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
)

// Version is set at build time.
var Version = "dev"

// metricsAddr, when set, overrides metrics.addr.
var metricsAddr string

func main() {
	configPath := flag.String("conf", "configs/config.yaml", "config file path")
	flag.StringVar(&metricsAddr, "metrics-addr", "", "address to serve /metrics on, overriding metrics.addr")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "market-registry", "version", Version)
	if err := run(*configPath, logger); err != nil {
		logger.Error("market-registry stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	config, err := conf.Load(configPath)
	if err != nil {
		return err
	}
	if config.State.Driver != conf.StatePostgres {
		return fmt.Errorf("market-registry needs state.driver %q, got %q", conf.StatePostgres, config.State.Driver)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = metrics.Start(ctx, cmp.Or(metricsAddr, config.Metrics.Addr), logger); err != nil {
		return err
	}

	store, err := pgstore.Open(ctx, pgstore.Options{DSN: config.State.Postgres.DSN, Schema: config.State.Postgres.Schema})
	if err != nil {
		return err
	}
	defer store.Close()

	engines := make([]*engine.Engine, 0, len(config.Chains))
	for _, chain := range config.Chains {
		handler, handlerErr := registry.New(registry.Options{
			ChainID: chain.ChainID, Store: store, HistoryDepth: config.Registry.HistoryDepth,
			Logger: logger.With("chain", chain.Name),
		})
		if handlerErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, handlerErr)
		}
		options := source.KafkaOptions{
			Brokers: config.Kafka.Brokers, ClientID: config.Kafka.ClientID, Topic: chain.Topic,
			FetchMaxBytes: config.Kafka.FetchMaxBytes, FetchMaxWait: config.Kafka.FetchMaxWait,
		}
		chainEngine, newErr := engine.New(engine.Options{
			ChainID: chain.ChainID, Chain: chain.Name, Handler: handler, Logger: logger,
			Open: func(start int64) (source.Source, error) {
				return source.NewKafka(options, start)
			},
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
