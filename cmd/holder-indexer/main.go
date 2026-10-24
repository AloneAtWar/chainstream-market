// Command holder-indexer consumes chainstream-evm chain topics and keeps
// ERC-20 balances per holder in PostgreSQL.
package main

import (
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

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/holders"
	"github.com/AloneAtWar/chainstream-market/internal/rpc"
	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
)

// Version is set at build time.
var Version = "dev"

func main() {
	configPath := flag.String("conf", "configs/config.yaml", "config file path")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "holder-indexer", "version", Version)
	if err := run(*configPath, logger); err != nil {
		logger.Error("holder-indexer stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	config, err := conf.Load(configPath)
	if err != nil {
		return err
	}
	if config.State.Driver != conf.StatePostgres {
		return fmt.Errorf("holder-indexer needs state.driver %q, got %q", conf.StatePostgres, config.State.Driver)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := pgstore.Open(ctx, pgstore.Options{DSN: config.State.Postgres.DSN, Schema: config.State.Postgres.Schema})
	if err != nil {
		return err
	}
	defer store.Close()

	reconcileEvery := max(config.Holders.ReconcileEvery, 0)
	engines := make([]*engine.Engine, 0, len(config.Chains))
	for _, chain := range config.Chains {
		if strings.TrimSpace(chain.RPCURL) == "" {
			return fmt.Errorf("chain %s: rpc_url is required", chain.Name)
		}
		client, rpcErr := rpc.New(rpc.Options{
			URL: chain.RPCURL, BatchSize: config.Holders.RPCBatchSize, Timeout: config.Holders.RPCTimeout,
		})
		if rpcErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, rpcErr)
		}
		indexer, indexerErr := holders.New(holders.Options{
			ChainID: chain.ChainID, Store: store, Balances: client,
			HistoryDepth: config.Holders.HistoryDepth, ReconcileEvery: reconcileEvery,
			ReconcileBatch: config.Holders.ReconcileBatch,
			Logger:         logger.With("chain", chain.Name),
		})
		if indexerErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, indexerErr)
		}
		options := source.KafkaOptions{
			Brokers: config.Kafka.Brokers, ClientID: config.Kafka.ClientID, Topic: chain.Topic,
			FetchMaxBytes: config.Kafka.FetchMaxBytes, FetchMaxWait: config.Kafka.FetchMaxWait,
		}
		chainEngine, newErr := engine.New(engine.Options{
			ChainID: chain.ChainID, Chain: chain.Name, Handler: indexer, Logger: logger,
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
