// Command market-rankings periodically rolls market-engine's ClickHouse
// output, holder counts and token metadata into the PostgreSQL token
// rankings that market-api lists.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/rankings"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
)

// Version is set at build time.
var Version = "dev"

func main() {
	configPath := flag.String("conf", "configs/config.yaml", "config file path")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "market-rankings", "version", Version)
	if err := run(*configPath, logger); err != nil {
		logger.Error("market-rankings stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	config, err := conf.Load(configPath)
	if err != nil {
		return err
	}
	if config.Storage.Driver != conf.StorageClickHouse || config.State.Driver != conf.StatePostgres {
		return fmt.Errorf("market-rankings needs storage.driver %q and state.driver %q", conf.StorageClickHouse, conf.StatePostgres)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	source, err := chstore.Open(ctx, chstore.Options{
		Addr: config.Storage.ClickHouse.Addr, Database: config.Storage.ClickHouse.Database,
		Username: config.Storage.ClickHouse.Username, Password: config.Storage.ClickHouse.Password,
		DialTimeout: config.Storage.ClickHouse.DialTimeout,
	})
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := source.Close(); closeErr != nil {
			logger.Error("close storage", "error", closeErr)
		}
	}()
	store, err := pgstore.Open(ctx, pgstore.Options{DSN: config.State.Postgres.DSN, Schema: config.State.Postgres.Schema})
	if err != nil {
		return err
	}
	defer store.Close()

	refreshers := make([]*rankings.Refresher, 0, len(config.Chains))
	for _, chain := range config.Chains {
		refresher, newErr := rankings.New(rankings.Options{
			ChainID: chain.ChainID, Source: source, Store: store,
			Interval: config.Rankings.CandleInterval, ReorgWindow: config.Rankings.ReorgWindow,
			Logger: logger.With("chain", chain.Name),
		})
		if newErr != nil {
			return fmt.Errorf("chain %s: %w", chain.Name, newErr)
		}
		refreshers = append(refreshers, refresher)
	}
	logger.Info("refreshing rankings", "every", config.Rankings.RefreshInterval, "candle_interval", config.Rankings.CandleInterval)
	var wg sync.WaitGroup
	for _, refresher := range refreshers {
		wg.Go(func() { refresher.Run(ctx, config.Rankings.RefreshInterval) })
	}
	wg.Wait()
	return nil
}
