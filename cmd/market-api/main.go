// Command market-api serves prices, candles and trades from the ClickHouse
// store that market-engine writes, and holders and token and pool metadata
// from the PostgreSQL store that holder-indexer and market-registry write.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/api"
	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
)

// Version is set at build time.
var Version = "dev"

func main() {
	configPath := flag.String("conf", "configs/config.yaml", "config file path")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "market-api", "version", Version)
	if err := run(*configPath, logger); err != nil {
		logger.Error("market-api stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	config, err := conf.Load(configPath)
	if err != nil {
		return err
	}
	if config.Storage.Driver != conf.StorageClickHouse {
		return fmt.Errorf("market-api needs storage.driver %q, got %q", conf.StorageClickHouse, config.Storage.Driver)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := chstore.Open(ctx, chstore.Options{
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

	handler := api.New(store, config.Market.Intervals, logger)
	if config.State.Driver == conf.StatePostgres {
		holderStore, openErr := pgstore.Open(ctx, pgstore.Options{DSN: config.State.Postgres.DSN, Schema: config.State.Postgres.Schema})
		if openErr != nil {
			return openErr
		}
		defer holderStore.Close()
		handler.WithHolders(holderStore).WithRegistry(holderStore)
	}
	server := &http.Server{
		Addr:              config.API.Addr,
		Handler:           handler.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errs := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", config.API.Addr)
		errs <- server.ListenAndServe()
	}()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-errs; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
