// Command market-api serves prices, candles and trades from the ClickHouse
// store that market-engine writes and the raw tables ch-loader writes, and
// holders, token and pool metadata and token lists from the PostgreSQL store
// that holder-indexer, market-registry and market-rankings write. With
// live.enabled it also streams market-engine's live topics over WebSocket.
package main

import (
	"cmp"
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

	"github.com/AloneAtWar/chainstream-market/internal/metrics"

	"github.com/AloneAtWar/chainstream-market/internal/api"
	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/live"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chraw"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
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
	if err = metrics.Start(ctx, cmp.Or(metricsAddr, config.Metrics.Addr), logger); err != nil {
		return err
	}

	clickhouse := chstore.Options{
		Addr: config.Storage.ClickHouse.Addr, Database: config.Storage.ClickHouse.Database,
		Username: config.Storage.ClickHouse.Username, Password: config.Storage.ClickHouse.Password,
		DialTimeout: config.Storage.ClickHouse.DialTimeout,
	}
	store, err := chstore.Open(ctx, clickhouse)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			logger.Error("close storage", "error", closeErr)
		}
	}()
	rawStore, err := chraw.Open(ctx, clickhouse)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := rawStore.Close(); closeErr != nil {
			logger.Error("close raw storage", "error", closeErr)
		}
	}()

	handler := api.New(store, config.Market.Intervals, logger).WithTransfers(rawStore)
	if config.State.Driver == conf.StatePostgres {
		holderStore, openErr := pgstore.Open(ctx, pgstore.Options{DSN: config.State.Postgres.DSN, Schema: config.State.Postgres.Schema})
		if openErr != nil {
			return openErr
		}
		defer holderStore.Close()
		handler.WithHolders(holderStore).WithRegistry(holderStore).WithRankings(holderStore)
	}
	var hubDone chan error
	if config.Live.Enabled {
		hub, hubErr := live.NewHub(live.HubOptions{
			Brokers: config.Kafka.Brokers, ClientID: config.Kafka.ClientID,
			FetchMaxBytes: config.Kafka.FetchMaxBytes, FetchMaxWait: config.Kafka.FetchMaxWait,
			Topics: config.LiveTopics(), Intervals: config.Market.Intervals,
			Throttle: config.Live.CandleThrottle, ClientBuffer: config.Live.ClientBuffer,
			MaxSubscriptions: config.Live.MaxSubscriptions, OriginPatterns: config.Live.AllowedOrigins,
			Logger: logger,
		})
		if hubErr != nil {
			return hubErr
		}
		hubDone = make(chan error, 1)
		go func() { hubDone <- hub.Run(ctx) }()
		handler.WithLive(hub)
	}
	server := &http.Server{
		Addr:              config.API.Addr,
		Handler:           metrics.Instrument(handler.Handler()),
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
	case err := <-hubDone:
		// The hub only stops on its own when it cannot start.
		if ctx.Err() == nil {
			return errors.Join(errors.New("live hub stopped"), err)
		}
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
