// Command market-rankings periodically rolls market-engine's ClickHouse
// output, holder counts and token metadata into the PostgreSQL token
// rankings that market-api lists.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/transport"
)

// Name is the service name.
const Name = "market-rankings"

var (
	// Version is set at build time.
	Version = "dev"

	flagconf string
	// metricsAddr, when set, overrides metrics.addr.
	metricsAddr string
	id, _       = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "configs/config.yaml", "config file or directory, eg: -conf configs/config.yaml")
	flag.StringVar(&metricsAddr, "metrics-addr", "", "address to serve /metrics on, overriding metrics.addr")
}

func newApp(servers []transport.Server) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Server(servers...),
	)
}

func main() {
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", Name, "version", Version)
	if err := run(logger); err != nil {
		logger.Error(Name+" stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) (resultErr error) {
	bootstrap, closeConfig, err := conf.Load(flagconf)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, closeConfig()) }()
	if metricsAddr != "" {
		bootstrap.Metrics.Addr = metricsAddr
	}

	// A signal while the stores connect stops the start; once the app runs,
	// Kratos handles signals itself.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	app, cleanup, err := wireApp(ctx, bootstrap, logger)
	stop()
	if err != nil {
		return err
	}
	defer cleanup()
	return app.Run()
}
