// Package server turns each command's work into Kratos transports: chain
// runners, market-api's HTTP server and the metrics endpoint.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/biz"
	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/live"
	"github.com/AloneAtWar/chainstream-market/internal/metrics"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/google/wire"
)

// The server provider sets: WorkerSet for the commands that only run
// Runners, APISet for market-api.
var (
	ProviderSet = wire.NewSet(NewMetricsServer)
	WorkerSet   = wire.NewSet(ProviderSet, NewWorkerServers)
	APISet      = wire.NewSet(ProviderSet, NewHTTPServer, NewAPIServers)
)

// NewMetricsServer serves /metrics on metrics.addr, or returns nil when it
// is empty.
func NewMetricsServer(config *conf.Metrics, logger *slog.Logger) *metrics.Server {
	if config.GetAddr() == "" {
		return nil
	}
	return metrics.NewServer(config.GetAddr(), logger)
}

// NewWorkerServers runs each of runners, and serves metrics.
func NewWorkerServers(metricsServer *metrics.Server, runners biz.Runners) []transport.Server {
	servers := withMetrics(metricsServer)
	for _, runner := range runners {
		servers = append(servers, NewRunner(runner.Name, runner.Run))
	}
	return servers
}

// NewAPIServers serves market-api, its live hub when there is one, and
// metrics.
func NewAPIServers(metricsServer *metrics.Server, httpServer *HTTPServer, hub *live.Hub) []transport.Server {
	servers := append(withMetrics(metricsServer), httpServer)
	if hub != nil {
		servers = append(servers, NewRunner("live hub", hub.Run))
	}
	return servers
}

func withMetrics(metricsServer *metrics.Server) []transport.Server {
	if metricsServer == nil {
		return nil
	}
	return []transport.Server{metricsServer}
}

// HTTPServer serves market-api's handler on api.addr.
type HTTPServer struct {
	server *http.Server
	logger *slog.Logger
}

// NewHTTPServer serves handler on api.addr.
func NewHTTPServer(config *conf.API, handler http.Handler, logger *slog.Logger) *HTTPServer {
	return &HTTPServer{
		server: &http.Server{Addr: config.GetAddr(), Handler: handler, ReadHeaderTimeout: 5 * time.Second},
		logger: logger,
	}
}

// Start serves until Stop.
func (server *HTTPServer) Start(ctx context.Context) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", server.server.Addr)
	if err != nil {
		return fmt.Errorf("api: listen %s: %w", server.server.Addr, err)
	}
	server.logger.Info("listening", "addr", server.server.Addr)
	if err := server.server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("api: serve: %w", err)
	}
	return nil
}

// Stop shuts the server down, waiting at most ten seconds for requests.
func (server *HTTPServer) Stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return server.server.Shutdown(ctx)
}

// Runner is a transport.Server that runs a function until Stop. Kratos does
// not cancel the context it passes Start, so Stop cancels it and waits for
// the function to return. A function that fails, or returns before Stop,
// stops the application.
type Runner struct {
	name string
	run  func(context.Context) error

	mu       sync.Mutex
	cancel   context.CancelFunc
	stopping bool
	done     chan struct{}
}

// NewRunner runs run under name.
func NewRunner(name string, run func(context.Context) error) *Runner {
	return &Runner{name: name, run: run, done: make(chan struct{})}
}

func (runner *Runner) Start(ctx context.Context) error {
	defer close(runner.done)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runner.mu.Lock()
	runner.cancel = cancel
	if runner.stopping {
		cancel()
	}
	runner.mu.Unlock()

	err := runner.run(ctx)
	runner.mu.Lock()
	stopping := runner.stopping
	runner.mu.Unlock()
	switch {
	case stopping:
		return nil
	case err != nil:
		return fmt.Errorf("%s: %w", runner.name, err)
	default:
		return fmt.Errorf("%s stopped", runner.name)
	}
}

func (runner *Runner) Stop(ctx context.Context) error {
	runner.mu.Lock()
	runner.stopping = true
	if runner.cancel != nil {
		runner.cancel()
	}
	runner.mu.Unlock()
	select {
	case <-runner.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: stop: %w", runner.name, ctx.Err())
	}
}
