// Package server turns each command's work into Kratos transports: chain
// runners, market-api's HTTP server and the metrics endpoint.
package server

import (
	"log/slog"
	"net/http"

	"github.com/AloneAtWar/chainstream-market/internal/biz"
	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/transport"
	kratostransport "github.com/go-kratos/kratos/v2/transport"
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
func NewMetricsServer(config *config.Metrics, logger *slog.Logger) *metrics.Server {
	if config.GetAddr() == "" {
		return nil
	}
	return metrics.NewServer(config.GetAddr(), logger)
}

// NewWorkerServers runs each of runners, and serves metrics.
func NewWorkerServers(metricsServer *metrics.Server, runners biz.Runners) []kratostransport.Server {
	servers := withMetrics(metricsServer)
	for _, runner := range runners {
		servers = append(servers, transport.NewRunner(runner.Name, runner.Run))
	}
	return servers
}

// NewAPIServers serves market-api, its live hub when there is one, and
// metrics.
func NewAPIServers(metricsServer *metrics.Server, httpServer *transport.HTTPServer, hub *live.Hub) []kratostransport.Server {
	servers := append(withMetrics(metricsServer), httpServer)
	if hub != nil {
		servers = append(servers, transport.NewRunner("live hub", hub.Run))
	}
	return servers
}

func withMetrics(metricsServer *metrics.Server) []kratostransport.Server {
	if metricsServer == nil {
		return nil
	}
	return []kratostransport.Server{metricsServer}
}

// NewHTTPServer serves market-api's handler on api.addr.
func NewHTTPServer(config *conf.API, handler http.Handler, logger *slog.Logger) *transport.HTTPServer {
	return transport.NewHTTPServer(config.GetAddr(), handler, logger)
}
