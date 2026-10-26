// Package server serves market-api's HTTP handler, its live hub and the
// metrics endpoint as Kratos servers.
package server

import (
	"log/slog"
	"net/http"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/transport"
	kratostransport "github.com/go-kratos/kratos/v2/transport"
	"github.com/google/wire"
)

// ProviderSet is the server provider set.
var ProviderSet = wire.NewSet(NewServers)

// NewServers serves handler on api.addr, runs the live hub when there is
// one, and serves metrics.
func NewServers(
	api *conf.API, metricsConfig *config.Metrics, handler http.Handler, hub *live.Hub, logger *slog.Logger,
) []kratostransport.Server {
	servers := append(metrics.Servers(metricsConfig, logger), transport.NewHTTPServer(api.GetAddr(), handler, logger))
	if hub != nil {
		servers = append(servers, transport.NewRunner("live hub", hub.Run))
	}
	return servers
}
