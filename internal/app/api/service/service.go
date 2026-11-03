package service

import (
	"log/slog"
	"net/http"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/google/wire"
)

// ProviderSet is the service provider set.
var ProviderSet = wire.NewSet(NewHandler)

// NewHandler serves market-engine's output, ch-loader's transfers,
// holder-indexer's holders, market-registry's metadata, market-rankings'
// token lists and market-launchpad's lists, and the live stream when hub is
// set.
func NewHandler(
	api *conf.API,
	reader Reader,
	transfers TransferReader,
	holders HolderReader,
	registry RegistryReader,
	rankings RankingReader,
	liquidity LiquidityReader,
	launchpad LaunchpadReader,
	hub *live.Hub,
	logger *slog.Logger,
) http.Handler {
	server := New(reader, api.IntervalDurations(), logger).
		WithTransfers(transfers).WithHolders(holders).WithRegistry(registry).WithRankings(rankings).
		WithLiquidity(liquidity).WithLaunchpad(launchpad)
	if hub != nil {
		server.WithLive(hub)
	}
	return metrics.Instrument(server.Handler())
}
