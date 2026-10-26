// Package service builds market-api's HTTP handler from the stores the
// data layer opened.
package service

import (
	"log/slog"
	"net/http"

	"github.com/AloneAtWar/chainstream-market/internal/api"
	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/live"
	"github.com/AloneAtWar/chainstream-market/internal/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chraw"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
	"github.com/google/wire"
)

// ProviderSet is the transport service provider set.
var ProviderSet = wire.NewSet(NewAPIHandler)

// NewAPIHandler serves market-engine's output and ch-loader's transfers
// from ClickHouse; holders, metadata and token lists from PostgreSQL when
// state.driver is postgres; and the live stream when hub is set.
func NewAPIHandler(
	marketConfig *conf.Market,
	store *chstore.Store,
	raw *chraw.Store,
	postgres *pgstore.Store,
	hub *live.Hub,
	logger *slog.Logger,
) http.Handler {
	handler := api.New(store, marketConfig.IntervalDurations(), logger).WithTransfers(raw)
	if postgres != nil {
		handler.WithHolders(postgres).WithRegistry(postgres).WithRankings(postgres)
	}
	if hub != nil {
		handler.WithLive(hub)
	}
	return metrics.Instrument(handler.Handler())
}
