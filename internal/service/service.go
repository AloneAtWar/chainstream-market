// Package service builds market-api's HTTP handler from the stores the
// data layer opened.
package service

import (
	"log/slog"
	"net/http"

	"github.com/AloneAtWar/chainstream-market/internal/api"
	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/data"
	"github.com/AloneAtWar/chainstream-market/internal/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/google/wire"
)

// ProviderSet is the transport service provider set.
var ProviderSet = wire.NewSet(NewAPIHandler)

// NewAPIHandler serves market-engine's output and ch-loader's transfers;
// holders, metadata and token lists when the state backend serves them; and
// the live stream when hub is set.
func NewAPIHandler(
	marketConfig *conf.Market,
	reader storage.Reader,
	transfers storage.TransferReader,
	state data.StateReaders,
	hub *live.Hub,
	logger *slog.Logger,
) http.Handler {
	handler := api.New(reader, marketConfig.IntervalDurations(), logger).WithTransfers(transfers)
	if state.Holders != nil {
		handler.WithHolders(state.Holders)
	}
	if state.Registry != nil {
		handler.WithRegistry(state.Registry)
	}
	if state.Rankings != nil {
		handler.WithRankings(state.Rankings)
	}
	if hub != nil {
		handler.WithLive(hub)
	}
	return metrics.Instrument(handler.Handler())
}
