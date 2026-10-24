// Package rankings rebuilds the token rankings that market-api lists from
// market-engine's ClickHouse output and holder-indexer's and
// market-registry's PostgreSQL tables.
//
// A refresh recomputes 24h volume, trades and price change from token
// candles over the window ending at the chain head. Latest prices are
// incremental: only tokens priced since the last refresh's head, less the
// reorg window, are read again, so a reorg that moved or removed a recent
// price is picked up while older prices, which no reorg reaches, stay as
// stored.
package rankings

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

// Window is the period the 24h columns cover.
const Window = 24 * time.Hour

type Options struct {
	ChainID uint64
	Source  storage.RankingSource
	Store   storage.RankingStore
	// Interval is the token candle size summed over the window; it must be
	// one market-engine writes.
	Interval time.Duration
	// ReorgWindow is how far behind the last refresh's head prices are read
	// again. It must exceed the time a reorg can reach back.
	ReorgWindow time.Duration
	Logger      *slog.Logger
	Now         func() time.Time
}

// Refresher refreshes one chain's rankings.
type Refresher struct {
	options Options
}

func New(options Options) (*Refresher, error) {
	if options.ChainID == 0 || options.Source == nil || options.Store == nil {
		return nil, errors.New("rankings: chain id, source and store are required")
	}
	if options.Interval < time.Second || options.Interval > Window {
		return nil, fmt.Errorf("rankings: candle interval %s must be between 1s and %s", options.Interval, Window)
	}
	if options.ReorgWindow <= 0 {
		return nil, errors.New("rankings: reorg window must be positive")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Refresher{options: options}, nil
}

// Refresh rebuilds the rankings once. It does nothing before market-engine
// has stored a block.
func (refresher *Refresher) Refresh(ctx context.Context) error {
	options := refresher.options
	head, ok, err := options.Source.ChainHead(ctx, options.ChainID)
	if err != nil || !ok {
		return err
	}
	cursor, resumed, err := options.Store.LoadRankingCursor(ctx, options.ChainID)
	if err != nil {
		return err
	}
	refresh := storage.RankingRefresh{ChainID: options.ChainID, Head: head}
	var recent []string
	if resumed {
		refresh.Since = cursor.Add(-options.ReorgWindow)
		if recent, err = options.Store.RankedSince(ctx, options.ChainID, refresh.Since); err != nil {
			return err
		}
	}
	if refresh.Quotes, err = options.Source.LatestQuotes(ctx, options.ChainID, refresh.Since, recent); err != nil {
		return err
	}
	since := head.Add(-Window).Truncate(options.Interval)
	if refresh.Activity, err = options.Source.TokenActivity(ctx, options.ChainID, options.Interval, since); err != nil {
		return err
	}
	refresh.RefreshedAt = options.Now().UTC()
	if err := options.Store.CommitRankings(ctx, refresh); err != nil {
		return err
	}
	options.Logger.Debug("rankings refreshed", "head", head, "quotes", len(refresh.Quotes), "active", len(refresh.Activity))
	return nil
}

// Run refreshes now and then every period until ctx ends. A failed refresh
// is logged and retried at the next tick, since the next one rebuilds the
// same data.
func (refresher *Refresher) Run(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		if err := refresher.Refresh(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			refresher.options.Logger.Error("refresh rankings", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
