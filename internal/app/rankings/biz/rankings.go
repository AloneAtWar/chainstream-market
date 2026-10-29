// Package biz is market-rankings' business layer: it rebuilds the token rankings that market-api lists from
// market-engine's ClickHouse output and holder-indexer's and
// market-registry's PostgreSQL tables.
//
// A refresh recomputes 24h volume, trades and price change from token
// candles over the window ending at the chain head. Latest prices are
// incremental: only tokens priced since the last refresh's head, less the
// reorg window, are read again, so a reorg that moved or removed a recent
// price is picked up while older prices, which no reorg reaches, stay as
// stored.
//
// market-rankings 业务层：定期重建 market-api 代币列表使用的 token_rankings 表。
// 24h 成交量、笔数、涨跌幅取自截至链头（ClickHouse 中最新区块，而非墙钟）的代币聚合
// K 线；最新价格增量读取，只重读上次刷新以来（减去 reorg 窗口）有新价格的代币；
// 持有人数来自 holder-indexer 的 holder_counts，部署信息来自 market-registry。
package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// RankingSource reads what the rankings are built from out of the
// market-engine output.
type RankingSource interface {
	// ChainHead returns the time of the highest stored block.
	ChainHead(ctx context.Context, chainID uint64) (time.Time, bool, error)
	// TokenActivity aggregates the current version of every token candle of
	// interval that opens at or after since.
	TokenActivity(ctx context.Context, chainID uint64, interval time.Duration, since time.Time) ([]model.TokenActivity, error)
	// LatestQuotes returns the latest price of every token priced at or
	// after since and of each of tokens. A zero since returns every token.
	LatestQuotes(ctx context.Context, chainID uint64, since time.Time, tokens []string) ([]model.TokenQuote, error)
}

// RankingRepo keeps the token rankings. CommitRankings applies a refresh
// and recounts holders and creation heights from the other PostgreSQL
// tables in one transaction.
type RankingRepo interface {
	// LoadRankingCursor returns the chain head time of the last refresh.
	LoadRankingCursor(ctx context.Context, chainID uint64) (time.Time, bool, error)
	// RankedSince returns ranked tokens whose price is at or after since.
	RankedSince(ctx context.Context, chainID uint64, since time.Time) ([]string, error)
	CommitRankings(ctx context.Context, refresh model.RankingRefresh) error
}

// Window is the period the 24h columns cover.
const Window = 24 * time.Hour

type Options struct {
	ChainID uint64
	// Chain labels the refresher's metrics.
	Chain  string
	Source RankingSource
	Repo   RankingRepo
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
	if options.ChainID == 0 || options.Source == nil || options.Repo == nil {
		return nil, errors.New("rankings: chain id, source and repo are required")
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
	cursor, resumed, err := options.Repo.LoadRankingCursor(ctx, options.ChainID)
	if err != nil {
		return err
	}
	refresh := model.RankingRefresh{ChainID: options.ChainID, Head: head}
	var recent []string
	if resumed {
		refresh.Since = cursor.Add(-options.ReorgWindow)
		if recent, err = options.Repo.RankedSince(ctx, options.ChainID, refresh.Since); err != nil {
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
	if err := options.Repo.CommitRankings(ctx, refresh); err != nil {
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
		start := time.Now()
		err := refresher.Refresh(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			metrics.RankingsRefreshFailures.WithLabelValues(refresher.options.Chain).Inc()
			refresher.options.Logger.Error("refresh rankings", "error", err)
		} else {
			metrics.RankingsRefreshSeconds.WithLabelValues(refresher.options.Chain).Observe(time.Since(start).Seconds())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
