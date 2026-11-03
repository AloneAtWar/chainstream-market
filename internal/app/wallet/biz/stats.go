package biz

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// day 是日汇总的粒度，按 UTC 切日。
const day = 24 * time.Hour

// Refresh 是一次汇总：Version 是 wallet_stats 行的版本（毫秒时间戳），Head 是当时最新成交
// 的时间，ApplyHeight 是当时 wallet_trades 的最高 apply_height。
type Refresh struct {
	ChainID     uint64
	Version     uint64
	Head        time.Time
	ApplyHeight int64
}

// SmartMoneyRule 是聪明钱规则：WindowDays 窗口内通过全部门槛、且不是机器人的钱包中，按
// 已实现盈亏取前 MaxWallets 个。
type SmartMoneyRule struct {
	WindowDays     int
	MinRealizedUSD float64
	MinWinRate     float64
	MinTokens      int64
	// MaxLossShare 是亏损超过 50% 的代币占交易代币数的上限。
	MaxLossShare float64
	MaxWallets   int
}

// BotRule 是机器人规则：WindowDays 窗口内交易笔数不少于 MinTrades，或清仓不少于 MinCloses
// 次且其中快进快出占比不低于 MinFastCloseShare。
type BotRule struct {
	WindowDays        int
	MinTrades         int64
	MinCloses         int64
	MinFastCloseShare float64
}

// StatsRepo 在 ClickHouse 里做汇总：wallet_trades → wallet_token_daily → wallet_stats。
type StatsRepo interface {
	// TradeHead 返回 wallet_trades 里最新的成交时间和最高的 apply_height。
	TradeHead(ctx context.Context, chainID uint64) (time.Time, int64, bool, error)
	LoadRefresh(ctx context.Context, chainID uint64) (Refresh, bool, error)
	// EarliestTradeAfter 返回 apply_height 大于 height 的流水里最早的成交时间。
	EarliestTradeAfter(ctx context.Context, chainID uint64, height int64) (time.Time, bool, error)
	// RebuildDaily 重算 from（某天零点）及之后每天的钱包 × 代币日汇总。
	RebuildDaily(ctx context.Context, chainID uint64, from time.Time, fastCloseSeconds int64) error
	// WriteStats 用 from 及之后的日汇总写出一个窗口的钱包统计，行版本为 version。
	WriteStats(ctx context.Context, chainID uint64, windowDays int, from time.Time, version uint64) error
	// Bots 返回版本 version 的统计中符合机器人规则的钱包。
	Bots(ctx context.Context, chainID uint64, rule BotRule, version uint64) ([]string, error)
	// SmartMoney 返回版本 version 的统计中符合聪明钱规则、且不符合机器人规则的钱包。
	SmartMoney(ctx context.Context, chainID uint64, rule SmartMoneyRule, bot BotRule, version uint64) ([]string, error)
	// SaveRefresh 记下这次汇总；market-api 只读最新一次汇总写出的统计。
	SaveRefresh(ctx context.Context, refresh Refresh) error
}

// LabelRepo 在 PostgreSQL 里保存钱包标签。
type LabelRepo interface {
	// ReplaceRuleLabels 把链上的规则标签换成 labels：新的插入，已有的保留生效时间，
	// 不再符合的删除。人工标签不动。
	ReplaceRuleLabels(ctx context.Context, chainID uint64, labels []model.WalletLabel) error
}

// StatsOptions 配置一条链的汇总。
type StatsOptions struct {
	ChainID uint64
	Chain   string
	Repo    StatsRepo
	Labels  LabelRepo
	// Windows 是统计窗口（天）。
	Windows          []int
	FastCloseSeconds int64
	SmartMoney       SmartMoneyRule
	Bot              BotRule
	Logger           *slog.Logger
	Now              func() time.Time
}

// Refresher 定时汇总一条链的钱包统计并重算规则标签。
type Refresher struct {
	options StatsOptions
}

// NewRefresher 创建 Refresher。
func NewRefresher(options StatsOptions) (*Refresher, error) {
	if options.ChainID == 0 || options.Repo == nil || options.Labels == nil || len(options.Windows) == 0 {
		return nil, errors.New("wallet stats: chain id, repos and windows are required")
	}
	if !slices.Contains(options.Windows, options.SmartMoney.WindowDays) || !slices.Contains(options.Windows, options.Bot.WindowDays) {
		return nil, errors.New("wallet stats: smart money and bot windows must be stat windows")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Refresher{options: options}, nil
}

// Refresh 汇总一次。还没有流水时什么都不做。
//
// 日汇总只重算变化过的日子：上次汇总时最新成交的前一天起（覆盖重组），以及上次汇总之后
// 新写入的流水里最早的那天起（补缺写进来的旧成交）。首次汇总重算最长窗口内的全部日子。
// 窗口按最新成交所在的日子往前数，不按墙钟，所以追块时统计也对得上。
func (refresher *Refresher) Refresh(ctx context.Context) error {
	options := refresher.options
	head, applyHeight, ok, err := options.Repo.TradeHead(ctx, options.ChainID)
	if err != nil || !ok {
		return err
	}
	headDay := head.UTC().Truncate(day)
	longest := slices.Max(options.Windows)
	from := headDay.AddDate(0, 0, -(longest - 1))
	previous, resumed, err := options.Repo.LoadRefresh(ctx, options.ChainID)
	if err != nil {
		return err
	}
	if resumed {
		changed := previous.Head.UTC().Truncate(day).AddDate(0, 0, -1)
		earliest, found, earliestErr := options.Repo.EarliestTradeAfter(ctx, options.ChainID, previous.ApplyHeight)
		if earliestErr != nil {
			return earliestErr
		}
		if found {
			changed = minTime(changed, earliest.UTC().Truncate(day))
		}
		from = maxTime(from, changed)
	}
	if err = options.Repo.RebuildDaily(ctx, options.ChainID, from, options.FastCloseSeconds); err != nil {
		return err
	}
	version := uint64(options.Now().UnixMilli())
	for _, window := range options.Windows {
		if err = options.Repo.WriteStats(ctx, options.ChainID, window, headDay.AddDate(0, 0, -(window-1)), version); err != nil {
			return err
		}
	}
	bots, err := options.Repo.Bots(ctx, options.ChainID, options.Bot, version)
	if err != nil {
		return err
	}
	smart, err := options.Repo.SmartMoney(ctx, options.ChainID, options.SmartMoney, options.Bot, version)
	if err != nil {
		return err
	}
	if err := options.Repo.SaveRefresh(ctx, Refresh{ChainID: options.ChainID, Version: version, Head: head, ApplyHeight: applyHeight}); err != nil {
		return err
	}
	since := options.Now().UTC()
	labels := make([]model.WalletLabel, 0, len(bots)+len(smart))
	for _, wallet := range smart {
		labels = append(labels, model.WalletLabel{Wallet: wallet, Label: model.LabelSmartMoney, Source: model.WalletLabelSource, Since: since})
	}
	for _, wallet := range bots {
		labels = append(labels, model.WalletLabel{Wallet: wallet, Label: model.LabelBot, Source: model.WalletLabelSource, Since: since})
	}
	if err := options.Labels.ReplaceRuleLabels(ctx, options.ChainID, labels); err != nil {
		return err
	}
	metrics.WalletLabels.WithLabelValues(options.Chain, model.LabelSmartMoney).Set(float64(len(smart)))
	metrics.WalletLabels.WithLabelValues(options.Chain, model.LabelBot).Set(float64(len(bots)))
	options.Logger.Info("wallet stats refreshed", "head", head, "from", from, "smart_money", len(smart), "bots", len(bots))
	return nil
}

// Run 立即汇总一次，之后每 period 一次，直到 ctx 结束。失败只记日志，下次重试。
func (refresher *Refresher) Run(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	chain := refresher.options.Chain
	for {
		started := time.Now()
		err := refresher.Refresh(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			metrics.WalletRefreshFailures.WithLabelValues(chain).Inc()
			refresher.options.Logger.Error("wallet stats refresh failed", "error", err)
		case err == nil:
			metrics.WalletRefreshSeconds.WithLabelValues(chain).Observe(time.Since(started).Seconds())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func minTime(left, right time.Time) time.Time {
	if right.Before(left) {
		return right
	}
	return left
}

func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}
