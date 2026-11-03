// Package conf 是 market-wallet 的配置：Bootstrap proto、默认值和校验。
package conf

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/google/wire"
)

const (
	defaultHistoryDepth    = 256
	defaultHistory         = 90 * 24 * time.Hour
	defaultFinalAge        = time.Hour
	defaultBackfillHeights = 2000
	defaultBatchLag        = time.Minute
	defaultFlushRows       = 20000
	defaultFlushInterval   = 5 * time.Second
	defaultTradesDays      = 120
	defaultCloseRatio      = 0.001

	defaultRefreshInterval  = 15 * time.Minute
	defaultFastCloseSeconds = 5

	defaultSmartWindow      = 30
	defaultSmartRealizedUSD = 5000
	defaultSmartWinRate     = 0.5
	defaultSmartTokens      = 10
	defaultSmartLossShare   = 0.3
	defaultSmartMaxWallets  = 1000

	defaultBotWindow         = 7
	defaultBotTrades         = 2000
	defaultBotCloses         = 10
	defaultBotFastCloseShare = 0.3

	// maxWindowDays 是统计窗口的上限，Alone 定的最长 90 天。
	maxWindowDays = 90
)

// defaultWindows 是默认的统计窗口（天）。
var defaultWindows = []int32{7, 30, 90}

// ProviderSet 把 Bootstrap 的各段提供给依赖注入。
var ProviderSet = wire.NewSet(
	wire.FieldsOf(new(*Bootstrap), "Kafka", "Clickhouse", "Postgres", "Metrics", "Chains", "Wallet"),
)

// Load 读取、补默认值并校验 path（文件或目录）处的配置；返回的函数关闭配置源。
func Load(path string) (*Bootstrap, func() error, error) {
	bootstrap := &Bootstrap{}
	closeConfig, err := config.Load(path, bootstrap)
	if err != nil {
		return nil, nil, err
	}
	return bootstrap, closeConfig, nil
}

// Parse 不读环境变量，直接解码、补默认值并校验 YAML。
func Parse(raw []byte) (*Bootstrap, error) {
	bootstrap := &Bootstrap{}
	if err := config.Parse(raw, bootstrap); err != nil {
		return nil, err
	}
	return bootstrap, nil
}

// ApplyDefaults 补齐每一段，并把没设置的项换成默认值。
func (bootstrap *Bootstrap) ApplyDefaults() {
	if bootstrap.Kafka == nil {
		bootstrap.Kafka = &config.Kafka{}
	}
	if bootstrap.Clickhouse == nil {
		bootstrap.Clickhouse = &config.ClickHouse{}
	}
	if bootstrap.Postgres == nil {
		bootstrap.Postgres = &config.Postgres{}
	}
	if bootstrap.Metrics == nil {
		bootstrap.Metrics = &config.Metrics{}
	}
	if bootstrap.Wallet == nil {
		bootstrap.Wallet = &Wallet{}
	}
	bootstrap.Kafka.ApplyDefaults()
	bootstrap.Clickhouse.ApplyDefaults()
	config.ApplyChainDefaults(bootstrap.Chains)

	wallet := bootstrap.Wallet
	if wallet.HistoryDepth == 0 {
		wallet.HistoryDepth = defaultHistoryDepth
	}
	config.DefaultDuration(&wallet.History, defaultHistory)
	config.DefaultDuration(&wallet.FinalAge, defaultFinalAge)
	if wallet.BackfillHeights == 0 {
		wallet.BackfillHeights = defaultBackfillHeights
	}
	config.DefaultDuration(&wallet.BatchLag, defaultBatchLag)
	if wallet.FlushRows == 0 {
		wallet.FlushRows = defaultFlushRows
	}
	config.DefaultDuration(&wallet.FlushInterval, defaultFlushInterval)
	if wallet.TradesDays == 0 {
		wallet.TradesDays = defaultTradesDays
	}
	if wallet.CloseRatio == 0 {
		wallet.CloseRatio = defaultCloseRatio
	}
	if wallet.Stats == nil {
		wallet.Stats = &Stats{}
	}
	applyStatsDefaults(wallet.Stats)
}

func applyStatsDefaults(stats *Stats) {
	config.DefaultDuration(&stats.RefreshInterval, defaultRefreshInterval)
	if len(stats.Windows) == 0 {
		stats.Windows = append([]int32(nil), defaultWindows...)
	}
	if stats.FastCloseSeconds == 0 {
		stats.FastCloseSeconds = defaultFastCloseSeconds
	}
	if stats.SmartMoney == nil {
		stats.SmartMoney = &SmartMoney{}
	}
	smart := stats.SmartMoney
	if smart.WindowDays == 0 {
		smart.WindowDays = defaultSmartWindow
	}
	if smart.MinRealizedUsd == 0 {
		smart.MinRealizedUsd = defaultSmartRealizedUSD
	}
	if smart.MinWinRate == 0 {
		smart.MinWinRate = defaultSmartWinRate
	}
	if smart.MinTokens == 0 {
		smart.MinTokens = defaultSmartTokens
	}
	if smart.MaxLossShare == 0 {
		smart.MaxLossShare = defaultSmartLossShare
	}
	if smart.MaxWallets == 0 {
		smart.MaxWallets = defaultSmartMaxWallets
	}
	if stats.Bot == nil {
		stats.Bot = &Bot{}
	}
	bot := stats.Bot
	if bot.WindowDays == 0 {
		bot.WindowDays = defaultBotWindow
	}
	if bot.MinTrades == 0 {
		bot.MinTrades = defaultBotTrades
	}
	if bot.MinCloses == 0 {
		bot.MinCloses = defaultBotCloses
	}
	if bot.MinFastCloseShare == 0 {
		bot.MinFastCloseShare = defaultBotFastCloseShare
	}
}

// Validate 返回补过默认值的 Bootstrap 中第一个无效的设置。
func (bootstrap *Bootstrap) Validate() error {
	if err := bootstrap.GetKafka().Validate(true); err != nil {
		return err
	}
	if err := bootstrap.GetClickhouse().Validate(); err != nil {
		return err
	}
	if err := bootstrap.GetPostgres().Validate(); err != nil {
		return err
	}
	// market-wallet 读的是 live topic，按 live 开启的规则检查它和链 topic 不冲突。
	if err := config.ValidateChains(bootstrap.GetChains(), &config.Live{Enabled: true}); err != nil {
		return err
	}
	wallet := bootstrap.GetWallet()
	if wallet.GetHistoryDepth() < 0 || wallet.GetBackfillHeights() <= 0 || wallet.GetFlushRows() < 0 || wallet.GetTradesDays() < 0 {
		return errors.New("conf: wallet history_depth, backfill_heights, flush_rows and trades_days must be positive")
	}
	for _, duration := range []time.Duration{
		wallet.GetHistory().AsDuration(), wallet.GetFinalAge().AsDuration(), wallet.GetBatchLag().AsDuration(),
		wallet.GetFlushInterval().AsDuration(), wallet.GetStats().GetRefreshInterval().AsDuration(),
	} {
		if duration < 0 {
			return errors.New("conf: wallet durations must not be negative")
		}
	}
	if wallet.GetHistory().AsDuration() > maxWindowDays*24*time.Hour {
		return fmt.Errorf("conf: wallet.history must be at most %d days", maxWindowDays)
	}
	if ratio := wallet.GetCloseRatio(); ratio < 0 || ratio >= 1 {
		return errors.New("conf: wallet.close_ratio must be in [0, 1)")
	}
	if err := validateStats(wallet.GetStats()); err != nil {
		return err
	}
	// 统计从 wallet_trades 重算，流水必须比最长的窗口留得久。
	if days := wallet.GetTradesDays(); days > 0 && days < slices.Max(wallet.GetStats().GetWindows()) {
		return errors.New("conf: wallet.trades_days must cover the longest stats window")
	}
	return nil
}

func validateStats(stats *Stats) error {
	windows := make(map[int32]struct{}, len(stats.GetWindows()))
	for _, window := range stats.GetWindows() {
		if window <= 0 || window > maxWindowDays {
			return fmt.Errorf("conf: wallet.stats.windows must be between 1 and %d days", maxWindowDays)
		}
		windows[window] = struct{}{}
	}
	if _, ok := windows[stats.GetSmartMoney().GetWindowDays()]; !ok {
		return fmt.Errorf("conf: wallet.stats.smart_money.window_days %d must be one of windows", stats.GetSmartMoney().GetWindowDays())
	}
	if _, ok := windows[stats.GetBot().GetWindowDays()]; !ok {
		return fmt.Errorf("conf: wallet.stats.bot.window_days %d must be one of windows", stats.GetBot().GetWindowDays())
	}
	if stats.GetFastCloseSeconds() < 0 {
		return errors.New("conf: wallet.stats.fast_close_seconds must not be negative")
	}
	smart, bot := stats.GetSmartMoney(), stats.GetBot()
	if smart.GetMinWinRate() < 0 || smart.GetMinWinRate() > 1 || smart.GetMaxLossShare() < 0 || smart.GetMaxLossShare() > 1 ||
		bot.GetMinFastCloseShare() < 0 || bot.GetMinFastCloseShare() > 1 {
		return errors.New("conf: wallet.stats shares and win rates must be between 0 and 1")
	}
	if smart.GetMaxWallets() < 0 || smart.GetMinTokens() < 0 || bot.GetMinTrades() < 0 || bot.GetMinCloses() < 0 {
		return errors.New("conf: wallet.stats counts must not be negative")
	}
	return nil
}
