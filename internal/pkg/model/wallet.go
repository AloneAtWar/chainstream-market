package model

import (
	"time"

	"github.com/shopspring/decimal"
)

// WalletKey 标识一笔持仓：一个钱包持有的一个代币。
type WalletKey struct {
	Wallet string
	Token  string
}

// Position 是 market-wallet 按移动加权平均成本维护的持仓账本，一个（链, 钱包, 代币）一行。
//
// Quantity 只包含经由 DEX 买入、有成本的数量；转入或空投来的代币不进账本，所以
// Quantity 可以小于链上余额。CostUSD 是 Quantity 的剩余成本，卖出按比例扣减，所以卖出
// 不改变均价。Wins 和 Losses 计数计入已实现盈亏的卖出（按笔算的胜率）。
type Position struct {
	Wallet      string
	Token       string
	Quantity    decimal.Decimal
	CostUSD     decimal.Decimal
	RealizedUSD decimal.Decimal
	BoughtQty   decimal.Decimal
	SoldQty     decimal.Decimal
	BoughtUSD   decimal.Decimal
	SoldUSD     decimal.Decimal
	Buys        int64
	Sells       int64
	Wins        int64
	Losses      int64
	// FirstBuyHeight 和 FirstBuyTime 是账本里第一次买入的区块，没有买入时为零。
	FirstBuyHeight int64
	FirstBuyTime   time.Time
	// OpenedAt 是当前这一轮持仓（Quantity 从零变正）开始的时间；清仓后为零。
	OpenedAt      time.Time
	LastTradeTime time.Time
	// UpdatedHeight 是最后一次改动这行的 live 记录高度。
	UpdatedHeight int64
}

// Key 返回持仓的键。
func (position Position) Key() WalletKey {
	return WalletKey{Wallet: position.Wallet, Token: position.Token}
}

// AverageCost 返回每个代币的平均成本，没有持仓时为零。
func (position Position) AverageCost() decimal.Decimal {
	if !position.Quantity.IsPositive() {
		return decimal.Zero
	}
	return position.CostUSD.DivRound(position.Quantity, 18)
}

// WalletEvent 描述一笔成交对持仓的作用，对应 GMGN 的建仓/加仓/减仓/清仓。
type WalletEvent uint8

const (
	// WalletEventUnknown 是未设置的值。
	WalletEventUnknown WalletEvent = iota
	// WalletEventOpen 是从零持仓买入。
	WalletEventOpen
	// WalletEventAdd 是在已有持仓上买入。
	WalletEventAdd
	// WalletEventReduce 是卖出一部分有成本的持仓。
	WalletEventReduce
	// WalletEventClose 是卖光有成本的持仓。
	WalletEventClose
	// WalletEventUntracked 是卖出账本里没有成本的代币（转入或空投来的），不计入已实现盈亏。
	WalletEventUntracked
)

var walletEventNames = [...]string{"unknown", "open", "add", "reduce", "close", "untracked"}

// String 返回事件名，与 ClickHouse 枚举一致。
func (event WalletEvent) String() string {
	if int(event) < len(walletEventNames) {
		return walletEventNames[event]
	}
	return "unknown"
}

// ParseWalletEvent 解析事件名，不认识的返回 WalletEventUnknown。
func ParseWalletEvent(name string) WalletEvent {
	for index, candidate := range walletEventNames {
		if candidate == name {
			return WalletEvent(index)
		}
	}
	return WalletEventUnknown
}

// WalletTrade 是一笔成交在钱包账本里的结果，market-wallet 写入 ClickHouse 的 wallet_trades。
//
// ApplyHeight 和 Part 是带来这笔成交的 live 记录（或从 ClickHouse 回放的高度），重组和
// 崩溃恢复按它们删除；BlockHeight 和 BlockTime 是成交本身所在的区块。
type WalletTrade struct {
	ChainID         uint64
	ApplyHeight     int64
	Part            int32
	BlockHeight     int64
	BlockTime       time.Time
	TransactionHash string
	LogIndex        uint32
	OperationIndex  uint32
	Wallet          string
	Token           string
	QuoteToken      string
	PoolID          string
	Side            Side
	Event           WalletEvent
	Amount          decimal.Decimal
	ValueUSD        decimal.Decimal
	PriceUSD        decimal.Decimal
	// RealizedUSD 和 CostSoldUSD 只在 Counted 时有意义：卖出的有成本部分的收入减成本。
	RealizedUSD decimal.Decimal
	CostSoldUSD decimal.Decimal
	Counted     bool
	// HoldSeconds 是清仓时这一轮持仓的时长，其他事件为零。
	HoldSeconds   int64
	QuantityAfter decimal.Decimal
	CostAfter     decimal.Decimal
}

// WalletStats 是一个钱包在一个时间窗口内的汇总，market-wallet 定时写入 ClickHouse 的 wallet_stats。
type WalletStats struct {
	ChainID     uint64
	WindowDays  uint16
	Wallet      string
	RealizedUSD decimal.Decimal
	BoughtUSD   decimal.Decimal
	SoldUSD     decimal.Decimal
	Buys        uint64
	Sells       uint64
	// Wins 和 Counted 是计入已实现盈亏的卖出中盈利的笔数和总笔数，胜率 = Wins / Counted。
	Wins    uint64
	Counted uint64
	Tokens  uint64
	// 按代币 ROI（已实现 / 卖出部分成本）分的五个桶。
	TokensOver5x   uint64
	Tokens2xTo5x   uint64
	Tokens0To2x    uint64
	TokensLoss50   uint64
	TokensLossOver uint64
	Closes         uint64
	HoldSeconds    uint64
	FastCloses     uint64
	RefreshedAt    time.Time
}

// WinRate 返回按笔算的胜率，没有计入盈亏的卖出时为零。
func (stats WalletStats) WinRate() float64 {
	if stats.Counted == 0 {
		return 0
	}
	return float64(stats.Wins) / float64(stats.Counted)
}

// ROI 返回已实现盈亏除以窗口内买入金额，没有买入时为零。
func (stats WalletStats) ROI() decimal.Decimal {
	if !stats.BoughtUSD.IsPositive() {
		return decimal.Zero
	}
	return stats.RealizedUSD.DivRound(stats.BoughtUSD, 6)
}

// AverageHold 返回窗口内清仓的平均持仓时长。
func (stats WalletStats) AverageHold() time.Duration {
	if stats.Closes == 0 {
		return 0
	}
	return time.Duration(stats.HoldSeconds/stats.Closes) * time.Second
}

// 钱包标签。规则标签由 market-wallet 每次汇总后重算。
const (
	// LabelSmartMoney 是聪明钱。
	LabelSmartMoney = "smart_money"
	// LabelBot 是机器人：交易笔数极多或大量快进快出。
	LabelBot = "bot"
)

// WalletLabel 是一个钱包的一个标签。
type WalletLabel struct {
	Wallet string
	Label  string
	// Source 是 rule（规则算出）或 manual（人工维护）。
	Source string
	// Since 是标签首次生效的时间。
	Since time.Time
}

// WalletLabelSource 是规则标签的来源名。
const WalletLabelSource = "rule"

// WalletStatsQuery 查询最近一次汇总的钱包统计。
type WalletStatsQuery struct {
	ChainID uint64
	// WindowDays 为零时返回全部窗口。
	WindowDays uint16
	// Wallets 不为空时只返回这些钱包。
	Wallets []string
	// Sort 是 realized、winrate、roi 或 volume。
	Sort  string
	Limit int
}

// WalletTradeQuery 查询一个钱包的账本流水，按（区块, log_index, operation_index）倒序。
type WalletTradeQuery struct {
	ChainID uint64
	Wallet  string
	// Token 不为空时只返回这个代币。
	Token string
	// BeforeHeight 不为零时只返回排在翻页位置之后的成交：只给高度时是更早的区块；再给
	// BeforeLogIndex（和 BeforeOperationIndex）时从同一区块里接着往下翻。
	BeforeHeight         int64
	BeforeLogIndex       *uint32
	BeforeOperationIndex *uint32
	Limit                int
}

// PositionQuery 查询持仓：按钱包（Wallet）或按代币（Token）。
type PositionQuery struct {
	ChainID uint64
	Wallet  string
	Token   string
	// Open 为 true 只返回仍有持仓的，为 false 只返回已清仓的，nil 都返回。
	Open  *bool
	Limit int
}

// PricedPosition 是带现价的持仓，供 market-api 返回未实现盈亏。
type PricedPosition struct {
	Position
	// PriceUSD 是 market-rankings 的最新价，没有价格时为 nil。
	PriceUSD *decimal.Decimal
	Symbol   *string
}

// UnrealizedUSD 返回 Quantity × 现价 − 剩余成本，没有价格时为 nil。
func (position PricedPosition) UnrealizedUSD() *decimal.Decimal {
	if position.PriceUSD == nil {
		return nil
	}
	value := position.Quantity.Mul(*position.PriceUSD).Sub(position.CostUSD).Round(18)
	return &value
}
