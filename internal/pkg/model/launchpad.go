package model

import "math/big"

// Launchpad token statuses, in the order a curve moves through them.
//
// 发射平台代币状态：曲线交易中、曲线已打满待迁移、已迁移到 DEX。
const (
	LaunchpadBonding   = "bonding"
	LaunchpadCompleted = "completed"
	LaunchpadMigrated  = "migrated"
)

// LaunchpadToken is what one block changed about one launchpad token. Nil
// fields leave the stored value alone; counters add to it.
//
// LaunchpadToken 是一个区块对一个发射平台代币的改动：为 nil 的字段不改已存值，计数字段累加。
type LaunchpadToken struct {
	Token    string
	Protocol string
	PoolID   string
	Manager  string
	Quote    string
	// Launched is set when the block holds the token's launch, with its
	// creator, name, symbol and metadata reference.
	Launched bool
	Creator  string
	Name     string
	Symbol   string
	MetaURI  string
	LaunchTx string
	// Status is the furthest status the block moved the token to; a stored
	// later status is kept.
	Status string
	// The curve state after the token's last curve trade in the block, raw
	// integers; Progress is a wad (1e18 = 100%).
	Progress      *big.Int
	BaseRemaining *big.Int
	QuoteRaised   *big.Int
	Price         *big.Int
	// Trades, Buys and Sells count the block's curve trades.
	Trades, Buys, Sells int64
	// LastTradeTime is the block time of the token's last curve trade in the
	// block, or zero.
	LastTradeTime int64
	// TargetPoolID is the DEX pool a migration moved the liquidity into.
	TargetPoolID string
	// Direct marks a token launched straight into a DEX pool with no curve
	// (Bankr, Long.xyz, Clanker, pools.trade). It stays bonding, and its
	// trades are those of TradingPoolID. LaunchStart and LaunchEnd are the
	// platform's launch window in unix seconds, or zero.
	Direct        bool
	TradingPoolID string
	LaunchStart   int64
	LaunchEnd     int64
}

// LaunchpadChange is what one record changed in the launchpad tables.
//
// LaunchpadChange 是一条记录对发射平台表的改动：回滚，或一个区块内各代币的改动。
type LaunchpadChange struct {
	// Reverted undoes every change above RevertTo before anything else.
	Reverted bool
	RevertTo int64
	// Applied marks a change that carries a block. Without it, and without
	// Reverted, a commit only moves the checkpoint: chainsync commits blocks
	// it skipped as already handled, and those must leave the head alone.
	Applied bool
	// Height and Time are the block the tokens belong to; each token
	// appears at most once.
	Height int64
	Time   int64
	Tokens []LaunchpadToken
	// PrunedThrough discards undo history at or below it.
	PrunedThrough int64
}

// LaunchpadColumn is one of the three launchpad lists.
//
// LaunchpadColumn 是三栏之一。
type LaunchpadColumn string

// The launchpad lists: new launches, curves filling up, and curves that
// moved to a DEX.
//
// 三栏：新币（按发射时间倒序）、发射中（按进度倒序）、已迁移（按迁移时间倒序）。
const (
	LaunchpadColumnNew      LaunchpadColumn = "new"
	LaunchpadColumnBonding  LaunchpadColumn = "bonding"
	LaunchpadColumnMigrated LaunchpadColumn = "migrated"
)

// LaunchpadQuery selects one launchpad list. Windows are seconds before the
// chain head's block time.
//
// LaunchpadQuery 选择一栏：MaxAge 是新币 / 发射中代币距发射的最长时间，Stale 是发射中代币
// 距最后一笔交易的最长时间，MigratedAge 是已迁移代币距迁移的最长时间，MinProgress 是发射中
// 代币的最低进度（wad）。
type LaunchpadQuery struct {
	ChainID     uint64
	Column      LaunchpadColumn
	Protocol    string
	MaxAge      int64
	Stale       int64
	MigratedAge int64
	MinProgress *big.Int
	Limit       int
	// Targets give direct launches their pseudo-progress, see
	// LaunchpadTargets.
	Targets LaunchpadTargets
}

// LaunchpadTargets are the fully diluted values, in USD, at which a direct
// launch without a launch window counts as fully launched: its progress is
// its market cap over the target of its protocol, or Default. A direct
// launch with a window progresses with time instead.
//
// LaunchpadTargets 是直发代币的目标完全稀释市值（美元）：没有发射窗口的直发代币，进度 =
// 市值 / 所属平台的目标（未配置时用 Default）；有发射窗口的按时间推进。
type LaunchpadTargets struct {
	Default    float64
	ByProtocol map[string]float64
}

// LaunchpadListing is a launchpad token as the lists and the token page
// show it, joined with registry metadata and market prices.
//
// LaunchpadListing 是列表与代币页展示的发射平台代币，合并了 registry 元数据与行情。
type LaunchpadListing struct {
	Token        string
	Protocol     string
	PoolID       string
	Manager      string
	Quote        string
	Creator      *string
	Name         *string
	Symbol       *string
	Decimals     *uint8
	MetaURI      *string
	Status       string
	LaunchHeight int64
	LaunchTime   int64
	LaunchSeen   bool
	// Progress is the curve's progress, or a direct launch's pseudo-progress
	// (see LaunchpadTargets), as a wad.
	Progress      *big.Int
	BaseRemaining *big.Int
	QuoteRaised   *big.Int
	Price         *big.Int
	Trades        int64
	Buys          int64
	Sells         int64
	LastTradeTime *int64
	CompletedTime *int64
	MigratedTime  *int64
	TargetPoolID  *string
	Direct        bool
	TradingPoolID *string
	LaunchStart   *int64
	LaunchEnd     *int64
	// QuoteSymbol and QuoteDecimals describe the quote token from the
	// registry.
	QuoteSymbol   *string
	QuoteDecimals *uint8
	// From market-rankings, when the token is ranked.
	PriceUSD     *string
	Volume24hUSD *string
	Holders      *int64
	// HeadTime is the newest block time market-launchpad applied.
	HeadTime int64
}
