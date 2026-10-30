package model

// StateKindBalancePools holds a BalancePool per pool ID: the latest balance
// snapshot chainstream-evm published for a Curve or Balancer pool.
//
// StateKindBalancePools 是 engine 状态中 Curve / Balancer 池余额快照的种类名，每个池子 ID 一条 BalancePool。
const StateKindBalancePools = "balance_pools"

// Curve families of a BalancePool.
//
// BalancePool 的曲线族：weighted（Balancer 加权池）、stable（Curve StableSwap / Balancer Stable）、
// crypto（Curve CryptoSwap）、unknown（无法建模，只用最近一笔 swap 的成交价）。
const (
	CurveWeighted = "weighted"
	CurveStable   = "stable"
	CurveCrypto   = "crypto"
	CurveUnknown  = "unknown"
)

// BalancePool is a multi-token pool's real balances and curve parameters at
// SnapshotHeight. Integers are decimal strings in raw token units, and every
// slice that is set is aligned with Tokens.
//
// BalancePool 是多币池在 SnapshotHeight 时的真实余额与曲线参数，由 chainstream-evm 在区块末读取后发布。
// 所有整数都是原始单位的十进制字符串；凡是设置了的切片，下标都与 Tokens 一一对应。
// 费率、权重、汇率都以 1e18 缩放；Ladders 只在 crypto 池上有，新快照没带时沿用上一次的。
type BalancePool struct {
	Protocol       string   `json:"protocol"`
	PoolAddress    string   `json:"pool_address,omitempty"`
	SnapshotHeight int64    `json:"snapshot_height"`
	Tokens         []string `json:"tokens"`
	Balances       []string `json:"balances"`
	// Curve is CurveWeighted, CurveStable, CurveCrypto or CurveUnknown; Reason
	// says why a pool is unknown.
	Curve  string `json:"curve"`
	Reason string `json:"reason,omitempty"`
	// Fee is the swap fee scaled by 1e18; DynamicFee marks fees that move
	// with pool state or a hook.
	Fee        string `json:"fee,omitempty"`
	DynamicFee bool   `json:"dynamic_fee,omitempty"`
	// Weights are normalized weights scaled by 1e18.
	Weights []string `json:"weights,omitempty"`
	// Amp is the amplification times AmpPrecision; Rates scale balances to
	// the invariant: xp = balance × rate / 1e18.
	Amp          string   `json:"amp,omitempty"`
	AmpPrecision string   `json:"amp_precision,omitempty"`
	Rates        []string `json:"rates,omitempty"`
	// Ladders are on-chain quotes of Curve crypto pools, kept from an
	// earlier snapshot when the latest has none.
	Ladders      []QuoteLadder `json:"ladders,omitempty"`
	LadderHeight int64         `json:"ladder_height,omitempty"`
	Derived      []DerivedRate `json:"derived,omitempty"`
}

// QuoteLadder is the pool's quoted output of Tokens[Out] for each amount of
// Tokens[In].
//
// QuoteLadder 是链上 get_dy 报价阶梯：卖出 Amounts[i] 个 Tokens[In] 能换到 Quotes[i] 个 Tokens[Out]。
type QuoteLadder struct {
	In      int      `json:"in"`
	Out     int      `json:"out"`
	Amounts []string `json:"amounts"`
	Quotes  []string `json:"quotes"`
}

// DerivedRate values one whole Token as Rate / 1e18 whole units of the
// cheapest of its Underlyings: a Curve base-pool LP token or a Balancer V3
// ERC4626 token.
//
// DerivedRate 表示一个 Token 按汇率定价：1 个 Token = Rate / 1e18 个最便宜的 Underlyings，
// 用于 Curve 元池的基础池 LP 代币（虚拟价格）和 Balancer V3 的 ERC4626 代币。
type DerivedRate struct {
	Token       string   `json:"token"`
	Underlyings []string `json:"underlyings"`
	Rate        string   `json:"rate"`
}
