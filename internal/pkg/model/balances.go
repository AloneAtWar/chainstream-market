package model

// StateKindBalancePools holds a BalancePool per pool ID: the latest balance
// snapshot chainstream-evm published for a Curve or Balancer pool.
const StateKindBalancePools = "balance_pools"

// Curve families of a BalancePool.
const (
	CurveWeighted = "weighted"
	CurveStable   = "stable"
	CurveCrypto   = "crypto"
	CurveUnknown  = "unknown"
)

// BalancePool is a multi-token pool's real balances and curve parameters at
// SnapshotHeight. Integers are decimal strings in raw token units, and every
// slice that is set is aligned with Tokens.
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
type QuoteLadder struct {
	In      int      `json:"in"`
	Out     int      `json:"out"`
	Amounts []string `json:"amounts"`
	Quotes  []string `json:"quotes"`
}

// DerivedRate values one whole Token as Rate / 1e18 whole units of the
// cheapest of its Underlyings: a Curve base-pool LP token or a Balancer V3
// ERC4626 token.
type DerivedRate struct {
	Token       string   `json:"token"`
	Underlyings []string `json:"underlyings"`
	Rate        string   `json:"rate"`
}
