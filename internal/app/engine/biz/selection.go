package biz

import (
	"math"

	"github.com/shopspring/decimal"
)

// A token is priced through the pool that can absorb the most of a trade
// near the price: the counterpart token it pays out while that token's
// price moves across the pool's fee-tier band, valued in USD. Pools of every
// protocol compete on that one measure. A pool whose depth is unknown (a
// stale tick table, an unknown Balancer hook, a Curve crypto pool without
// quotes yet) ranks after every pool with a known depth, and among those by
// the USD value of its counterpart balance, as every candidate still needs
// MinLiquidityUSD of real counterpart balance.
//
// 定价池的选择：代币通过"在价格移动一个费率档价格带的过程中，能付出最多 USD 对手方代币"的池子定价，
// 所有协议的池子都按这一个指标比较。深度未知的池子（tick 表过期、未知 Balancer hook、还没有报价阶梯的
// Curve crypto 池）排在所有深度已知的池子之后，它们之间再按对手方余额的 USD 价值排序；
// 任何候选都仍需达到 MinLiquidityUSD 的真实对手方余额。

// quoteCandidate is one pool's price for a token and the depth behind it.
//
// quoteCandidate 是某个池子给出的代币价格及其背后的深度。
type quoteCandidate struct {
	price    tokenPrice
	depthUSD decimal.Decimal
	hasDepth bool
}

// beats reports whether candidate is a better pricing pool than best: a
// known depth over an unknown one, then the deeper, then the larger
// counterpart balance, then the lower pool ID.
//
// beats 比较顺序：深度已知优先 → 深度更大 → 对手方余额更大 → 池子 ID 更小。
func (candidate quoteCandidate) beats(best quoteCandidate) bool {
	if candidate.hasDepth != best.hasDepth {
		return candidate.hasDepth
	}
	if candidate.hasDepth && !candidate.depthUSD.Equal(best.depthUSD) {
		return candidate.depthUSD.GreaterThan(best.depthUSD)
	}
	if !candidate.price.LiquidityUSD.Equal(best.price.LiquidityUSD) {
		return candidate.price.LiquidityUSD.GreaterThan(best.price.LiquidityUSD)
	}
	return candidate.price.Pool < best.price.Pool
}

// constantProductFee is the fee tier, in hundredths of a basis point, whose
// band sizes the depth of a pool known only by its reserves.
//
// constantProductFee：只有储备的池子按 0.3% 费率档的价格带计算深度。
const constantProductFee = 3000

// pairDepth returns the Tokens[position] a two-token pool pays out within its
// band: a concentrated pool's depth from its tick table, or for a pool with
// reserves and no tick table the constant-product depth
// B·(1 − 1/√(1 + band)).
//
// pairDepth 返回两币池在价格带内付出的 Tokens[position]：集中流动性池用 tick 表算出的深度；
// 只有储备的池子按常数乘积公式 B·(1 − 1/√(1 + band)) 计算；集中流动性池没有可用深度时返回 false。
func (processor *Processor) pairDepth(id string, state pool, position int) (decimal.Decimal, bool) {
	if state.HasDepth {
		return state.Depth[position], true
	}
	if _, concentrated := processor.clamm.get(id); concentrated || !state.HasReserves {
		return decimal.Decimal{}, false
	}
	band := float64(processor.depthBPS(constantProductFee)) / 10_000
	share := decimal.NewFromFloat(1 - 1/math.Sqrt(1+band))
	return state.Reserves[position].Mul(share).Round(precision), true
}
