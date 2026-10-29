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

// quoteCandidate is one pool's price for a token and the depth behind it.
type quoteCandidate struct {
	price    tokenPrice
	depthUSD decimal.Decimal
	hasDepth bool
}

// beats reports whether candidate is a better pricing pool than best: a
// known depth over an unknown one, then the deeper, then the larger
// counterpart balance, then the lower pool ID.
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
const constantProductFee = 3000

// pairDepth returns the Tokens[position] a two-token pool pays out within its
// band: a concentrated pool's depth from its tick table, or for a pool with
// reserves and no tick table the constant-product depth
// B·(1 − 1/√(1 + band)).
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
