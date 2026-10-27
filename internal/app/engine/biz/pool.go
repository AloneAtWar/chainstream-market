package biz

import (
	"math/big"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

// precision is the number of decimal places kept by divisions, enough for
// long-tail token prices far below one cent.
const precision = 36

var q96 = decimal.NewFromBigInt(new(big.Int).Lsh(big.NewInt(1), 96), 0)

// pool is a two-token pool's latest observed state in human units.
type pool struct {
	Tokens [2]string // pool token order, normalized
	// reserves are the tokens the pool holds: its reserves for
	// constant-product pools, and for concentrated-liquidity pools what its
	// tick table holds at the current price. A concentrated pool without a
	// usable table has none.
	Reserves    [2]decimal.Decimal
	HasReserves bool
	// Depth is what a concentrated pool trades within its fee tier's price
	// band: tokens[0] sold while the price rises to the band's top and
	// tokens[1] while it falls to the bottom.
	Depth    [2]decimal.Decimal
	HasDepth bool
	// price is tokens[1] per tokens[0].
	Price    decimal.Decimal
	HasPrice bool
}

// tableAmounts returns the raw token amounts a concentrated pool's tick table
// holds at a sqrt price, or false when the pool has no usable table.
type tableAmounts func(poolID string, sqrtPriceX96 *big.Int) ([2]*big.Int, bool)

// observePool returns the pool state after trade. decimals reports known
// token decimals. ok is false when the trade carries nothing new.
func observePool(prev pool, trade *streamv1.Trade, decimals func(string) (uint32, bool), amountsAt tableAmounts) (pool, bool) {
	amounts := trade.GetAmounts()
	if len(amounts) != 2 {
		return prev, false
	}
	next := prev
	next.Tokens = [2]string{normalize(amounts[0].GetAsset()), normalize(amounts[1].GetAsset())}
	if next.Tokens[0] == next.Tokens[1] {
		return prev, false
	}
	d0, ok0 := decimals(next.Tokens[0])
	d1, ok1 := decimals(next.Tokens[1])
	if !ok0 || !ok1 {
		return prev, false
	}
	state := trade.GetPoolState()
	switch {
	case len(state.GetReservesAfter()) == 2:
		r0, err0 := decimal.NewFromString(state.GetReservesAfter()[0])
		r1, err1 := decimal.NewFromString(state.GetReservesAfter()[1])
		if err0 != nil || err1 != nil || r0.IsNegative() || r1.IsNegative() {
			return prev, false
		}
		next.Reserves = [2]decimal.Decimal{r0.Shift(-int32(d0)), r1.Shift(-int32(d1))}
		next.HasReserves = true
		next.HasPrice = !next.Reserves[0].IsZero()
		if next.HasPrice {
			next.Price = next.Reserves[1].DivRound(next.Reserves[0], precision)
		}
	case state.GetSqrtPriceX96() != "":
		sqrtPrice, price, ok := concentratedPrice(state.GetSqrtPriceX96(), d0, d1)
		if !ok {
			return prev, false
		}
		next.Price, next.HasPrice = price, true
		next.Reserves, next.HasReserves = [2]decimal.Decimal{}, false
		if raw, ok := amountsAt(normalize(trade.GetPoolId()), sqrtPrice); ok {
			next.Reserves = [2]decimal.Decimal{decimal.NewFromBigInt(raw[0], -int32(d0)), decimal.NewFromBigInt(raw[1], -int32(d1))}
			next.HasReserves = true
		}
	case trade.GetAction() == streamv1.TradeAction_TRADE_ACTION_SWAP:
		a0, okA := rawValue(amounts[0].GetRaw(), d0)
		a1, okB := rawValue(amounts[1].GetRaw(), d1)
		if !okA || !okB || a0.IsZero() || a1.IsZero() {
			return prev, false
		}
		next.Price = a1.Abs().DivRound(a0.Abs(), precision)
		next.HasPrice = true
	default:
		return prev, false
	}
	return next, true
}

// concentratedPrice parses a Uniswap v3 style sqrtPriceX96 and converts it to
// a human price of token1 per token0.
func concentratedPrice(sqrtPriceX96 string, d0, d1 uint32) (*big.Int, decimal.Decimal, bool) {
	sqrtRaw, ok := new(big.Int).SetString(sqrtPriceX96, 10)
	if !ok || sqrtRaw.Sign() <= 0 {
		return nil, decimal.Decimal{}, false
	}
	sqrtPrice := decimal.NewFromBigInt(sqrtRaw, 0).DivRound(q96, precision)
	if !sqrtPrice.IsPositive() {
		return nil, decimal.Decimal{}, false
	}
	price := sqrtPrice.Mul(sqrtPrice).Shift(int32(d0) - int32(d1)).Round(precision)
	return sqrtRaw, price, price.IsPositive()
}

// index returns token's position in the pool after canonicalization.
func (p pool) index(catalog asset.Catalog, token string) (int, bool) {
	for position, member := range p.Tokens {
		if catalog.Canonical(member) == token {
			return position, true
		}
	}
	return 0, false
}

// priceOf returns the price of tokens[position] in units of the other token.
func (p pool) priceOf(position int) (decimal.Decimal, bool) {
	if !p.HasPrice || !p.Price.IsPositive() {
		return decimal.Decimal{}, false
	}
	if position == 0 {
		return p.Price, true
	}
	return decimal.NewFromInt(1).DivRound(p.Price, precision), true
}

// rawValue converts a raw integer amount to human units.
func rawValue(raw string, decimals uint32) (decimal.Decimal, bool) {
	value, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		return decimal.Decimal{}, false
	}
	return decimal.NewFromBigInt(value, -int32(decimals)), true
}

func rawSign(amount *streamv1.Amount) int {
	raw, ok := new(big.Int).SetString(amount.GetRaw(), 10)
	if !ok {
		return 0
	}
	return raw.Sign()
}

func normalize(address string) string { return asset.Normalize(address) }
