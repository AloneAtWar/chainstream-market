package market

import (
	"math/big"

	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

// precision is the number of decimal places kept by divisions, enough for
// long-tail token prices far below one cent.
const precision = 36

var q96 = decimal.NewFromBigInt(new(big.Int).Lsh(big.NewInt(1), 96), 0)

// pool is a two-token pool's latest observed state in human units.
type pool struct {
	tokens [2]string // pool token order, normalized
	// reserves are actual reserves for constant-product pools and virtual
	// reserves at the current price for concentrated-liquidity pools.
	reserves    [2]decimal.Decimal
	hasReserves bool
	// price is tokens[1] per tokens[0].
	price    decimal.Decimal
	hasPrice bool
}

// observePool returns the pool state after trade. decimals reports known
// token decimals. ok is false when the trade carries nothing new.
func observePool(prev pool, trade *streamv1.Trade, decimals func(string) (uint32, bool)) (pool, bool) {
	amounts := trade.GetAmounts()
	if len(amounts) != 2 {
		return prev, false
	}
	next := prev
	next.tokens = [2]string{normalize(amounts[0].GetAsset()), normalize(amounts[1].GetAsset())}
	if next.tokens[0] == next.tokens[1] {
		return prev, false
	}
	d0, ok0 := decimals(next.tokens[0])
	d1, ok1 := decimals(next.tokens[1])
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
		next.reserves = [2]decimal.Decimal{r0.Shift(-int32(d0)), r1.Shift(-int32(d1))}
		next.hasReserves = true
		next.hasPrice = !next.reserves[0].IsZero()
		if next.hasPrice {
			next.price = next.reserves[1].DivRound(next.reserves[0], precision)
		}
	case state.GetSqrtPriceX96() != "" && state.GetLiquidity() != "":
		reserves, price, ok := concentratedState(state.GetSqrtPriceX96(), state.GetLiquidity(), d0, d1)
		if !ok {
			return prev, false
		}
		next.reserves, next.price = reserves, price
		next.hasReserves, next.hasPrice = true, true
	case trade.GetAction() == streamv1.TradeAction_TRADE_ACTION_SWAP:
		a0, okA := amountValue(amounts[0])
		a1, okB := amountValue(amounts[1])
		if !okA || !okB || a0.IsZero() || a1.IsZero() {
			return prev, false
		}
		next.price = a1.Abs().DivRound(a0.Abs(), precision)
		next.hasPrice = true
	default:
		return prev, false
	}
	return next, true
}

// concentratedState converts a Uniswap v3 style sqrtPriceX96 and in-range
// liquidity to virtual reserves and a human price of token1 per token0.
func concentratedState(sqrtPriceX96, liquidity string, d0, d1 uint32) ([2]decimal.Decimal, decimal.Decimal, bool) {
	sqrtRaw, err0 := decimal.NewFromString(sqrtPriceX96)
	active, err1 := decimal.NewFromString(liquidity)
	if err0 != nil || err1 != nil || !sqrtRaw.IsPositive() || active.IsNegative() {
		return [2]decimal.Decimal{}, decimal.Decimal{}, false
	}
	sqrtPrice := sqrtRaw.DivRound(q96, precision)
	if !sqrtPrice.IsPositive() {
		return [2]decimal.Decimal{}, decimal.Decimal{}, false
	}
	raw0 := active.DivRound(sqrtPrice, precision)
	raw1 := active.Mul(sqrtPrice)
	reserves := [2]decimal.Decimal{raw0.Shift(-int32(d0)), raw1.Shift(-int32(d1)).Round(precision)}
	price := sqrtPrice.Mul(sqrtPrice).Shift(int32(d0) - int32(d1)).Round(precision)
	return reserves, price, price.IsPositive()
}

// index returns token's position in the pool after canonicalization.
func (p pool) index(catalog Catalog, token string) (int, bool) {
	for position, member := range p.tokens {
		if catalog.Canonical(member) == token {
			return position, true
		}
	}
	return 0, false
}

// priceOf returns the price of tokens[position] in units of the other token.
func (p pool) priceOf(position int) (decimal.Decimal, bool) {
	if !p.hasPrice || !p.price.IsPositive() {
		return decimal.Decimal{}, false
	}
	if position == 0 {
		return p.price, true
	}
	return decimal.NewFromInt(1).DivRound(p.price, precision), true
}

func amountValue(amount *streamv1.Amount) (decimal.Decimal, bool) {
	if !amount.GetDecimalsKnown() {
		return decimal.Decimal{}, false
	}
	raw, ok := new(big.Int).SetString(amount.GetRaw(), 10)
	if !ok {
		return decimal.Decimal{}, false
	}
	return decimal.NewFromBigInt(raw, -int32(amount.GetDecimals())), true
}

func rawSign(amount *streamv1.Amount) int {
	raw, ok := new(big.Int).SetString(amount.GetRaw(), 10)
	if !ok {
		return 0
	}
	return raw.Sign()
}
