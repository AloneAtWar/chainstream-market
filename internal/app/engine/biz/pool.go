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
	// reserves are actual reserves for constant-product pools and virtual
	// reserves at the current price for concentrated-liquidity pools.
	Reserves    [2]decimal.Decimal
	HasReserves bool
	// price is tokens[1] per tokens[0].
	Price    decimal.Decimal
	HasPrice bool
}

// observePool returns the pool state after trade. decimals reports known
// token decimals. ok is false when the trade carries nothing new.
func observePool(prev pool, trade *streamv1.Trade, decimals func(string) (uint32, bool)) (pool, bool) {
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
	case state.GetSqrtPriceX96() != "" && state.GetLiquidity() != "":
		reserves, price, ok := concentratedState(state.GetSqrtPriceX96(), state.GetLiquidity(), d0, d1)
		if !ok {
			return prev, false
		}
		next.Reserves, next.Price = reserves, price
		next.HasReserves, next.HasPrice = true, true
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
