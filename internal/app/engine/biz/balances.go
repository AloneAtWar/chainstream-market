package biz

import (
	"maps"
	"math/big"
	"slices"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/AloneAtWar/oh-my-blockchain/chain/evm/feature/dex/mechanism/ladder"
	"github.com/AloneAtWar/oh-my-blockchain/chain/evm/feature/dex/mechanism/stableswap"
	"github.com/AloneAtWar/oh-my-blockchain/chain/evm/feature/dex/mechanism/weighted"
	"github.com/shopspring/decimal"
)

// Curve and Balancer pools hold more than two tokens and keep fees and rates
// that events never show, so chainstream-evm reads each touched pool's real
// balances and curve parameters at the end of the block and publishes them.
// Each snapshot replaces the pool's record; prices and depths between every
// pair of its tokens follow from the record alone.

// wad is the 1e18 scale of fees, weights and rates.
var wad = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

// applyBalances replaces the records of the pools the block snapshots and
// marks their tokens, and the tokens they price by rate, for repricing.
func (processor *Processor) applyBalances(height int64, block *streamv1.BlockEvent, touched map[string]struct{}) {
	for _, snapshot := range block.GetPoolBalances() {
		id := normalize(snapshot.GetPoolId())
		tokens := make([]string, len(snapshot.GetTokens()))
		for index, token := range snapshot.GetTokens() {
			tokens[index] = normalize(token)
		}
		if id == "" || len(tokens) < 2 || len(snapshot.GetBalances()) != len(tokens) {
			continue
		}
		record := model.BalancePool{
			Protocol: snapshot.GetProtocol(), PoolAddress: normalize(snapshot.GetPoolAddress()),
			SnapshotHeight: snapshot.GetSnapshotHeight(), Tokens: tokens,
			Balances: slices.Clone(snapshot.GetBalances()), Curve: snapshot.GetCurve(), Reason: snapshot.GetReason(),
			Fee: snapshot.GetFee(), DynamicFee: snapshot.GetDynamicFee(), Weights: slices.Clone(snapshot.GetWeights()),
			Amp: snapshot.GetAmp(), AmpPrecision: snapshot.GetAmpPrecision(), Rates: slices.Clone(snapshot.GetRates()),
		}
		for _, quotes := range snapshot.GetLadders() {
			record.Ladders = append(record.Ladders, model.QuoteLadder{
				In: int(quotes.GetTokenIn()), Out: int(quotes.GetTokenOut()),
				Amounts: slices.Clone(quotes.GetAmounts()), Quotes: slices.Clone(quotes.GetQuotes()),
			})
		}
		if len(record.Ladders) > 0 {
			record.LadderHeight = snapshot.GetSnapshotHeight()
		} else if previous, ok := processor.balances.get(id); ok && previous.Curve == record.Curve &&
			slices.Equal(previous.Tokens, record.Tokens) {
			// Ladders are read every few blocks; keep the last ones.
			record.Ladders, record.LadderHeight = previous.Ladders, previous.LadderHeight
		}
		for _, derived := range snapshot.GetDerived() {
			underlyings := make([]string, len(derived.GetUnderlyings()))
			for index, underlying := range derived.GetUnderlyings() {
				underlyings[index] = normalize(underlying)
			}
			record.Derived = append(record.Derived, model.DerivedRate{
				Token: normalize(derived.GetToken()), Underlyings: underlyings, Rate: derived.GetRate(),
			})
		}
		processor.balances.set(height, id, record)
		delete(processor.balanceEdges, id)
		processor.indexBalancePool(id, record)
		for _, token := range record.Tokens {
			touched[processor.options.Catalog.Canonical(token)] = struct{}{}
		}
		for _, derived := range record.Derived {
			touched[processor.options.Catalog.Canonical(derived.Token)] = struct{}{}
		}
	}
}

// indexBalancePool lists a pool under each of its tokens and each token it
// prices by rate. The indexes only grow; lookups skip stale entries.
func (processor *Processor) indexBalancePool(id string, record model.BalancePool) {
	for _, member := range record.Tokens {
		token := processor.options.Catalog.Canonical(member)
		if !slices.Contains(processor.tokenBalancePools[token], id) {
			processor.tokenBalancePools[token] = append(processor.tokenBalancePools[token], id)
		}
	}
	for _, derived := range record.Derived {
		token := processor.options.Catalog.Canonical(derived.Token)
		if !slices.Contains(processor.derivedPools[token], id) {
			processor.derivedPools[token] = append(processor.derivedPools[token], id)
		}
	}
}

// rebuildBalances indexes the balance journal after a restore or revert.
func (processor *Processor) rebuildBalances() {
	processor.tokenBalancePools = make(map[string][]string)
	processor.derivedPools = make(map[string][]string)
	processor.balanceEdges = make(map[string]*poolEdges)
	for _, id := range slices.Sorted(maps.Keys(processor.balances.values)) {
		processor.indexBalancePool(id, processor.balances.values[id])
	}
}

// poolEdges caches what a pool record implies for each ordered token pair.
type poolEdges struct {
	// xp and d are a stable pool's scaled balances and invariant.
	xp []*big.Int
	d  *big.Int
	// pairs are keyed by [sold, bought] positions.
	pairs map[[2]int]balanceEdge
}

// balanceEdge is what a pool says about selling Tokens[sold] for
// Tokens[bought], in human units.
type balanceEdge struct {
	// price is one sold token in bought tokens.
	price    decimal.Decimal
	hasPrice bool
	// depth is the bought token the pool pays out before the bought token's
	// price rises by the pool's band.
	depth    decimal.Decimal
	hasDepth bool
}

// balanceOf returns a pool token's balance in human units.
func (processor *Processor) balanceOf(record model.BalancePool, position int) (decimal.Decimal, bool) {
	decimals, ok := processor.tokenDecimals(record.Tokens[position])
	raw, okRaw := parseRaw(record.Balances[position])
	if !ok || !okRaw {
		return decimal.Decimal{}, false
	}
	return decimal.NewFromBigInt(raw, -int32(decimals)), true
}

// tokenDecimals returns a token's decimals; the native coin always has 18.
func (processor *Processor) tokenDecimals(token string) (uint32, bool) {
	if decimals, ok := processor.decimals.get(token); ok {
		return decimals, true
	}
	if token == asset.NativeAddress {
		return 18, true
	}
	return 0, false
}

// edge returns what pool id says about selling Tokens[sold] for
// Tokens[bought], computing it once per record.
func (processor *Processor) edge(id string, record model.BalancePool, sold, bought int) balanceEdge {
	cache := processor.balanceEdges[id]
	if cache == nil {
		cache = &poolEdges{pairs: make(map[[2]int]balanceEdge)}
		if record.Curve == model.CurveStable {
			cache.xp, cache.d = stableInvariant(record)
		}
		processor.balanceEdges[id] = cache
	}
	key := [2]int{sold, bought}
	if value, ok := cache.pairs[key]; ok {
		return value
	}
	value := processor.computeEdge(record, cache, sold, bought)
	cache.pairs[key] = value
	return value
}

func (processor *Processor) computeEdge(record model.BalancePool, cache *poolEdges, sold, bought int) balanceEdge {
	var result balanceEdge
	dSold, okSold := processor.tokenDecimals(record.Tokens[sold])
	dBought, okBought := processor.tokenDecimals(record.Tokens[bought])
	if !okSold || !okBought {
		return result
	}
	// A raw price (bought raw units per sold raw unit) becomes human by the
	// decimals gap.
	human := func(raw *big.Float) (decimal.Decimal, bool) {
		value, err := decimal.NewFromString(raw.Text('g', 40))
		if err != nil || !value.IsPositive() {
			return decimal.Decimal{}, false
		}
		return value.Shift(int32(dSold) - int32(dBought)).Round(precision), true
	}
	rawDepth := func(raw *big.Int) (decimal.Decimal, bool) {
		if raw == nil || raw.Sign() < 0 {
			return decimal.Decimal{}, false
		}
		return decimal.NewFromBigInt(raw, -int32(dBought)), true
	}
	bps := processor.depthBPS(feeTier(record))
	switch record.Curve {
	case model.CurveStable:
		if cache.d == nil {
			return result
		}
		amp, ok := stableAmp(record)
		if !ok {
			return result
		}
		// SpotPrice(i=bought, j=sold) is a sold xp unit in bought xp units.
		spot, err := stableswap.SpotPrice(cache.xp, amp, cache.d, bought, sold)
		rateSold, okRateSold := parseRaw(record.Rates[sold])
		rateBought, okRateBought := parseRaw(record.Rates[bought])
		if err == nil && okRateSold && okRateBought && rateBought.Sign() > 0 {
			raw := new(big.Float).SetPrec(256).Mul(spot, new(big.Float).SetPrec(256).SetInt(rateSold))
			raw.Quo(raw, new(big.Float).SetPrec(256).SetInt(rateBought))
			result.price, result.hasPrice = human(raw)
		}
		if depth, err := stableswap.Depth(cache.xp, amp, sold, bought, bps); err == nil && okRateBought && rateBought.Sign() > 0 {
			raw := new(big.Int).Mul(depth, wad)
			result.depth, result.hasDepth = rawDepth(raw.Quo(raw, rateBought))
		}
	case model.CurveWeighted:
		balanceSold, okBalanceSold := parseRaw(record.Balances[sold])
		balanceBought, okBalanceBought := parseRaw(record.Balances[bought])
		weightSold, okWeightSold := parseRaw(at(record.Weights, sold))
		weightBought, okWeightBought := parseRaw(at(record.Weights, bought))
		if !okBalanceSold || !okBalanceBought || !okWeightSold || !okWeightBought ||
			balanceSold.Sign() <= 0 || weightBought.Sign() <= 0 {
			return result
		}
		// Rates cancel out of a weighted pool's price: it is
		// (B_bought / w_bought) / (B_sold / w_sold) in raw units.
		raw := new(big.Float).SetPrec(256).SetInt(new(big.Int).Mul(balanceBought, weightSold))
		raw.Quo(raw, new(big.Float).SetPrec(256).SetInt(new(big.Int).Mul(balanceSold, weightBought)))
		result.price, result.hasPrice = human(raw)
		if depth, err := weighted.Depth(balanceBought, weightSold, weightBought, bps); err == nil {
			result.depth, result.hasDepth = rawDepth(depth)
		}
	case model.CurveCrypto:
		amounts, quotes, ok := ladderOf(record, sold, bought)
		if !ok {
			return result
		}
		// The first rung is close to the marginal price after the fee.
		raw := new(big.Float).SetPrec(256).SetInt(quotes[0])
		raw.Quo(raw, new(big.Float).SetPrec(256).SetInt(amounts[0]))
		if fee, ok := parseRaw(record.Fee); ok && fee.Cmp(wad) < 0 {
			kept := new(big.Float).SetPrec(256).SetInt(new(big.Int).Sub(wad, fee))
			raw.Quo(raw, kept.Quo(kept, new(big.Float).SetPrec(256).SetInt(wad)))
		}
		result.price, result.hasPrice = human(raw)
		if depth, _, err := ladder.Depth(amounts, quotes, bps); err == nil {
			result.depth, result.hasDepth = rawDepth(depth)
		}
	}
	return result
}

// stableInvariant scales a stable pool's balances by its rates and returns
// them with their invariant, or nil when the record cannot give one.
func stableInvariant(record model.BalancePool) ([]*big.Int, *big.Int) {
	amp, ok := stableAmp(record)
	if !ok || len(record.Rates) != len(record.Tokens) {
		return nil, nil
	}
	xp := make([]*big.Int, len(record.Tokens))
	for index := range record.Tokens {
		balance, okBalance := parseRaw(record.Balances[index])
		rate, okRate := parseRaw(record.Rates[index])
		if !okBalance || !okRate {
			return nil, nil
		}
		xp[index] = new(big.Int).Quo(new(big.Int).Mul(balance, rate), wad)
	}
	d, err := stableswap.D(xp, amp)
	if err != nil {
		return nil, nil
	}
	return xp, d
}

func stableAmp(record model.BalancePool) (stableswap.Amp, bool) {
	value, okValue := parseRaw(record.Amp)
	precision, okPrecision := parseRaw(record.AmpPrecision)
	if !okValue || !okPrecision || value.Sign() <= 0 || precision.Sign() <= 0 {
		return stableswap.Amp{}, false
	}
	return stableswap.Amp{Value: value, Precision: precision}, true
}

// derivedQuote prices token by the rate a pool reports for it against the
// cheapest of its priced underlyings. A rate is exact, so it is preferred to
// any market price.
func (processor *Processor) derivedQuote(height int64, token string) (tokenPrice, bool) {
	var best tokenPrice
	found := false
	for _, id := range processor.derivedPools[token] {
		record, ok := processor.balances.get(id)
		if !ok {
			continue
		}
		for _, derived := range record.Derived {
			if processor.options.Catalog.Canonical(derived.Token) != token {
				continue
			}
			rate, ok := parseRaw(derived.Rate)
			if !ok || rate.Sign() <= 0 {
				continue
			}
			var cheapest tokenPrice
			via, priced := "", false
			for _, member := range derived.Underlyings {
				underlying := processor.options.Catalog.Canonical(member)
				price, ok := processor.priceOf(underlying)
				if !ok || underlying == token || price.Via == token || price.Hops >= processor.options.MaxHops {
					continue
				}
				if !priced || price.USD.LessThan(cheapest.USD) {
					cheapest, via, priced = price, underlying, true
				}
			}
			if !priced {
				continue
			}
			usd := decimal.NewFromBigInt(rate, -18).Mul(cheapest.USD).Round(precision)
			liquidity := decimal.Zero
			if position := slices.Index(record.Tokens, normalize(derived.Token)); position >= 0 {
				if balance, ok := processor.balanceOf(record, position); ok {
					liquidity = balance.Mul(usd).Round(precision)
				}
			}
			if found && (liquidity.LessThan(best.LiquidityUSD) || (liquidity.Equal(best.LiquidityUSD) && id > best.Pool)) {
				continue
			}
			best = tokenPrice{
				USD: usd, Hops: cheapest.Hops + 1, Via: via, Pool: id, LiquidityUSD: liquidity, Height: height,
			}
			found = true
		}
	}
	return best, found
}

// balanceQuote prices token through the balance pool whose priced
// counterpart balance is worth the most in USD, competing with best.
func (processor *Processor) balanceQuote(height int64, token string, best tokenPrice, found bool) (tokenPrice, bool) {
	for _, id := range processor.tokenBalancePools[token] {
		record, ok := processor.balances.get(id)
		if !ok {
			continue
		}
		for position, member := range record.Tokens {
			if processor.options.Catalog.Canonical(member) != token {
				continue
			}
			for other, otherMember := range record.Tokens {
				counterToken := processor.options.Catalog.Canonical(otherMember)
				if other == position || counterToken == token {
					continue
				}
				counterpart, ok := processor.priceOf(counterToken)
				if !ok || counterpart.Via == token || counterpart.Hops >= processor.options.MaxHops {
					continue
				}
				balance, ok := processor.balanceOf(record, other)
				if !ok {
					continue
				}
				liquidity := balance.Mul(counterpart.USD).Round(precision)
				if liquidity.LessThan(processor.options.MinLiquidityUSD) {
					continue
				}
				relative, ok := processor.balancePrice(id, record, position, other)
				if !ok {
					continue
				}
				if found && (liquidity.LessThan(best.LiquidityUSD) || (liquidity.Equal(best.LiquidityUSD) && id > best.Pool)) {
					continue
				}
				best = tokenPrice{
					USD: relative.Mul(counterpart.USD).Round(precision), Hops: counterpart.Hops + 1,
					Via: counterToken, Pool: id, LiquidityUSD: liquidity, Height: height,
				}
				found = true
			}
		}
	}
	return best, found
}

// balancePrice returns one Tokens[sold] in Tokens[bought] from the pool's
// curve, or from its latest swap of that pair when the curve gives none.
func (processor *Processor) balancePrice(id string, record model.BalancePool, sold, bought int) (decimal.Decimal, bool) {
	if edge := processor.edge(id, record, sold, bought); edge.hasPrice {
		return edge.price, true
	}
	last, ok := processor.pools.get(id)
	if !ok {
		return decimal.Decimal{}, false
	}
	switch {
	case last.Tokens[0] == record.Tokens[sold] && last.Tokens[1] == record.Tokens[bought]:
		return last.priceOf(0)
	case last.Tokens[1] == record.Tokens[sold] && last.Tokens[0] == record.Tokens[bought]:
		return last.priceOf(1)
	}
	return decimal.Decimal{}, false
}

// feeTier converts the 1e18 fee into hundredths of a basis point, with
// model.DynamicFeeFlag for a dynamic fee.
func feeTier(record model.BalancePool) uint32 {
	if record.DynamicFee {
		return model.DynamicFeeFlag
	}
	fee, ok := parseRaw(record.Fee)
	if !ok {
		return 0
	}
	tier := new(big.Int).Quo(fee, big.NewInt(1_000_000_000_000))
	if !tier.IsUint64() || tier.Uint64() >= model.DynamicFeeFlag {
		return model.DynamicFeeFlag - 1
	}
	return uint32(tier.Uint64())
}

// ladderOf returns the pool's quotes for selling Tokens[sold] for
// Tokens[bought].
func ladderOf(record model.BalancePool, sold, bought int) ([]*big.Int, []*big.Int, bool) {
	for _, quotes := range record.Ladders {
		if quotes.In != sold || quotes.Out != bought || len(quotes.Amounts) < 2 || len(quotes.Amounts) != len(quotes.Quotes) {
			continue
		}
		amounts, outputs := make([]*big.Int, len(quotes.Amounts)), make([]*big.Int, len(quotes.Quotes))
		for index := range quotes.Amounts {
			amount, okAmount := parseRaw(quotes.Amounts[index])
			output, okOutput := parseRaw(quotes.Quotes[index])
			if !okAmount || !okOutput || amount.Sign() <= 0 || output.Sign() <= 0 {
				return nil, nil, false
			}
			amounts[index], outputs[index] = amount, output
		}
		return amounts, outputs, true
	}
	return nil, nil, false
}

func parseRaw(value string) (*big.Int, bool) {
	if value == "" {
		return nil, false
	}
	parsed, ok := new(big.Int).SetString(value, 10)
	if !ok || parsed.Sign() < 0 {
		return nil, false
	}
	return parsed, true
}

func at(values []string, index int) string {
	if index < 0 || index >= len(values) {
		return ""
	}
	return values[index]
}
