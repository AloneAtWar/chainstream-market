package market

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

// missedSweep is how often, in blocks, tokens that stopped missing their
// decimals are forgotten.
const missedSweep = 256

// missedToken spans the heights that had a trade waiting on a token's
// decimals.
type missedToken struct {
	First int64
	Last  int64
}

func (processor *Processor) backfilling() bool {
	return processor.options.History != nil && processor.options.BackfillBlocks > 0
}

// miss records the tokens a trade waits on, so the block that brings their
// decimals knows which heights to backfill.
func (processor *Processor) miss(height int64, source *streamv1.Trade) {
	if !processor.backfilling() {
		return
	}
	for _, amount := range source.GetAmounts() {
		if _, ok := processor.decimalsOf(amount); ok || rawSign(amount) == 0 {
			continue
		}
		token := normalize(amount.GetAsset())
		entry, ok := processor.missed.get(token)
		switch {
		case !ok:
			processor.missed.set(height, token, missedToken{First: height, Last: height})
		case entry.Last < height:
			entry.Last = height
			processor.missed.set(height, token, entry)
		}
	}
}

// expireMissed forgets tokens that have not missed their decimals for
// BackfillBlocks, which a backfill could no longer reach.
func (processor *Processor) expireMissed(height int64) {
	if height%missedSweep != 0 || len(processor.missed.values) == 0 {
		return
	}
	cutoff := height - processor.options.BackfillBlocks
	for _, token := range slices.Sorted(maps.Keys(processor.missed.values)) {
		if processor.missed.values[token].Last <= cutoff {
			processor.missed.remove(height, token)
		}
	}
}

// backfill adds to ready every stored trade of the tokens whose decimals
// this block brought, back to BackfillBlocks, that held trades do not cover:
// trades past LateTradeBlocks or LateTradesPerToken, or all of them when
// holding is off. Each is valued with the prices market-engine stored for
// its block. A trade that still waits on another token is left for that
// token's backfill.
//
// When the raw store has not caught up with the heights to read, only the
// held trades are replayed.
func (processor *Processor) backfill(ctx context.Context, height int64, chainID uint64, learned map[string]struct{}, ready []readyTrade) ([]readyTrade, map[string][]storage.PricePoint, error) {
	if !processor.backfilling() {
		return ready, nil, nil
	}
	var tokens []string
	from, through := height, int64(0)
	for _, token := range slices.Sorted(maps.Keys(learned)) {
		entry, ok := processor.missed.get(token)
		if !ok {
			continue
		}
		processor.missed.remove(height, token)
		tokens = append(tokens, token)
		from = min(from, max(entry.First, height-processor.options.BackfillBlocks))
		through = max(through, entry.Last)
	}
	if len(tokens) == 0 || through < from {
		return ready, nil, nil
	}
	raws, ok, err := processor.options.History.RawTrades(ctx, chainID, tokens, from, through)
	if err != nil {
		return nil, nil, fmt.Errorf("market: backfill %s: %w", strings.Join(tokens, ","), err)
	}
	if !ok {
		processor.options.Logger.Warn("raw trades not stored yet, replaying held trades only",
			"chain_id", chainID, "height", height, "tokens", tokens, "through", through)
		return ready, nil, nil
	}

	waiting := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		waiting[token] = struct{}{}
	}
	held := make(map[string]struct{}, len(ready))
	for _, item := range ready {
		held[item.key] = struct{}{}
	}
	catalog := processor.options.Catalog
	counterparts := make(map[string]struct{})
	var added []readyTrade
	for _, raw := range raws {
		source := streamTrade(raw.RawTrade)
		key := lateKey(raw.Height, source)
		if _, ok := held[key]; ok || !processor.missedOn(source, waiting) {
			continue
		}
		added = append(added, readyTrade{
			key: key, source: source,
			held: lateTrade{Height: raw.Height, Time: raw.BlockTime.Unix(), Prices: make(map[string]decimal.Decimal)},
		})
		for _, amount := range source.GetAmounts() {
			if token := catalog.Canonical(amount.GetAsset()); !isLearned(catalog, learned, token) {
				counterparts[token] = struct{}{}
			}
		}
	}
	if len(added) == 0 {
		return ready, nil, nil
	}
	series, err := processor.options.History.PriceHistory(ctx, chainID, slices.Sorted(maps.Keys(counterparts)), from, through)
	if err != nil {
		return nil, nil, fmt.Errorf("market: backfill prices: %w", err)
	}
	for index := range added {
		item := &added[index]
		item.hops = make(map[string]uint8)
		for _, amount := range item.source.GetAmounts() {
			token := normalize(amount.GetAsset())
			canonical := catalog.Canonical(token)
			if isLearned(catalog, learned, canonical) {
				continue
			}
			if usd, hops, ok := processor.priceAt(series, canonical, item.held.Height); ok {
				item.held.Prices[token], item.hops[token] = usd, hops
			}
		}
	}
	processor.options.Logger.Info("backfilled late token trades", "chain_id", chainID, "height", height,
		"tokens", tokens, "from", from, "through", through, "trades", len(added), "held", len(ready))
	ready = append(ready, added...)
	sortReady(ready)
	return ready, series, nil
}

// missedOn reports whether a trade had a nonzero leg in one of tokens, so
// it waited on their decimals instead of being applied at its block.
func (processor *Processor) missedOn(source *streamv1.Trade, tokens map[string]struct{}) bool {
	missed := false
	for _, amount := range source.GetAmounts() {
		if rawSign(amount) == 0 {
			continue
		}
		if _, ok := tokens[normalize(amount.GetAsset())]; ok {
			missed = true
		}
	}
	return missed
}

func isLearned(catalog asset.Catalog, learned map[string]struct{}, canonical string) bool {
	for token := range learned {
		if catalog.Canonical(token) == canonical {
			return true
		}
	}
	return false
}

// priceAt returns token's stored price at the close of height: its latest
// stored price at or below height, or an anchor's peg when none is stored.
func (processor *Processor) priceAt(series map[string][]storage.PricePoint, token string, height int64) (decimal.Decimal, uint8, bool) {
	points := series[token]
	index := sort.Search(len(points), func(index int) bool { return points[index].Height > height })
	if index > 0 {
		return points[index-1].USD, points[index-1].Hops, true
	}
	if peg, ok := processor.options.Catalog.Anchor(token); ok {
		return peg, 0, true
	}
	return decimal.Decimal{}, 0, false
}

// streamTrade rebuilds the published trade from ch-loader's copy.
func streamTrade(raw storage.RawTrade) *streamv1.Trade {
	trade := &streamv1.Trade{
		TransactionHash: raw.TransactionHash, TransactionIndex: raw.TransactionIndex,
		OperationIndex: raw.OperationIndex, Protocol: raw.Protocol, DexAddress: raw.DexAddress,
		PoolId: raw.PoolID, Sender: raw.Sender, LogIndex: raw.LogIndex, Trader: raw.Trader,
		Recipient: raw.Recipient,
		Action:    streamv1.TradeAction(streamv1.TradeAction_value["TRADE_ACTION_"+strings.ToUpper(raw.Action)]),
	}
	for _, amount := range raw.Amounts {
		trade.Amounts = append(trade.Amounts, &streamv1.Amount{
			Asset: amount.Asset, Raw: amount.Raw, Value: amount.Value,
			Decimals: amount.Decimals, DecimalsKnown: amount.DecimalsKnown,
		})
	}
	if len(raw.ReservesAfter) > 0 || raw.SqrtPriceX96 != "" || raw.Liquidity != "" || raw.Tick != "" || raw.ActiveBinID != nil {
		trade.PoolState = &streamv1.PoolState{
			ReservesAfter: raw.ReservesAfter, SqrtPriceX96: raw.SqrtPriceX96,
			Liquidity: raw.Liquidity, Tick: raw.Tick, ActiveBinId: raw.ActiveBinID,
		}
	}
	return trade
}

// pastKey is a token's price at one block.
type pastKey struct {
	token  string
	height int64
}

// pastPrices are the recomputed prices of tokens whose decimals a block
// brought, at the earlier blocks of their replayed trades.
type pastPrices struct {
	prices []storage.TokenPrice
	byKey  map[pastKey]tokenPrice
}

func (past pastPrices) at(token string, height int64) (tokenPrice, bool) {
	price, ok := past.byKey[pastKey{token, height}]
	return price, ok
}

// pastQuote is a counterpart's USD price and graph hops at a past block.
type pastQuote struct {
	usd  decimal.Decimal
	hops uint8
}

// pastPrices prices each token whose decimals just arrived at every earlier
// block where a ready trade touched one of its pools, the way reprice would
// have: through the pool, as of that block's close, whose priced
// counterpart reserve was worth the most. Pool states come from the ready
// trades, and counterpart prices from the stored series a backfill read, or
// else from the prices the trades carry. A counterpart whose decimals also
// just arrived has no past price, so it prices nothing.
func (processor *Processor) pastPrices(chainID uint64, ready []readyTrade, series map[string][]storage.PricePoint, learned map[string]struct{}) pastPrices {
	past := pastPrices{byKey: make(map[pastKey]tokenPrice)}
	catalog := processor.options.Catalog
	subjects := make(map[string]struct{}, len(learned))
	for token := range learned {
		subjects[catalog.Canonical(token)] = struct{}{}
	}
	states := make(map[string]pool)
	poolsOf := make(map[string][]string)
	quotes := make(map[string]pastQuote)
	for start := 0; start < len(ready); {
		height := ready[start].held.Height
		touched := make(map[string]struct{})
		end := start
		for ; end < len(ready) && ready[end].held.Height == height; end++ {
			item := ready[end]
			for token, usd := range item.held.Prices {
				hops, ok := item.hops[token]
				if !ok {
					hops = processor.currentHops(catalog.Canonical(token))
				}
				quotes[catalog.Canonical(token)] = pastQuote{usd: usd, hops: hops}
			}
			id := normalize(item.source.GetPoolId())
			next, ok := observePool(states[id], item.source, processor.decimals.get)
			if !ok {
				continue
			}
			states[id] = next
			for _, member := range next.Tokens {
				token := catalog.Canonical(member)
				if _, ok := subjects[token]; !ok {
					continue
				}
				touched[token] = struct{}{}
				if !slices.Contains(poolsOf[token], id) {
					poolsOf[token] = append(poolsOf[token], id)
				}
			}
		}
		blockTime := time.Unix(ready[start].held.Time, 0).UTC()
		quote := func(token string) (pastQuote, bool) {
			if _, stored := series[token]; stored {
				usd, hops, ok := processor.priceAt(series, token, height)
				return pastQuote{usd: usd, hops: hops}, ok
			}
			value, ok := quotes[token]
			return value, ok
		}
		for _, token := range slices.Sorted(maps.Keys(touched)) {
			best, ok := processor.pastQuote(token, poolsOf[token], states, quote, subjects)
			if !ok {
				continue
			}
			best.Height = height
			past.byKey[pastKey{token, height}] = best
			past.prices = append(past.prices, storage.TokenPrice{
				ChainID: chainID, Token: token, BlockHeight: height, BlockTime: blockTime,
				PriceUSD: best.USD, Source: storage.PriceSourceGraph, Hops: best.Hops, LiquidityUSD: best.LiquidityUSD,
			})
		}
		start = end
	}
	return past
}

// pastQuote is bestQuote over past pool states and counterpart prices.
func (processor *Processor) pastQuote(token string, pools []string, states map[string]pool, quote func(string) (pastQuote, bool), subjects map[string]struct{}) (tokenPrice, bool) {
	catalog := processor.options.Catalog
	var best tokenPrice
	found := false
	for _, id := range pools {
		state := states[id]
		if !state.HasReserves {
			continue
		}
		position, ok := state.index(catalog, token)
		if !ok {
			continue
		}
		other := catalog.Canonical(state.Tokens[1-position])
		if _, learning := subjects[other]; learning || other == token {
			continue
		}
		counterpart, ok := quote(other)
		if !ok || counterpart.hops >= processor.options.MaxHops {
			continue
		}
		liquidity := state.Reserves[1-position].Mul(counterpart.usd).Round(precision)
		if liquidity.LessThan(processor.options.MinLiquidityUSD) {
			continue
		}
		relative, ok := state.priceOf(position)
		if !ok {
			continue
		}
		if found && (liquidity.LessThan(best.LiquidityUSD) || (liquidity.Equal(best.LiquidityUSD) && id > best.Pool)) {
			continue
		}
		best = tokenPrice{
			USD: relative.Mul(counterpart.usd).Round(precision), Hops: counterpart.hops + 1,
			Via: other, Pool: id, LiquidityUSD: liquidity,
		}
		found = true
	}
	return best, found
}

// currentHops is a token's graph distance now, for a held trade's prices,
// which were stored without it.
func (processor *Processor) currentHops(token string) uint8 {
	if price, ok := processor.priceOf(token); ok {
		return price.Hops
	}
	return 0
}
