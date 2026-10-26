package market

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/registry"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"
)

// recognized is the AssetFact recognition whose metadata chainstream-evm
// read from the contract; only its decimals are trusted.
const recognized = "recognized"

// lateTrade is a trade held back because the decimals of one of its tokens
// were unknown when its block was applied. It is replayed once they arrive.
type lateTrade struct {
	Height int64
	Time   int64
	// Trade is the proto-encoded source trade.
	Trade []byte
	// Waiting are the tokens whose decimals were unknown.
	Waiting []string
	// Prices are the USD closing prices at Height of the trade's priced
	// tokens, so a replayed trade is valued as of its own block.
	Prices map[string]decimal.Decimal `json:",omitempty"`
}

// readyTrade is a held or backfilled trade whose tokens all have decimals
// now.
type readyTrade struct {
	key    string
	held   lateTrade
	source *streamv1.Trade
	// hops are the graph hops of the tokens in held.Prices, when known.
	hops map[string]uint8
}

// lateKey orders held trades by height, so the oldest expire first.
func lateKey(height int64, trade *streamv1.Trade) string {
	return fmt.Sprintf("%012d|%s|%d|%d", height, normalize(trade.GetTransactionHash()),
		trade.GetLogIndex(), trade.GetOperationIndex())
}

// learn records token's decimals and adds token to learned when they were
// unknown before.
func (processor *Processor) learn(height int64, token string, value uint32, learned map[string]struct{}) {
	known, ok := processor.decimals.get(token)
	if !ok {
		learned[token] = struct{}{}
	}
	if !ok || known != value {
		processor.decimals.set(height, token, value)
	}
}

// learnAssets takes decimals from the metadata of recognized assets, which
// can arrive blocks after the asset's first trades.
func (processor *Processor) learnAssets(height int64, block *streamv1.BlockEvent, learned map[string]struct{}) {
	for _, fact := range block.GetAssets() {
		if fact.GetRecognition() != recognized {
			continue
		}
		token := normalize(fact.GetAddress())
		if _, _, value := registry.ParseMetadata(fact.GetMetadataJson()); value != nil && token != "" {
			processor.learn(height, token, uint32(*value), learned)
		}
	}
}

// decimalsOf returns an amount's decimals, falling back to what the
// processor learned from earlier blocks.
func (processor *Processor) decimalsOf(amount *streamv1.Amount) (uint32, bool) {
	if amount.GetDecimalsKnown() {
		return amount.GetDecimals(), true
	}
	return processor.decimals.get(normalize(amount.GetAsset()))
}

func (processor *Processor) amountOf(amount *streamv1.Amount) (decimal.Decimal, bool) {
	value, ok := processor.decimalsOf(amount)
	if !ok {
		return decimal.Decimal{}, false
	}
	return rawValue(amount.GetRaw(), value)
}

// hold keeps a trade whose decimals are unknown for replay, unless holding
// is off or a token it waits on already has LateTradesPerToken held trades.
func (processor *Processor) hold(height, blockTime int64, source *streamv1.Trade) {
	if processor.options.LateTradeBlocks <= 0 {
		return
	}
	held := lateTrade{Height: height, Time: blockTime, Prices: make(map[string]decimal.Decimal)}
	for _, amount := range source.GetAmounts() {
		token := normalize(amount.GetAsset())
		if _, ok := processor.decimalsOf(amount); !ok {
			if !slices.Contains(held.Waiting, token) {
				held.Waiting = append(held.Waiting, token)
			}
			continue
		}
		if price, ok := processor.priceOf(token); ok {
			held.Prices[token] = price.USD
		}
	}
	if len(held.Waiting) == 0 {
		return
	}
	for _, token := range held.Waiting {
		if len(processor.lateByToken[token]) >= processor.options.LateTradesPerToken {
			return
		}
	}
	key := lateKey(height, source)
	if _, exists := processor.late.get(key); exists {
		return
	}
	raw, err := proto.Marshal(source)
	if err != nil {
		return
	}
	held.Trade = raw
	processor.late.set(height, key, held)
	processor.indexLate(key, held.Waiting)
	processor.lateOrder = append(processor.lateOrder, key)
}

// ready returns, in chain order, the held trades that tokens just learned
// make complete.
func (processor *Processor) ready(learned map[string]struct{}) []readyTrade {
	keys := make(map[string]struct{})
	for token := range learned {
		for key := range processor.lateByToken[token] {
			keys[key] = struct{}{}
		}
	}
	var ready []readyTrade
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		held, ok := processor.late.get(key)
		if !ok || !processor.complete(held) {
			continue
		}
		source := &streamv1.Trade{}
		if err := proto.Unmarshal(held.Trade, source); err != nil {
			continue
		}
		ready = append(ready, readyTrade{key: key, held: held, source: source})
	}
	sortReady(ready)
	return ready
}

// sortReady puts ready trades in chain order.
func sortReady(ready []readyTrade) {
	slices.SortStableFunc(ready, func(left, right readyTrade) int {
		return cmp.Or(
			cmp.Compare(left.held.Height, right.held.Height),
			cmp.Compare(left.source.GetLogIndex(), right.source.GetLogIndex()),
			cmp.Compare(left.source.GetOperationIndex(), right.source.GetOperationIndex()),
		)
	})
}

func (processor *Processor) complete(held lateTrade) bool {
	for _, token := range held.Waiting {
		if _, ok := processor.decimals.get(token); !ok {
			return false
		}
	}
	return true
}

// seedPools gives pools that only held trades have touched the state from
// their latest held trade, so the block reprices through them.
func (processor *Processor) seedPools(height int64, ready []readyTrade, touched map[string]struct{}) {
	latest := make(map[string]*streamv1.Trade)
	for _, trade := range ready {
		latest[normalize(trade.source.GetPoolId())] = trade.source
	}
	for _, id := range slices.Sorted(maps.Keys(latest)) {
		if _, exists := processor.pools.get(id); exists {
			continue
		}
		if next, ok := observePool(pool{}, latest[id], processor.decimals.get); ok {
			processor.setPool(height, id, next, false, touched)
		}
	}
}

// replay emits ready trades valued at their own block's prices and builds
// their candles. A candle key that already has a bucket is left alone: its
// trades all came after the decimals arrived, so a held trade would land
// before them. It returns every bucket it built per key, oldest first.
//
// A trade quoted in a token whose decimals just arrived is valued at that
// token's recomputed price, and a swap of such a token is checked for
// outliers against it.
func (processor *Processor) replay(height int64, output *storage.BlockOutput, ready []readyTrade, past pastPrices, touched map[candleKey]struct{}) map[candleKey][]storage.Candle {
	built := make(map[candleKey][]storage.Candle)
	for _, item := range ready {
		processor.late.remove(height, item.key)
		processor.unindexLate(item.key, item.held.Waiting)
		trade, status := processor.orient(height, item.source)
		if status != orientOK {
			continue
		}
		trade.ChainID, trade.BlockHeight = output.ChainID, item.held.Height
		trade.BlockTime = time.Unix(item.held.Time, 0).UTC()
		quoteUSD, priced := item.held.Prices[trade.QuoteToken]
		if !priced {
			if reference, ok := past.at(processor.options.Catalog.Canonical(trade.QuoteToken), item.held.Height); ok {
				quoteUSD, priced = reference.USD, true
			}
		}
		if priced {
			valueTrade(&trade, quoteUSD)
			if reference, ok := past.at(processor.options.Catalog.Canonical(trade.BaseToken), item.held.Height); ok {
				trade.Outlier = processor.strays(trade, reference, true)
			}
		}
		output.Trades = append(output.Trades, trade)
		if !priced {
			continue
		}
		state, _ := observePool(pool{}, item.source, processor.decimals.get)
		liquidity := reserveUSD(processor.options.Catalog, state, trade.QuoteToken, quoteUSD)
		for _, subject := range processor.candleSubjects(trade, liquidity) {
			for _, interval := range processor.options.Intervals {
				key := candleKey{Subject: subject.Subject, Key: subject.Key, Interval: interval}
				buckets := built[key]
				if len(buckets) == 0 {
					if _, exists := processor.candles.get(key); exists {
						continue
					}
					built[key] = []storage.Candle{updateCandle(storage.Candle{}, false, trade, key, height)}
					continue
				}
				last := buckets[len(buckets)-1]
				next := updateCandle(last, true, trade, key, height)
				if next.OpenTime.Equal(last.OpenTime) {
					buckets[len(buckets)-1] = next
				} else {
					buckets = append(buckets, next)
				}
				built[key] = buckets
			}
		}
	}
	for _, key := range slices.SortedFunc(maps.Keys(built), compareCandleKeys) {
		buckets := built[key]
		processor.candles.set(height, key, buckets[len(buckets)-1])
		touched[key] = struct{}{}
	}
	return built
}

// expireLate drops held trades older than LateTradeBlocks.
func (processor *Processor) expireLate(height int64) {
	cutoff := height - processor.options.LateTradeBlocks
	for len(processor.lateOrder) > 0 {
		key := processor.lateOrder[0]
		held, ok := processor.late.get(key)
		if ok && held.Height > cutoff {
			return
		}
		processor.lateOrder = processor.lateOrder[1:]
		if ok {
			processor.late.remove(height, key)
			processor.unindexLate(key, held.Waiting)
		}
	}
	processor.lateOrder = nil
}

func (processor *Processor) indexLate(key string, tokens []string) {
	for _, token := range tokens {
		keys := processor.lateByToken[token]
		if keys == nil {
			keys = make(map[string]struct{})
			processor.lateByToken[token] = keys
		}
		keys[key] = struct{}{}
	}
}

func (processor *Processor) unindexLate(key string, tokens []string) {
	for _, token := range tokens {
		delete(processor.lateByToken[token], key)
		if len(processor.lateByToken[token]) == 0 {
			delete(processor.lateByToken, token)
		}
	}
}

// rebuildLate rebuilds the held-trade indexes from the journal after a
// restore or a revert.
func (processor *Processor) rebuildLate() {
	processor.lateByToken = make(map[string]map[string]struct{})
	processor.lateOrder = slices.Sorted(maps.Keys(processor.late.values))
	for _, key := range processor.lateOrder {
		processor.indexLate(key, processor.late.values[key].Waiting)
	}
}
