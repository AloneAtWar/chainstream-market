// Package market orients trades, prices tokens through a liquidity-weighted
// graph anchored on USD assets, and aggregates candles.
package market

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

// ErrReorgTooDeep means a revert reaches below the retained undo history.
var ErrReorgTooDeep = errors.New("market: reorg deeper than retained history")

type Options struct {
	Catalog Catalog
	// MinLiquidityUSD is the counterpart reserve value a pool needs before a
	// price can propagate through it.
	MinLiquidityUSD decimal.Decimal
	// CandleMinLiquidityUSD is the quote reserve value a pool needs before its
	// trades join a token's aggregated candles.
	CandleMinLiquidityUSD decimal.Decimal
	// MaxHops bounds how far from an anchor a price may propagate.
	MaxHops uint8
	// Intervals are the candle bucket sizes.
	Intervals []time.Duration
	// HistoryDepth is how many blocks of undo history to keep in addition to
	// everything above the finalized height.
	HistoryDepth int64
	// Feeds are the oracle aggregators trusted for USD prices. Updates from
	// any other address are ignored.
	Feeds []Feed
	// OracleDeviation is the largest relative gap allowed between a
	// feed-backed token's graph price and its oracle price before the oracle
	// price is used instead.
	OracleDeviation decimal.Decimal
}

// Feed is a trusted Chainlink-style aggregator answering in USD.
type Feed struct {
	Address  string
	Token    string
	Decimals uint8
}

// tokenPrice is a graph-derived USD price.
type tokenPrice struct {
	USD          decimal.Decimal
	Hops         uint8
	Via          string // counterpart token the price came through
	Pool         string
	LiquidityUSD decimal.Decimal
	Height       int64
	FromOracle   bool // the price is an oracle answer, not a graph price
}

// orientation fixes a pool's quote token once it is known.
type orientation struct {
	Quote string
	Fixed bool
}

type candleKey struct {
	Subject  storage.CandleSubject
	Key      string
	Interval time.Duration
}

// Processor is one chain's market state. It is not safe for concurrent use.
type Processor struct {
	options      Options
	decimals     *journal[string, uint32]
	pools        *journal[string, pool]
	prices       *journal[string, tokenPrice]
	orientations *journal[string, orientation]
	candles      *journal[candleKey, storage.Candle]
	// oracle holds the latest trusted oracle answer per canonical token.
	oracle *journal[string, decimal.Decimal]
	feeds  map[string]Feed
	// tokenPools indexes pools by canonical token. It only grows; pools that
	// a revert removed are skipped on lookup.
	tokenPools map[string][]string
	// prunedThrough is the highest height whose undo history was discarded.
	prunedThrough int64
	// reverted and revertTo record a revert since the last TakeDelta.
	reverted bool
	revertTo int64
}

func NewProcessor(options Options) (*Processor, error) {
	if options.MaxHops == 0 {
		return nil, errors.New("market: max hops must be positive")
	}
	if len(options.Intervals) == 0 {
		return nil, errors.New("market: at least one candle interval is required")
	}
	for _, interval := range options.Intervals {
		if interval < time.Second || interval%time.Second != 0 {
			return nil, fmt.Errorf("market: candle interval %s must be whole seconds", interval)
		}
	}
	if options.HistoryDepth <= 0 {
		return nil, errors.New("market: history depth must be positive")
	}
	if options.OracleDeviation.IsNegative() {
		return nil, errors.New("market: oracle deviation must not be negative")
	}
	feeds := make(map[string]Feed, len(options.Feeds))
	for _, feed := range options.Feeds {
		address := normalize(feed.Address)
		if address == "" || normalize(feed.Token) == "" {
			return nil, errors.New("market: feeds need an address and a token")
		}
		if _, duplicate := feeds[address]; duplicate {
			return nil, fmt.Errorf("market: duplicate feed %s", address)
		}
		feed.Token = options.Catalog.Canonical(feed.Token)
		feeds[address] = feed
	}
	return &Processor{
		options:      options,
		decimals:     newJournal[string, uint32](stringKeys(kindDecimals)),
		pools:        newJournal[string, pool](stringKeys(kindPools)),
		prices:       newJournal[string, tokenPrice](stringKeys(kindPrices)),
		orientations: newJournal[string, orientation](stringKeys(kindOrientations)),
		candles:      newJournal[candleKey, storage.Candle](candleKeys),
		oracle:       newJournal[string, decimal.Decimal](stringKeys(kindOracle)),
		feeds:        feeds,
		tokenPools:   make(map[string][]string),
	}, nil
}

// Apply updates pool states, reprices the tokens they touch, then orients and
// values the block's trades at the block's closing prices.
func (processor *Processor) Apply(ctx context.Context, block *streamv1.BlockEvent) (storage.BlockOutput, error) {
	if err := ctx.Err(); err != nil {
		return storage.BlockOutput{}, err
	}
	output := engine.NewBlockOutput(block)
	height := block.GetBlockHeight()

	touched := make(map[string]struct{})
	processor.applyOracle(height, block, touched)
	for _, trade := range block.GetTrades() {
		processor.learnDecimals(height, trade)
		processor.observe(height, trade, touched)
	}
	output.Prices = processor.reprice(height, output, touched)

	touchedCandles := make(map[candleKey]struct{})
	for _, source := range block.GetTrades() {
		trade, ok := processor.orient(height, output, source)
		if !ok {
			continue
		}
		output.Trades = append(output.Trades, trade)
		processor.aggregate(height, trade, touchedCandles)
	}
	output.Candles = processor.collectCandles(touchedCandles)
	processor.prune(block)
	return output, nil
}

func (processor *Processor) Revert(_ context.Context, rollbackTo int64) error {
	if rollbackTo < processor.prunedThrough {
		return fmt.Errorf("%w: rollback to %d, history starts after %d", ErrReorgTooDeep, rollbackTo, processor.prunedThrough)
	}
	processor.decimals.revert(rollbackTo)
	processor.pools.revert(rollbackTo)
	processor.prices.revert(rollbackTo)
	processor.orientations.revert(rollbackTo)
	processor.candles.revert(rollbackTo)
	processor.oracle.revert(rollbackTo)
	if !processor.reverted || rollbackTo < processor.revertTo {
		processor.reverted, processor.revertTo = true, rollbackTo
	}
	return nil
}

func (processor *Processor) prune(block *streamv1.BlockEvent) {
	through := block.GetFinalizedHeight()
	if byDepth := block.GetBlockHeight() - processor.options.HistoryDepth; byDepth > through {
		through = byDepth
	}
	if through <= processor.prunedThrough {
		return
	}
	processor.decimals.prune(through)
	processor.pools.prune(through)
	processor.prices.prune(through)
	processor.orientations.prune(through)
	processor.candles.prune(through)
	processor.oracle.prune(through)
	processor.prunedThrough = through
}

func (processor *Processor) learnDecimals(height int64, trade *streamv1.Trade) {
	for _, amount := range trade.GetAmounts() {
		if !amount.GetDecimalsKnown() {
			continue
		}
		token := normalize(amount.GetAsset())
		if known, ok := processor.decimals.get(token); !ok || known != amount.GetDecimals() {
			processor.decimals.set(height, token, amount.GetDecimals())
		}
	}
}

func (processor *Processor) observe(height int64, trade *streamv1.Trade, touched map[string]struct{}) {
	id := normalize(trade.GetPoolId())
	prev, existed := processor.pools.get(id)
	next, ok := observePool(prev, trade, processor.decimals.get)
	if !ok {
		return
	}
	processor.pools.set(height, id, next)
	for _, member := range next.Tokens {
		token := processor.options.Catalog.Canonical(member)
		touched[token] = struct{}{}
		if !existed && !slices.Contains(processor.tokenPools[token], id) {
			processor.tokenPools[token] = append(processor.tokenPools[token], id)
		}
	}
}

// applyOracle records answers from trusted feeds and marks their tokens for
// repricing.
func (processor *Processor) applyOracle(height int64, block *streamv1.BlockEvent, touched map[string]struct{}) {
	for _, update := range block.GetOracleUpdates() {
		feed, ok := processor.feeds[normalize(update.GetFeed())]
		if !ok {
			continue
		}
		answer, err := decimal.NewFromString(update.GetAnswer())
		if err != nil || !answer.IsPositive() {
			continue
		}
		processor.oracle.set(height, feed.Token, answer.Shift(-int32(feed.Decimals)))
		touched[feed.Token] = struct{}{}
	}
}

// anchorPrice returns the USD price of a catalog anchor: its oracle answer
// when a feed covers it, otherwise the catalog's peg.
func (processor *Processor) anchorPrice(token string) (decimal.Decimal, bool) {
	peg, ok := processor.options.Catalog.Anchor(token)
	if !ok {
		return decimal.Decimal{}, false
	}
	if answer, ok := processor.oracle.get(processor.options.Catalog.Canonical(token)); ok {
		return answer, true
	}
	return peg, true
}

// priceOf returns token's current USD price, anchors included.
func (processor *Processor) priceOf(token string) (tokenPrice, bool) {
	if usd, ok := processor.anchorPrice(token); ok {
		return tokenPrice{USD: usd, FromOracle: true}, true
	}
	return processor.prices.get(processor.options.Catalog.Canonical(token))
}

// guard keeps a feed-backed token's price within OracleDeviation of its
// oracle answer, and falls back to the answer when the graph has no price.
func (processor *Processor) guard(height int64, token string, candidate tokenPrice, found bool) (tokenPrice, bool) {
	answer, ok := processor.oracle.get(token)
	if !ok {
		return candidate, found
	}
	if found {
		deviation := candidate.USD.Sub(answer).Abs().DivRound(answer, precision)
		if !deviation.GreaterThan(processor.options.OracleDeviation) {
			return candidate, true
		}
	}
	return tokenPrice{USD: answer, Height: height, FromOracle: true}, true
}

// reprice recomputes every touched token from its most liquid priced
// counterpart. Each pass can extend prices one hop further from an anchor.
func (processor *Processor) reprice(height int64, output storage.BlockOutput, touched map[string]struct{}) []storage.TokenPrice {
	tokens := make([]string, 0, len(touched))
	for token := range touched {
		tokens = append(tokens, token)
	}
	slices.Sort(tokens)
	for range processor.options.MaxHops {
		changed := false
		for _, token := range tokens {
			if _, anchor := processor.options.Catalog.Anchor(token); anchor {
				continue
			}
			candidate, found := processor.bestQuote(height, token)
			candidate, ok := processor.guard(height, token, candidate, found)
			if !ok {
				continue
			}
			current, exists := processor.prices.get(token)
			if exists && current.Height == height && current.Via == candidate.Via &&
				current.FromOracle == candidate.FromOracle && current.USD.Equal(candidate.USD) {
				continue
			}
			processor.prices.set(height, token, candidate)
			changed = true
		}
		if !changed {
			break
		}
	}

	var prices []storage.TokenPrice
	for _, token := range tokens {
		value := storage.TokenPrice{
			ChainID: output.ChainID, Token: token, BlockHeight: height, BlockTime: output.BlockTime,
		}
		if usd, anchor := processor.anchorPrice(token); anchor {
			value.PriceUSD, value.Source = usd, storage.PriceSourceAnchor
		} else if price, ok := processor.prices.get(token); ok && price.Height == height {
			value.PriceUSD, value.Source = price.USD, storage.PriceSourceGraph
			value.Hops, value.LiquidityUSD = price.Hops, price.LiquidityUSD
			if price.FromOracle {
				value.Source = storage.PriceSourceAnchor
			}
		} else {
			continue
		}
		prices = append(prices, value)
	}
	return prices
}

// bestQuote prices token through the pool whose priced counterpart reserve
// is worth the most in USD.
func (processor *Processor) bestQuote(height int64, token string) (tokenPrice, bool) {
	var best tokenPrice
	found := false
	for _, id := range processor.tokenPools[token] {
		state, ok := processor.pools.get(id)
		if !ok || !state.HasReserves {
			continue
		}
		position, ok := state.index(processor.options.Catalog, token)
		if !ok {
			continue
		}
		other := processor.options.Catalog.Canonical(state.Tokens[1-position])
		if other == token {
			continue
		}
		counterpart, ok := processor.priceOf(other)
		if !ok || counterpart.Via == token || counterpart.Hops >= processor.options.MaxHops {
			continue
		}
		liquidity := state.Reserves[1-position].Mul(counterpart.USD).Round(precision)
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
			USD: relative.Mul(counterpart.USD).Round(precision), Hops: counterpart.Hops + 1,
			Via: other, Pool: id, LiquidityUSD: liquidity, Height: height,
		}
		found = true
	}
	return best, found
}

// chooseQuote picks the quote side of a pair. fixed is false when neither
// token has a catalog rank or a price, so the choice may still change.
func (processor *Processor) chooseQuote(left, right string) (string, bool) {
	catalog := processor.options.Catalog
	leftRank, leftRanked := catalog.QuotePriority(left)
	rightRank, rightRanked := catalog.QuotePriority(right)
	switch {
	case leftRanked && rightRanked:
		if leftRank < rightRank {
			return left, true
		}
		return right, true
	case leftRanked:
		return left, true
	case rightRanked:
		return right, true
	}
	leftPrice, leftPriced := processor.priceOf(left)
	rightPrice, rightPriced := processor.priceOf(right)
	switch {
	case leftPriced && rightPriced:
		if leftPrice.Hops < rightPrice.Hops ||
			(leftPrice.Hops == rightPrice.Hops && leftPrice.LiquidityUSD.GreaterThan(rightPrice.LiquidityUSD)) {
			return left, true
		}
		return right, true
	case leftPriced:
		return left, true
	case rightPriced:
		return right, true
	default:
		return right, false
	}
}

func (processor *Processor) orient(height int64, output storage.BlockOutput, source *streamv1.Trade) (storage.Trade, bool) {
	var legs []*streamv1.Amount
	for _, amount := range source.GetAmounts() {
		if rawSign(amount) != 0 {
			legs = append(legs, amount)
		}
	}
	if len(legs) != 2 || normalize(legs[0].GetAsset()) == normalize(legs[1].GetAsset()) {
		return storage.Trade{}, false
	}
	poolID := normalize(source.GetPoolId())
	left, right := normalize(legs[0].GetAsset()), normalize(legs[1].GetAsset())
	quote, decided := processor.orientations.get(poolID)
	if !decided || !quote.Fixed {
		token, fixed := processor.chooseQuote(left, right)
		if !decided || quote.Quote != token || fixed {
			quote = orientation{Quote: token, Fixed: fixed}
			processor.orientations.set(height, poolID, quote)
		}
	}
	baseLeg, quoteLeg := legs[0], legs[1]
	switch quote.Quote {
	case left:
		baseLeg, quoteLeg = legs[1], legs[0]
	case right:
	default:
		// The fixed quote token is not among this operation's legs.
		return storage.Trade{}, false
	}

	var side storage.Side
	switch source.GetAction() {
	case streamv1.TradeAction_TRADE_ACTION_SWAP:
		baseSign, quoteSign := rawSign(baseLeg), rawSign(quoteLeg)
		if baseSign == quoteSign {
			return storage.Trade{}, false
		}
		side = storage.SideSell
		if baseSign < 0 {
			side = storage.SideBuy
		}
	case streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_ADDED:
		side = storage.SideLiquidityAdded
	case streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_REMOVED:
		side = storage.SideLiquidityRemoved
	case streamv1.TradeAction_TRADE_ACTION_UNSPECIFIED:
		return storage.Trade{}, false
	default:
		return storage.Trade{}, false
	}
	baseAmount, okBase := amountValue(baseLeg)
	quoteAmount, okQuote := amountValue(quoteLeg)
	if !okBase || !okQuote {
		return storage.Trade{}, false
	}
	trade := storage.Trade{
		ChainID: output.ChainID, BlockHeight: height, BlockTime: output.BlockTime,
		TransactionHash: source.GetTransactionHash(), TransactionIndex: source.GetTransactionIndex(),
		LogIndex: source.GetLogIndex(), OperationIndex: source.GetOperationIndex(),
		Protocol: source.GetProtocol(), PoolID: poolID, Trader: normalize(source.GetTrader()),
		Side: side, BaseToken: normalize(baseLeg.GetAsset()), QuoteToken: normalize(quoteLeg.GetAsset()),
		BaseAmount: baseAmount.Abs(), QuoteAmount: quoteAmount.Abs(),
	}
	if trade.BaseAmount.IsZero() {
		return storage.Trade{}, false
	}
	trade.PriceQuote = trade.QuoteAmount.DivRound(trade.BaseAmount, precision)
	if quotePrice, ok := processor.priceOf(trade.QuoteToken); ok {
		trade.PriceUSD = trade.PriceQuote.Mul(quotePrice.USD).Round(precision)
		trade.ValueUSD = trade.QuoteAmount.Mul(quotePrice.USD).Round(precision)
	}
	return trade, true
}

// quoteLiquidityUSD is the USD value of the quote-side reserve of a trade's
// pool, or zero when unknown.
func (processor *Processor) quoteLiquidityUSD(trade storage.Trade) decimal.Decimal {
	state, ok := processor.pools.get(trade.PoolID)
	if !ok || !state.HasReserves {
		return decimal.Zero
	}
	quote := processor.options.Catalog.Canonical(trade.QuoteToken)
	position, ok := state.index(processor.options.Catalog, quote)
	if !ok {
		return decimal.Zero
	}
	price, ok := processor.priceOf(quote)
	if !ok {
		return decimal.Zero
	}
	return state.Reserves[position].Mul(price.USD)
}

// aggregate adds a priced swap to its pool candles and, when the pool is
// liquid enough, to its base token's aggregated candles.
func (processor *Processor) aggregate(height int64, trade storage.Trade, touched map[candleKey]struct{}) {
	if trade.Side != storage.SideBuy && trade.Side != storage.SideSell {
		return
	}
	if !trade.PriceUSD.IsPositive() {
		return
	}
	subjects := []candleKey{{Subject: storage.CandleSubjectPool, Key: trade.PoolID}}
	if !processor.quoteLiquidityUSD(trade).LessThan(processor.options.CandleMinLiquidityUSD) {
		subjects = append(subjects, candleKey{
			Subject: storage.CandleSubjectToken, Key: processor.options.Catalog.Canonical(trade.BaseToken),
		})
	}
	for _, subject := range subjects {
		for _, interval := range processor.options.Intervals {
			key := candleKey{Subject: subject.Subject, Key: subject.Key, Interval: interval}
			current, exists := processor.candles.get(key)
			processor.candles.set(height, key, updateCandle(current, exists, trade, key, height))
			touched[key] = struct{}{}
		}
	}
}

// updateCandle adds trade to the bucket it falls in. A new bucket opens at the
// previous bucket's close, so consecutive candles never gap.
func updateCandle(current storage.Candle, exists bool, trade storage.Trade, key candleKey, height int64) storage.Candle {
	price := trade.PriceUSD
	bucket := trade.BlockTime.Truncate(key.Interval)
	if !exists || bucket.After(current.OpenTime) {
		open := price
		if exists {
			open = current.Close
		}
		current = storage.Candle{
			ChainID: trade.ChainID, Subject: key.Subject, Key: key.Key, Interval: key.Interval,
			OpenTime: bucket, Open: open, High: decimal.Max(open, price), Low: decimal.Min(open, price),
		}
	}
	current.High = decimal.Max(current.High, price)
	current.Low = decimal.Min(current.Low, price)
	current.Close = price
	current.VolumeBase = current.VolumeBase.Add(trade.BaseAmount)
	current.VolumeUSD = current.VolumeUSD.Add(trade.ValueUSD)
	current.Trades++
	current.UpdateHeight = height
	return current
}

func (processor *Processor) collectCandles(touched map[candleKey]struct{}) []storage.Candle {
	candles := make([]storage.Candle, 0, len(touched))
	for key := range touched {
		if candle, ok := processor.candles.get(key); ok {
			candles = append(candles, candle)
		}
	}
	slices.SortFunc(candles, func(left, right storage.Candle) int {
		switch {
		case left.Subject != right.Subject:
			return int(left.Subject) - int(right.Subject)
		case left.Key != right.Key:
			if left.Key < right.Key {
				return -1
			}
			return 1
		default:
			return int(left.Interval - right.Interval)
		}
	})
	return candles
}

var (
	_ engine.Processor = (*Processor)(nil)
	_ engine.Stateful  = (*Processor)(nil)
)
