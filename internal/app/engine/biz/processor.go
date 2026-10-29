// Package market orients trades, prices tokens through a liquidity-weighted
// graph anchored on USD assets, and aggregates candles.
package biz

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

// ErrReorgTooDeep means a revert reaches below the retained undo history.
var ErrReorgTooDeep = errors.New("market: reorg deeper than retained history")

type Options struct {
	Catalog asset.Catalog
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
	// OutlierDeviation is how far, as a ratio, a swap's USD price may stray
	// from its base token's reference price before the swap is marked an
	// outlier and left out of the token's candles. The reference must come
	// from another pool, an anchor or an oracle answer. Zero turns it off.
	OutlierDeviation decimal.Decimal
	// LateTradeBlocks is how many blocks a trade whose token decimals are
	// unknown is held for replay once they arrive. Zero turns holding off.
	LateTradeBlocks int64
	// LateTradesPerToken caps the trades held while waiting on one token.
	LateTradesPerToken int
	// History, when set, lets the block that brings a token's decimals
	// recompute the token's trades from the raw trades stored for earlier
	// blocks, reaching back at most BackfillBlocks blocks. Without it only
	// held trades are replayed.
	History        History
	BackfillBlocks int64
	// DepthBands size the price band a concentrated pool's depth is measured
	// over by its fee tier, ascending by fee; a fee above every band uses the
	// last. DynamicFeeDepthBPS is the band of a pool whose hook sets its fee.
	// Empty or zero use model.DefaultDepthBands and
	// model.DefaultDynamicFeeDepthBPS.
	DepthBands         []model.DepthBand
	DynamicFeeDepthBPS int
	// Logger reports backfills; nil discards.
	Logger *slog.Logger
}

// Feed is a trusted Chainlink-style aggregator answering in USD. One
// aggregator may price several tokens, each in its own Feed.
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
	Subject  model.CandleSubject
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
	candles      *journal[candleKey, model.Candle]
	// oracle holds the latest trusted oracle answer per canonical token.
	oracle *journal[string, decimal.Decimal]
	// feeds lists the tokens each trusted aggregator prices.
	feeds map[string][]Feed
	// tokenPools indexes pools by canonical token. It only grows; pools that
	// a revert removed are skipped on lookup.
	tokenPools map[string][]string
	// late holds trades waiting on token decimals, by lateKey.
	late *journal[string, lateTrade]
	// lateByToken indexes held trades by each token they wait on, and
	// lateOrder lists them oldest first; keys no longer in late are skipped.
	lateByToken map[string]map[string]struct{}
	lateOrder   []string
	// missed records, per token, the first and last heights that had a
	// trade waiting on its decimals, so a backfill knows what to read.
	missed *journal[string, missedToken]
	// ticks and clamm are the tick tables of seeded concentrated-liquidity
	// pools. poolTicks indexes ticks by pool, and books caches each pool's
	// prepared table until its ticks change; neither is persisted.
	ticks     *journal[tickKey, string]
	clamm     *journal[string, model.CLAMMPool]
	poolTicks map[string]map[int32]struct{}
	books     map[string]*tickBook
	// balances are the latest balance snapshots of Curve and Balancer pools.
	// tokenBalancePools and derivedPools index them by canonical token, pool
	// token and token priced by rate, and only grow; balanceEdges caches what
	// each record implies per token pair. None of the three is persisted.
	balances          *journal[string, model.BalancePool]
	tokenBalancePools map[string][]string
	derivedPools      map[string][]string
	balanceEdges      map[string]*poolEdges
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
	if options.OracleDeviation.IsNegative() || options.OutlierDeviation.IsNegative() {
		return nil, errors.New("market: oracle and outlier deviations must not be negative")
	}
	if options.LateTradeBlocks < 0 || (options.LateTradeBlocks > 0 && options.LateTradesPerToken <= 0) {
		return nil, errors.New("market: holding late trades needs positive blocks and a positive per-token cap")
	}
	if options.BackfillBlocks < 0 {
		return nil, errors.New("market: backfill blocks must not be negative")
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}
	if err := validateDepthBands(&options); err != nil {
		return nil, err
	}
	feeds := make(map[string][]Feed, len(options.Feeds))
	for _, feed := range options.Feeds {
		address := normalize(feed.Address)
		if address == "" || normalize(feed.Token) == "" {
			return nil, errors.New("market: feeds need an address and a token")
		}
		feed.Token = options.Catalog.Canonical(feed.Token)
		for _, existing := range feeds[address] {
			if existing.Token == feed.Token {
				return nil, fmt.Errorf("market: duplicate feed %s for %s", address, feed.Token)
			}
		}
		feeds[address] = append(feeds[address], feed)
	}
	return &Processor{
		options:           options,
		decimals:          newJournal[string, uint32](stringKeys(kindDecimals)),
		pools:             newJournal[string, pool](stringKeys(kindPools)),
		prices:            newJournal[string, tokenPrice](stringKeys(kindPrices)),
		orientations:      newJournal[string, orientation](stringKeys(kindOrientations)),
		candles:           newJournal[candleKey, model.Candle](candleKeys),
		oracle:            newJournal[string, decimal.Decimal](stringKeys(kindOracle)),
		late:              newJournal[string, lateTrade](stringKeys(kindLate)),
		missed:            newJournal[string, missedToken](stringKeys(kindMissed)),
		ticks:             newJournal[tickKey, string](tickKeys),
		clamm:             newJournal[string, model.CLAMMPool](stringKeys(model.StateKindCLAMMPools)),
		poolTicks:         make(map[string]map[int32]struct{}),
		books:             make(map[string]*tickBook),
		balances:          newJournal[string, model.BalancePool](stringKeys(model.StateKindBalancePools)),
		tokenBalancePools: make(map[string][]string),
		derivedPools:      make(map[string][]string),
		balanceEdges:      make(map[string]*poolEdges),
		feeds:             feeds,
		tokenPools:        make(map[string][]string),
		lateByToken:       make(map[string]map[string]struct{}),
	}, nil
}

// Apply updates pool states, reprices the tokens they touch, then orients and
// values the block's trades at the block's closing prices.
//
// Trades whose token decimals are unknown are held. When a block brings
// those decimals, from an asset's metadata or another trade, the held trades,
// and with a History every stored trade of the token, are replayed first:
// valued at their own block's prices and added to their candles, ahead of
// the block's own trades. The token's prices at those blocks are
// recomputed too.
func (processor *Processor) Apply(ctx context.Context, block *streamv1.BlockEvent) (model.BlockOutput, error) {
	if err := ctx.Err(); err != nil {
		return model.BlockOutput{}, err
	}
	output := NewBlockOutput(block)
	height := block.GetBlockHeight()

	touched := make(map[string]struct{})
	learned := make(map[string]struct{})
	processor.applyOracle(height, block, touched)
	processor.learnAssets(height, block, learned)
	for _, trade := range block.GetTrades() {
		processor.learnDecimals(height, trade, learned)
	}
	ready, series, err := processor.backfill(ctx, height, output.ChainID, learned, processor.ready(learned))
	if err != nil {
		return model.BlockOutput{}, err
	}
	past := processor.pastPrices(output.ChainID, ready, series, learned)
	tables := make(map[string]struct{})
	processor.applySnapshots(height, block, tables)
	processor.seedPools(height, ready, touched)
	for _, trade := range block.GetTrades() {
		processor.applyRange(height, trade, tables)
		processor.checkSwap(height, trade, tables)
		processor.observe(height, trade, touched)
	}
	processor.settleTables(height, tables, touched)
	processor.applyBalances(height, block, touched)
	output.Prices = past.prices
	output.Prices = append(output.Prices, processor.reprice(height, output, touched)...)

	touchedCandles := make(map[candleKey]struct{})
	replayed := processor.replay(height, &output, ready, past, touchedCandles)
	for _, source := range block.GetTrades() {
		trade, status := processor.orient(height, source)
		switch status {
		case orientLate:
			processor.hold(height, block.GetBlockTime(), source)
			processor.miss(height, source)
			continue
		case orientSkip:
			continue
		case orientOK:
		}
		trade.ChainID, trade.BlockHeight, trade.BlockTime = output.ChainID, height, output.BlockTime
		if quotePrice, ok := processor.priceOf(trade.QuoteToken); ok {
			valueTrade(&trade, quotePrice.USD)
			trade.Outlier = processor.outlier(trade)
		}
		output.Trades = append(output.Trades, trade)
		processor.aggregate(height, trade, touchedCandles)
	}
	output.Candles = processor.collectCandles(touchedCandles, replayed)
	processor.expireLate(height)
	processor.expireMissed(height)
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
	processor.late.revert(rollbackTo)
	processor.missed.revert(rollbackTo)
	processor.ticks.revert(rollbackTo)
	processor.clamm.revert(rollbackTo)
	processor.balances.revert(rollbackTo)
	processor.rebuildLate()
	processor.rebuildTicks()
	processor.rebuildBalances()
	if !processor.reverted || rollbackTo < processor.revertTo {
		processor.reverted, processor.revertTo = true, rollbackTo
	}
	return nil
}

// RevertedCandles returns the candle series that Revert(rollbackTo) will
// change, each with its newest bucket as of rollbackTo. Call it before Revert.
//
// A series only gains buckets forward in time, and late trades only build
// buckets for series that had none, so the bucket current at rollbackTo is the
// only older bucket a reverted block can have changed.
func (processor *Processor) RevertedCandles(rollbackTo int64) []model.CandleReset {
	restored := processor.candles.restored(rollbackTo)
	resets := make([]model.CandleReset, 0, len(restored))
	for _, key := range slices.SortedFunc(maps.Keys(restored), compareCandleKeys) {
		reset := model.CandleReset{Subject: key.Subject, Key: key.Key, Interval: key.Interval}
		if entry := restored[key]; entry.Existed {
			latest := entry.Prev
			reset.Latest = &latest
		}
		resets = append(resets, reset)
	}
	return resets
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
	processor.late.prune(through)
	processor.missed.prune(through)
	processor.ticks.prune(through)
	processor.clamm.prune(through)
	processor.balances.prune(through)
	processor.prunedThrough = through
}

func (processor *Processor) learnDecimals(height int64, trade *streamv1.Trade, learned map[string]struct{}) {
	for _, amount := range trade.GetAmounts() {
		if amount.GetDecimalsKnown() {
			processor.learn(height, normalize(amount.GetAsset()), amount.GetDecimals(), learned)
		}
	}
}

func (processor *Processor) observe(height int64, trade *streamv1.Trade, touched map[string]struct{}) {
	id := normalize(trade.GetPoolId())
	prev, existed := processor.pools.get(id)
	next, ok := observePool(prev, trade, processor.decimals.get, processor.tableAmounts)
	if !ok {
		return
	}
	processor.setPool(height, id, next, existed, touched)
}

func (processor *Processor) setPool(height int64, id string, next pool, existed bool, touched map[string]struct{}) {
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
		feeds := processor.feeds[normalize(update.GetFeed())]
		if len(feeds) == 0 {
			continue
		}
		answer, err := decimal.NewFromString(update.GetAnswer())
		if err != nil || !answer.IsPositive() {
			continue
		}
		for _, feed := range feeds {
			processor.oracle.set(height, feed.Token, answer.Shift(-int32(feed.Decimals)))
			touched[feed.Token] = struct{}{}
		}
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
func (processor *Processor) reprice(height int64, output model.BlockOutput, touched map[string]struct{}) []model.TokenPrice {
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

	var prices []model.TokenPrice
	for _, token := range tokens {
		value := model.TokenPrice{
			ChainID: output.ChainID, Token: token, BlockHeight: height, BlockTime: output.BlockTime,
		}
		if usd, anchor := processor.anchorPrice(token); anchor {
			value.PriceUSD, value.Source = usd, model.PriceSourceAnchor
		} else if price, ok := processor.prices.get(token); ok && price.Height == height {
			value.PriceUSD, value.Source = price.USD, model.PriceSourceGraph
			value.Hops, value.LiquidityUSD = price.Hops, price.LiquidityUSD
			if price.FromOracle {
				value.Source = model.PriceSourceAnchor
			}
		} else {
			continue
		}
		prices = append(prices, value)
	}
	return prices
}

// bestQuote prices token through its best pool, see quoteCandidate.beats.
// A rate reported for the token itself wins over every pool.
func (processor *Processor) bestQuote(height int64, token string) (tokenPrice, bool) {
	if derived, ok := processor.derivedQuote(height, token); ok {
		return derived, true
	}
	var best quoteCandidate
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
		candidate := quoteCandidate{price: tokenPrice{
			USD: relative.Mul(counterpart.USD).Round(precision), Hops: counterpart.Hops + 1,
			Via: other, Pool: id, LiquidityUSD: liquidity, Height: height,
		}}
		if depth, ok := processor.pairDepth(id, state, 1-position); ok {
			candidate.depthUSD, candidate.hasDepth = depth.Mul(counterpart.USD).Round(precision), true
		}
		if !found || candidate.beats(best) {
			best, found = candidate, true
		}
	}
	best, found = processor.balanceQuote(height, token, best, found)
	return best.price, found
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

type orientStatus uint8

const (
	orientSkip orientStatus = iota
	orientOK
	// orientLate means a token's decimals are not known yet.
	orientLate
)

// orient fixes a trade's base and quote. The caller sets the chain, block
// and USD fields.
func (processor *Processor) orient(height int64, source *streamv1.Trade) (model.Trade, orientStatus) {
	var legs []*streamv1.Amount
	for _, amount := range source.GetAmounts() {
		if rawSign(amount) != 0 {
			legs = append(legs, amount)
		}
	}
	if len(legs) != 2 || normalize(legs[0].GetAsset()) == normalize(legs[1].GetAsset()) {
		return model.Trade{}, orientSkip
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
		return model.Trade{}, orientSkip
	}

	var side model.Side
	switch source.GetAction() {
	case streamv1.TradeAction_TRADE_ACTION_SWAP:
		baseSign, quoteSign := rawSign(baseLeg), rawSign(quoteLeg)
		if baseSign == quoteSign {
			return model.Trade{}, orientSkip
		}
		side = model.SideSell
		if baseSign < 0 {
			side = model.SideBuy
		}
	case streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_ADDED:
		side = model.SideLiquidityAdded
	case streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_REMOVED:
		side = model.SideLiquidityRemoved
	case streamv1.TradeAction_TRADE_ACTION_UNSPECIFIED:
		return model.Trade{}, orientSkip
	default:
		return model.Trade{}, orientSkip
	}
	baseAmount, okBase := processor.amountOf(baseLeg)
	quoteAmount, okQuote := processor.amountOf(quoteLeg)
	if !okBase || !okQuote {
		return model.Trade{}, orientLate
	}
	trade := model.Trade{
		TransactionHash: source.GetTransactionHash(), TransactionIndex: source.GetTransactionIndex(),
		LogIndex: source.GetLogIndex(), OperationIndex: source.GetOperationIndex(),
		Protocol: source.GetProtocol(), PoolID: poolID, Trader: normalize(source.GetTrader()),
		Side: side, BaseToken: normalize(baseLeg.GetAsset()), QuoteToken: normalize(quoteLeg.GetAsset()),
		BaseAmount: baseAmount.Abs(), QuoteAmount: quoteAmount.Abs(),
	}
	if trade.BaseAmount.IsZero() {
		return model.Trade{}, orientSkip
	}
	trade.PriceQuote = trade.QuoteAmount.DivRound(trade.BaseAmount, precision)
	return trade, orientOK
}

// valueTrade prices a trade from its quote token's USD price.
func valueTrade(trade *model.Trade, quoteUSD decimal.Decimal) {
	trade.PriceUSD = trade.PriceQuote.Mul(quoteUSD).Round(precision)
	trade.ValueUSD = trade.QuoteAmount.Mul(quoteUSD).Round(precision)
}

// outlier reports whether a priced swap's USD price strays further than
// OutlierDeviation from its base token's reference price: the anchor or
// oracle price, or the graph price set by the token's most liquid priced
// pool. A swap in that pool is never an outlier, since its own reserves set
// the reference; a thin pool trading far from it is, such as a manipulated
// or mispriced pool.
func (processor *Processor) outlier(trade model.Trade) bool {
	reference, ok := processor.priceOf(trade.BaseToken)
	return processor.strays(trade, reference, ok)
}

// strays reports whether trade is an outlier against its base token's
// reference price.
func (processor *Processor) strays(trade model.Trade, reference tokenPrice, ok bool) bool {
	limit := processor.options.OutlierDeviation
	if !limit.IsPositive() || (trade.Side != model.SideBuy && trade.Side != model.SideSell) || !trade.PriceUSD.IsPositive() {
		return false
	}
	if !ok || !reference.USD.IsPositive() || (!reference.FromOracle && reference.Pool == trade.PoolID) {
		return false
	}
	ratio := trade.PriceUSD.DivRound(reference.USD, precision)
	if ratio.LessThan(decimal.NewFromInt(1)) {
		ratio = decimal.NewFromInt(1).DivRound(ratio, precision)
	}
	return ratio.Sub(decimal.NewFromInt(1)).GreaterThan(limit)
}

// quoteLiquidityUSD is the USD value of the quote-side reserve of a trade's
// pool, or zero when unknown.
func (processor *Processor) quoteLiquidityUSD(trade model.Trade) decimal.Decimal {
	price, ok := processor.priceOf(trade.QuoteToken)
	if !ok {
		return decimal.Zero
	}
	if record, isBalancePool := processor.balances.get(trade.PoolID); isBalancePool {
		quote := processor.options.Catalog.Canonical(trade.QuoteToken)
		for position, member := range record.Tokens {
			if processor.options.Catalog.Canonical(member) != quote {
				continue
			}
			if balance, known := processor.balanceOf(record, position); known {
				return balance.Mul(price.USD)
			}
		}
		return decimal.Zero
	}
	state, ok := processor.pools.get(trade.PoolID)
	if !ok {
		return decimal.Zero
	}
	return reserveUSD(processor.options.Catalog, state, trade.QuoteToken, price.USD)
}

// reserveUSD values token's reserve in state at usd, or zero when unknown.
func reserveUSD(catalog asset.Catalog, state pool, token string, usd decimal.Decimal) decimal.Decimal {
	if !state.HasReserves {
		return decimal.Zero
	}
	position, ok := state.index(catalog, catalog.Canonical(token))
	if !ok {
		return decimal.Zero
	}
	return state.Reserves[position].Mul(usd)
}

// candleSubjects returns the candles a priced swap joins: its pool's, and its
// base token's when the quote reserve reaches CandleMinLiquidityUSD and the
// swap is not an outlier. An outlier still joins its pool's candles, which
// show that pool's own prices.
func (processor *Processor) candleSubjects(trade model.Trade, quoteLiquidity decimal.Decimal) []candleKey {
	if trade.Side != model.SideBuy && trade.Side != model.SideSell {
		return nil
	}
	if !trade.PriceUSD.IsPositive() {
		return nil
	}
	subjects := []candleKey{{Subject: model.CandleSubjectPool, Key: trade.PoolID}}
	if !trade.Outlier && !quoteLiquidity.LessThan(processor.options.CandleMinLiquidityUSD) {
		subjects = append(subjects, candleKey{
			Subject: model.CandleSubjectToken, Key: processor.options.Catalog.Canonical(trade.BaseToken),
		})
	}
	return subjects
}

// aggregate adds a priced swap to its candles.
func (processor *Processor) aggregate(height int64, trade model.Trade, touched map[candleKey]struct{}) {
	for _, subject := range processor.candleSubjects(trade, processor.quoteLiquidityUSD(trade)) {
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
func updateCandle(current model.Candle, exists bool, trade model.Trade, key candleKey, height int64) model.Candle {
	price := trade.PriceUSD
	bucket := trade.BlockTime.Truncate(key.Interval)
	if !exists || bucket.After(current.OpenTime) {
		open := price
		if exists {
			open = current.Close
		}
		current = model.Candle{
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

// collectCandles returns the current bucket of every touched key, and the
// older buckets that replayed trades built.
func (processor *Processor) collectCandles(touched map[candleKey]struct{}, replayed map[candleKey][]model.Candle) []model.Candle {
	candles := make([]model.Candle, 0, len(touched))
	for key := range touched {
		current, ok := processor.candles.get(key)
		if !ok {
			continue
		}
		candles = append(candles, current)
		for _, bucket := range replayed[key] {
			if !bucket.OpenTime.Equal(current.OpenTime) {
				candles = append(candles, bucket)
			}
		}
	}
	slices.SortFunc(candles, func(left, right model.Candle) int {
		return cmp.Or(
			compareCandleKeys(
				candleKey{Subject: left.Subject, Key: left.Key, Interval: left.Interval},
				candleKey{Subject: right.Subject, Key: right.Key, Interval: right.Interval},
			),
			left.OpenTime.Compare(right.OpenTime),
		)
	})
	return candles
}

func compareCandleKeys(left, right candleKey) int {
	return cmp.Or(
		cmp.Compare(left.Subject, right.Subject),
		cmp.Compare(left.Key, right.Key),
		cmp.Compare(left.Interval, right.Interval),
	)
}

var (
	_ BlockProcessor = (*Processor)(nil)
	_ Stateful       = (*Processor)(nil)

	_ CandleReverter = (*Processor)(nil)
)
