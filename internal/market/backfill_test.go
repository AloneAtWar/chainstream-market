package market

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"
)

const memeThin = "0xpool-meme-weth-thin"

// fakeHistory serves raw trades and prices the way chraw.History does.
type fakeHistory struct {
	trades   []storage.RawTradeAt
	prices   map[string][]storage.PricePoint
	notReady bool
	// calls records each RawTrades request as tokens, from and through.
	calls      []historyCall
	priceCalls [][]string
}

type historyCall struct {
	tokens        []string
	from, through int64
}

func (history *fakeHistory) RawTrades(_ context.Context, _ uint64, tokens []string, from, through int64) ([]storage.RawTradeAt, bool, error) {
	history.calls = append(history.calls, historyCall{slices.Clone(tokens), from, through})
	if history.notReady {
		return nil, false, nil
	}
	var trades []storage.RawTradeAt
	for _, trade := range history.trades {
		if trade.Height < from || trade.Height > through {
			continue
		}
		for _, amount := range trade.Amounts {
			if slices.Contains(tokens, amount.Asset) {
				trades = append(trades, trade)
				break
			}
		}
	}
	return trades, true, nil
}

func (history *fakeHistory) PriceHistory(_ context.Context, _ uint64, tokens []string, _, _ int64) (map[string][]storage.PricePoint, error) {
	history.priceCalls = append(history.priceCalls, slices.Clone(tokens))
	prices := make(map[string][]storage.PricePoint)
	for _, token := range tokens {
		if points, ok := history.prices[token]; ok {
			prices[token] = points
		}
	}
	return prices, nil
}

// store keeps the trades of block the way ch-loader does.
func (history *fakeHistory) store(block *streamv1.BlockEvent) {
	for _, trade := range block.GetTrades() {
		history.trades = append(history.trades, storage.RawTradeAt{
			Height: block.GetBlockHeight(), BlockTime: time.Unix(block.GetBlockTime(), 0).UTC(), RawTrade: rawOf(trade),
		})
	}
}

func rawOf(trade *streamv1.Trade) storage.RawTrade {
	raw := storage.RawTrade{
		TransactionHash: trade.GetTransactionHash(), TransactionIndex: trade.GetTransactionIndex(),
		LogIndex: trade.GetLogIndex(), OperationIndex: trade.GetOperationIndex(), Protocol: trade.GetProtocol(),
		DexAddress: trade.GetDexAddress(), PoolID: trade.GetPoolId(), Sender: trade.GetSender(),
		Trader: trade.GetTrader(), Recipient: trade.GetRecipient(),
		ReservesAfter: trade.GetPoolState().GetReservesAfter(), SqrtPriceX96: trade.GetPoolState().GetSqrtPriceX96(),
		Liquidity: trade.GetPoolState().GetLiquidity(), Tick: trade.GetPoolState().GetTick(),
	}
	switch trade.GetAction() {
	case streamv1.TradeAction_TRADE_ACTION_SWAP:
		raw.Action = "swap"
	case streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_ADDED:
		raw.Action = "liquidity_added"
	case streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_REMOVED:
		raw.Action = "liquidity_removed"
	case streamv1.TradeAction_TRADE_ACTION_UNSPECIFIED:
		raw.Action = "unspecified"
	}
	if trade.GetPoolState() != nil && trade.GetPoolState().ActiveBinId != nil {
		bin := trade.GetPoolState().GetActiveBinId()
		raw.ActiveBinID = &bin
	}
	for _, amount := range trade.GetAmounts() {
		raw.Amounts = append(raw.Amounts, storage.RawAmount{
			Asset: amount.GetAsset(), Raw: amount.GetRaw(), Value: amount.GetValue(),
			Decimals: amount.GetDecimals(), DecimalsKnown: amount.GetDecimalsKnown(),
		})
	}
	return raw
}

func newBackfillProcessor(t *testing.T, history storage.History, heldBlocks int64, perToken int) *Processor {
	t.Helper()
	processor, err := NewProcessor(Options{
		Catalog:               DefaultCatalog(1),
		MinLiquidityUSD:       decimal.NewFromInt(1000),
		CandleMinLiquidityUSD: decimal.NewFromInt(10000),
		MaxHops:               3,
		Intervals:             []time.Duration{time.Minute, 5 * time.Minute},
		HistoryDepth:          16,
		OutlierDeviation:      decimal.RequireFromString("0.5"),
		LateTradeBlocks:       heldBlocks,
		LateTradesPerToken:    perToken,
		History:               history,
		BackfillBlocks:        1000,
	})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	return processor
}

// lateMemeBlocks are three blocks of MEME trades without decimals, then
// the block whose asset metadata brings them. Block 106 also trades MEME
// at $0.09 in a thin pool, far from the $0.03 its main pool sets.
func lateMemeBlocks() []*streamv1.BlockEvent {
	// 100 MEME for 0.002 WETH: $0.06.
	pricier := swap(memeWETH,
		leg{meme, "100000000000000000000", "1000000000000000000000000", 18},
		leg{weth, "-2000000000000000", "10000000000000000000", 18},
	)
	// 100 MEME for 0.003 WETH: $0.09, in a pool with 1 WETH.
	thin := swap(memeThin,
		leg{meme, "100000000000000000000", "100000000000000000000000", 18},
		leg{weth, "-3000000000000000", "1000000000000000000", 18},
	)
	thin.TransactionHash = "0xtx-thin"
	metadata := testBlock(107, 84)
	metadata.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
	return []*streamv1.BlockEvent{
		testBlock(100, 0, wethSwap(), undecided(memeSell(), meme, 1)),
		testBlock(106, 72, undecided(pricier, meme, 1), undecided(thin, meme, 2)),
		metadata,
	}
}

func wethPrices() map[string][]storage.PricePoint {
	return map[string][]storage.PricePoint{
		weth: {{Height: 90, USD: decimal.NewFromInt(2000), Hops: 1}, {Height: 100, USD: decimal.NewFromInt(3000), Hops: 1}},
	}
}

func TestBackfillRecomputesTradesBeyondHeldOnes(t *testing.T) {
	history := &fakeHistory{prices: wethPrices()}
	// Only one trade fits the per-token cap; the backfill brings the others.
	processor := newBackfillProcessor(t, history, 100, 1)
	var output storage.BlockOutput
	for _, block := range lateMemeBlocks() {
		history.store(block)
		output = apply(t, processor, block)
	}
	if len(history.calls) != 1 || !slices.Equal(history.calls[0].tokens, []string{meme}) ||
		history.calls[0].from != 100 || history.calls[0].through != 106 {
		t.Fatalf("raw trade reads = %+v", history.calls)
	}
	if len(history.priceCalls) != 1 || !slices.Equal(history.priceCalls[0], []string{weth}) {
		t.Fatalf("price reads = %+v", history.priceCalls)
	}

	if len(output.Trades) != 3 {
		t.Fatalf("trades = %+v, want the held one and two backfilled", output.Trades)
	}
	for index, want := range []struct {
		height  int64
		pool    string
		usd     string
		outlier bool
	}{{100, memeWETH, "0.03", false}, {106, memeWETH, "0.06", false}, {106, memeThin, "0.09", true}} {
		trade := output.Trades[index]
		if trade.BlockHeight != want.height || trade.PoolID != want.pool || trade.BaseToken != meme ||
			trade.QuoteToken != weth || trade.Outlier != want.outlier {
			t.Fatalf("trade %d = %+v, want %+v", index, trade, want)
		}
		assertDecimal(t, "price usd", trade.PriceUSD, want.usd)
	}
	if !output.Trades[1].BlockTime.Equal(time.Unix(startTime+72, 0)) {
		t.Fatalf("backfilled trade time = %s", output.Trades[1].BlockTime)
	}

	// MEME is priced at both earlier blocks through its liquid pool, ahead
	// of the block's own price.
	var past []storage.TokenPrice
	for _, price := range output.Prices {
		if price.Token == meme && price.BlockHeight < 107 {
			past = append(past, price)
		}
	}
	if len(past) != 2 || past[0].BlockHeight != 100 || past[1].BlockHeight != 106 ||
		past[0].Source != storage.PriceSourceGraph || past[0].Hops != 2 {
		t.Fatalf("past MEME prices = %+v", past)
	}
	assertDecimal(t, "meme at 100", past[0].PriceUSD, "0.03")
	assertDecimal(t, "meme liquidity", past[0].LiquidityUSD, "30000")
	if current, ok := priceMap(output.Prices)[meme]; !ok || current.BlockHeight != 107 {
		t.Fatalf("current MEME price = %+v, %v", current, ok)
	}

	poolMinutes := 0
	for _, candle := range output.Candles {
		if candle.Subject == storage.CandleSubjectPool && candle.Key == memeWETH && candle.Interval == time.Minute {
			poolMinutes++
		}
	}
	if poolMinutes != 2 {
		t.Fatalf("pool minute buckets = %d, want 2", poolMinutes)
	}
	tokenFive := findCandle(t, output.Candles, storage.CandleSubjectToken, meme, 5*time.Minute)
	if tokenFive.Trades != 2 || tokenFive.UpdateHeight != 107 {
		t.Fatalf("token 5m candle = %+v, want both liquid-pool trades and not the outlier", tokenFive)
	}
	if len(processor.missed.values) != 0 || len(processor.late.values) != 0 {
		t.Fatalf("missed = %d, held = %d after the backfill", len(processor.missed.values), len(processor.late.values))
	}
}

func TestBackfillWithoutHolding(t *testing.T) {
	history := &fakeHistory{prices: wethPrices()}
	processor := newBackfillProcessor(t, history, 0, 0)
	var output storage.BlockOutput
	for _, block := range lateMemeBlocks() {
		history.store(block)
		output = apply(t, processor, block)
	}
	if len(output.Trades) != 3 {
		t.Fatalf("trades = %d, want all three backfilled", len(output.Trades))
	}
}

func TestBackfillFallsBackToHeldTrades(t *testing.T) {
	history := &fakeHistory{prices: wethPrices(), notReady: true}
	processor := newBackfillProcessor(t, history, 100, 1)
	var output storage.BlockOutput
	for _, block := range lateMemeBlocks() {
		output = apply(t, processor, block)
	}
	if len(history.calls) != 1 || len(output.Trades) != 1 || output.Trades[0].BlockHeight != 100 {
		t.Fatalf("calls = %d, trades = %+v, want only the held trade", len(history.calls), output.Trades)
	}
	if len(processor.missed.values) != 0 {
		t.Fatal("missed tokens kept after their decimals arrived")
	}
}

func TestBackfillReverts(t *testing.T) {
	history := &fakeHistory{prices: wethPrices()}
	processor := newBackfillProcessor(t, history, 100, 1)
	blocks := lateMemeBlocks()
	var want storage.BlockOutput
	for _, block := range blocks {
		history.store(block)
		want = apply(t, processor, block)
	}
	if err := processor.Revert(context.Background(), 106); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	if entry, ok := processor.missed.get(meme); !ok || entry.First != 100 || entry.Last != 106 {
		t.Fatalf("missed after revert = %+v, %v", entry, ok)
	}
	got := apply(t, processor, blocks[2])
	if len(history.calls) != 2 || len(got.Trades) != len(want.Trades) || len(got.Prices) != len(want.Prices) ||
		len(got.Candles) != len(want.Candles) {
		t.Fatalf("after revert: %d reads, %d trades, %d prices, %d candles; want %d trades, %d prices, %d candles",
			len(history.calls), len(got.Trades), len(got.Prices), len(got.Candles),
			len(want.Trades), len(want.Prices), len(want.Candles))
	}
}

// Trades whose MEME leg is zero were applied at their block, and trades
// that still wait on another token are left for that token.
func TestBackfillSkipsTradesThatDidNotWait(t *testing.T) {
	history := &fakeHistory{prices: wethPrices()}
	processor := newBackfillProcessor(t, history, 0, 0)
	blocks := lateMemeBlocks()
	for _, block := range blocks[:2] {
		history.store(block)
		apply(t, processor, block)
	}
	zero := memeSell()
	zero.Amounts[0].Raw = "0"
	zero.LogIndex = 9
	both := undecided(undecided(swap(dustWETH,
		leg{meme, "100000000000000000000", "1000000000000000000000000", 18},
		leg{dust, "-1000000000000000", "10000000000000000000", 18},
	), meme, 10), dust, 10)
	history.store(testBlock(103, 36, zero, both))
	output := apply(t, processor, blocks[2])
	if len(output.Trades) != 3 {
		t.Fatalf("trades = %+v, want the three that waited on MEME alone", output.Trades)
	}
}

func TestHeldReplayPricesEarlierBlocks(t *testing.T) {
	processor := newLateProcessor(t, 100, 10)
	apply(t, processor, testBlock(100, 0, wethSwap(), undecided(memeSell(), meme, 1)))
	block := testBlock(101, 12)
	block.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
	output := apply(t, processor, block)
	if len(output.Prices) == 0 || output.Prices[0].Token != meme || output.Prices[0].BlockHeight != 100 ||
		!output.Prices[0].BlockTime.Equal(time.Unix(startTime, 0)) {
		t.Fatalf("prices = %+v, want MEME at 100 first", output.Prices)
	}
	assertDecimal(t, "meme at 100", output.Prices[0].PriceUSD, "0.03")
}

func TestStreamTradeRoundTrip(t *testing.T) {
	bin := uint32(8_388_608)
	trade := &streamv1.Trade{
		TransactionHash: "0xtx", TransactionIndex: 3, OperationIndex: 1, Protocol: "uniswap_v3",
		DexAddress: "0xdex", PoolId: "0xpool", Sender: "0xsender", LogIndex: 7, Trader: "0xtrader",
		Recipient: "0xrecipient", Action: streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_REMOVED,
		Amounts: []*streamv1.Amount{
			{Asset: meme, Raw: "-5", Value: "", DecimalsKnown: false},
			{Asset: weth, Raw: "7", Value: "0.000000000000000007", Decimals: 18, DecimalsKnown: true},
		},
		PoolState: &streamv1.PoolState{SqrtPriceX96: "79228162514264337593543950336", Liquidity: "10", Tick: "0", ActiveBinId: &bin},
	}
	if got := streamTrade(rawOf(trade)); !proto.Equal(got, trade) {
		t.Fatalf("streamTrade() = %v, want %v", got, trade)
	}
	plain := swap(memeWETH, leg{meme, "1", "2", 18}, leg{weth, "-1", "3", 18})
	plain.PoolState.ReservesAfter = nil
	plain.PoolState = nil
	if got := streamTrade(rawOf(plain)); got.GetPoolState() != nil {
		t.Fatalf("pool state = %v, want none", got.GetPoolState())
	}
}

func TestRestoredProcessorBackfills(t *testing.T) {
	for name, open := range stateStores(t) {
		t.Run(name, func(t *testing.T) {
			history := &fakeHistory{prices: wethPrices()}
			reference := &harness{t: t, processor: newBackfillProcessor(t, history, 100, 1)}
			persisted := &harness{t: t, processor: newBackfillProcessor(t, history, 100, 1), store: open(t)}
			blocks := lateMemeBlocks()
			for _, block := range blocks[:2] {
				history.store(block)
				reference.apply(block)
				persisted.apply(block)
			}
			persisted.restart()
			assertSameValues(t, persisted, reference)
			if _, ok := persisted.processor.missed.get(meme); !ok {
				t.Fatal("missed MEME not restored")
			}
			want := reference.apply(blocks[2])
			if got := persisted.apply(blocks[2]); got != want {
				t.Fatalf("backfill after restart:\n got %s\nwant %s", got, want)
			}
		})
	}
}
