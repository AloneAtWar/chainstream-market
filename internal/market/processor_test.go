package market

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

const (
	usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
	meme = "0x1111111111111111111111111111111111111111"
	dust = "0x2222222222222222222222222222222222222222"

	wethUSDC = "0xpool-weth-usdc"
	memeWETH = "0xpool-meme-weth"
	dustWETH = "0xpool-dust-weth"

	startTime = 1_700_000_000
)

func newTestProcessor(t *testing.T) *Processor {
	t.Helper()
	processor, err := NewProcessor(Options{
		Catalog:               asset.DefaultCatalog(1),
		MinLiquidityUSD:       decimal.NewFromInt(1000),
		CandleMinLiquidityUSD: decimal.NewFromInt(10000),
		MaxHops:               3,
		Intervals:             []time.Duration{time.Minute, 5 * time.Minute},
		HistoryDepth:          16,
	})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	return processor
}

type leg struct {
	token    string
	delta    string
	reserve  string
	decimals uint32
}

func swap(pool string, legs ...leg) *streamv1.Trade {
	trade := &streamv1.Trade{
		TransactionHash: "0xtx-" + pool, Protocol: "uniswap_v2", PoolId: pool,
		Trader: "0xtrader", Action: streamv1.TradeAction_TRADE_ACTION_SWAP,
		PoolState: &streamv1.PoolState{},
	}
	for _, value := range legs {
		trade.Amounts = append(trade.Amounts, &streamv1.Amount{
			Asset: value.token, Raw: value.delta, Decimals: value.decimals, DecimalsKnown: true,
		})
		trade.PoolState.ReservesAfter = append(trade.PoolState.ReservesAfter, value.reserve)
	}
	return trade
}

func testBlock(height int64, offset int64, trades ...*streamv1.Trade) *streamv1.BlockEvent {
	return &streamv1.BlockEvent{
		ChainId: 1, Chain: "eth", BlockHeight: height, BlockHash: "0xhash",
		BlockTime: startTime + offset, Trades: trades,
	}
}

// Reserves: 1000 WETH / 3,000,000 USDC, so WETH is $3000.
func wethSwap() *streamv1.Trade {
	return swap(wethUSDC,
		leg{weth, "-1000000000000000000", "1000000000000000000000", 18},
		leg{usdc, "3000000000", "3000000000000", 6},
	)
}

// Reserves: 1,000,000 MEME / 10 WETH, so MEME is $0.03 through WETH.
// The trader sells 100 MEME for 0.001 WETH.
func memeSell() *streamv1.Trade {
	return swap(memeWETH,
		leg{meme, "100000000000000000000", "1000000000000000000000000", 18},
		leg{weth, "-1000000000000000", "10000000000000000000", 18},
	)
}

func priceMap(prices []storage.TokenPrice) map[string]storage.TokenPrice {
	result := make(map[string]storage.TokenPrice, len(prices))
	for _, price := range prices {
		result[price.Token] = price
	}
	return result
}

func findCandle(t *testing.T, candles []storage.Candle, subject storage.CandleSubject, key string, interval time.Duration) storage.Candle {
	t.Helper()
	for _, candle := range candles {
		if candle.Subject == subject && candle.Key == key && candle.Interval == interval {
			return candle
		}
	}
	t.Fatalf("candle %d/%s/%s not found in %d candles", subject, key, interval, len(candles))
	return storage.Candle{}
}

func assertDecimal(t *testing.T, name string, got decimal.Decimal, want string) {
	t.Helper()
	if !got.Equal(decimal.RequireFromString(want)) {
		t.Fatalf("%s = %s, want %s", name, got, want)
	}
}

func TestProcessorPricesThroughGraphWithinOneBlock(t *testing.T) {
	processor := newTestProcessor(t)
	// The long-tail trade comes first; pricing still reaches it in a later pass.
	output, err := processor.Apply(context.Background(), testBlock(100, 0, memeSell(), wethSwap()))
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	prices := priceMap(output.Prices)
	if got := prices[usdc]; got.Source != storage.PriceSourceAnchor {
		t.Fatalf("USDC price = %+v, want anchor", got)
	}
	wethPrice := prices[weth]
	assertDecimal(t, "WETH price", wethPrice.PriceUSD, "3000")
	if wethPrice.Hops != 1 || wethPrice.Source != storage.PriceSourceGraph {
		t.Fatalf("WETH price = %+v", wethPrice)
	}
	assertDecimal(t, "WETH liquidity", wethPrice.LiquidityUSD, "3000000")
	memePrice := prices[meme]
	assertDecimal(t, "MEME price", memePrice.PriceUSD, "0.03")
	if memePrice.Hops != 2 {
		t.Fatalf("MEME hops = %d, want 2", memePrice.Hops)
	}

	if len(output.Trades) != 2 {
		t.Fatalf("trades = %d, want 2", len(output.Trades))
	}
	memeTrade := output.Trades[0]
	if memeTrade.BaseToken != meme || memeTrade.QuoteToken != weth || memeTrade.Side != storage.SideSell {
		t.Fatalf("MEME trade orientation = %+v", memeTrade)
	}
	assertDecimal(t, "MEME trade base", memeTrade.BaseAmount, "100")
	assertDecimal(t, "MEME trade price", memeTrade.PriceQuote, "0.00001")
	assertDecimal(t, "MEME trade USD price", memeTrade.PriceUSD, "0.03")
	assertDecimal(t, "MEME trade value", memeTrade.ValueUSD, "3")

	wethTrade := output.Trades[1]
	if wethTrade.BaseToken != weth || wethTrade.QuoteToken != usdc || wethTrade.Side != storage.SideBuy {
		t.Fatalf("WETH trade orientation = %+v", wethTrade)
	}
	assertDecimal(t, "WETH trade USD price", wethTrade.PriceUSD, "3000")
}

func TestProcessorSkipsIlliquidPoolsForPricingAndTokenCandles(t *testing.T) {
	processor := newTestProcessor(t)
	// 0.1 WETH of counterpart liquidity is $300, below the $1000 floor.
	dustBuy := swap(dustWETH,
		leg{dust, "-1000000000000000000000", "1000000000000000000000000", 18},
		leg{weth, "100000000000000", "100000000000000000", 18},
	)
	output, err := processor.Apply(context.Background(), testBlock(100, 0, wethSwap(), dustBuy))
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if _, ok := priceMap(output.Prices)[dust]; ok {
		t.Fatal("DUST was priced through an illiquid pool")
	}
	trade := output.Trades[1]
	if trade.BaseToken != dust || trade.Side != storage.SideBuy {
		t.Fatalf("DUST trade = %+v", trade)
	}
	// The trade is still valued through its quote token.
	assertDecimal(t, "DUST trade value", trade.ValueUSD, "0.3")
	findCandle(t, output.Candles, storage.CandleSubjectPool, dustWETH, time.Minute)
	for _, candle := range output.Candles {
		if candle.Subject == storage.CandleSubjectToken && candle.Key == dust {
			t.Fatal("illiquid pool joined DUST's aggregated candles")
		}
	}
}

func TestProcessorCandlesOpenAtPreviousCloseAndRevert(t *testing.T) {
	ctx := context.Background()
	processor := newTestProcessor(t)
	if _, err := processor.Apply(ctx, testBlock(100, 0, wethSwap(), memeSell())); err != nil {
		t.Fatalf("Apply(100) error = %v", err)
	}
	// One minute later MEME reserves halve and someone buys at $0.06.
	memeBuy := swap(memeWETH,
		leg{meme, "-100000000000000000000", "500000000000000000000000", 18},
		leg{weth, "2000000000000000", "10000000000000000000", 18},
	)
	output, err := processor.Apply(ctx, testBlock(101, 60, memeBuy))
	if err != nil {
		t.Fatalf("Apply(101) error = %v", err)
	}
	minute := findCandle(t, output.Candles, storage.CandleSubjectToken, meme, time.Minute)
	assertDecimal(t, "1m open", minute.Open, "0.03")
	assertDecimal(t, "1m close", minute.Close, "0.06")
	assertDecimal(t, "1m low", minute.Low, "0.03")
	if minute.Trades != 1 || !minute.OpenTime.Equal(time.Unix(startTime+60, 0).Truncate(time.Minute)) {
		t.Fatalf("1m candle = %+v", minute)
	}
	fiveMinutes := findCandle(t, output.Candles, storage.CandleSubjectToken, meme, 5*time.Minute)
	if fiveMinutes.Trades != 2 {
		t.Fatalf("5m trades = %d, want 2", fiveMinutes.Trades)
	}
	assertDecimal(t, "5m volume", fiveMinutes.VolumeUSD, "9")

	if err := processor.Revert(ctx, 100); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	price, _ := processor.priceOf(meme)
	assertDecimal(t, "MEME price after revert", price.USD, "0.03")
	candle, _ := processor.candles.get(candleKey{storage.CandleSubjectToken, meme, 5 * time.Minute})
	if candle.Trades != 1 {
		t.Fatalf("5m trades after revert = %d, want 1", candle.Trades)
	}
}

func TestProcessorRejectsReorgBelowHistory(t *testing.T) {
	processor := newTestProcessor(t)
	block := testBlock(100, 0, wethSwap())
	block.FinalizedHeight = 90
	if _, err := processor.Apply(context.Background(), block); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if err := processor.Revert(context.Background(), 89); !errors.Is(err, ErrReorgTooDeep) {
		t.Fatalf("Revert(89) error = %v, want ErrReorgTooDeep", err)
	}
	if err := processor.Revert(context.Background(), 95); err != nil {
		t.Fatalf("Revert(95) error = %v", err)
	}
}

func TestOrientationFixesOnceAQuoteIsKnown(t *testing.T) {
	ctx := context.Background()
	processor := newTestProcessor(t)
	pair := "0xpool-meme-dust"
	trade := func() *streamv1.Trade {
		return swap(pair,
			leg{meme, "-5000000000000000000", "1000000000000000000000", 18},
			leg{dust, "1000000000000000000", "1000000000000000000000", 18},
		)
	}
	output, err := processor.Apply(ctx, testBlock(100, 0, trade()))
	if err != nil {
		t.Fatalf("Apply(100) error = %v", err)
	}
	if got, _ := processor.orientations.get(pair); got.Fixed || output.Trades[0].QuoteToken != dust {
		t.Fatalf("unpriced pair orientation = %+v, trade quote %s", got, output.Trades[0].QuoteToken)
	}
	// MEME becomes priced, so it becomes the quote and stays fixed.
	if _, err := processor.Apply(ctx, testBlock(101, 12, wethSwap(), memeSell(), trade())); err != nil {
		t.Fatalf("Apply(101) error = %v", err)
	}
	if got, _ := processor.orientations.get(pair); !got.Fixed || got.Quote != meme {
		t.Fatalf("orientation = %+v, want fixed MEME quote", got)
	}
}

func TestNativeAliasSharesWrappedPrice(t *testing.T) {
	processor := newTestProcessor(t)
	nativeSwap := swap("0xpool-eth-usdc",
		leg{asset.NativeAddress, "-1000000000000000000", "1000000000000000000000", 18},
		leg{usdc, "3000000000", "3000000000000", 6},
	)
	if _, err := processor.Apply(context.Background(), testBlock(100, 0, nativeSwap)); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	price, ok := processor.priceOf(asset.NativeAddress)
	if !ok {
		t.Fatal("native coin not priced")
	}
	assertDecimal(t, "native price", price.USD, "3000")
	if wrapped, _ := processor.priceOf(weth); !wrapped.USD.Equal(price.USD) {
		t.Fatalf("WETH price %s differs from native %s", wrapped.USD, price.USD)
	}
}

func TestConcentratedState(t *testing.T) {
	// sqrtPrice = 2 means price 4 and, with L = 1e18 and 18 decimals,
	// virtual reserves of 0.5 and 2.
	sqrt := q96.Mul(decimal.NewFromInt(2)).String()
	reserves, price, ok := concentratedState(sqrt, "1000000000000000000", 18, 18)
	if !ok {
		t.Fatal("concentratedState() ok = false")
	}
	assertDecimal(t, "price", price, "4")
	assertDecimal(t, "reserve0", reserves[0], "0.5")
	assertDecimal(t, "reserve1", reserves[1], "2")
	if _, _, ok := concentratedState("0", "1", 18, 18); ok {
		t.Fatal("zero sqrt price accepted")
	}
}

func TestNewProcessorValidatesOptions(t *testing.T) {
	valid := Options{MaxHops: 3, Intervals: []time.Duration{time.Minute}, HistoryDepth: 1}
	invalid := []Options{
		{Intervals: valid.Intervals, HistoryDepth: 1},
		{MaxHops: 3, HistoryDepth: 1},
		{MaxHops: 3, Intervals: []time.Duration{1500 * time.Millisecond}, HistoryDepth: 1},
		{MaxHops: 3, Intervals: valid.Intervals},
	}
	for index, options := range invalid {
		if _, err := NewProcessor(options); err == nil {
			t.Fatalf("options %d accepted", index)
		}
	}
	if _, err := NewProcessor(valid); err != nil {
		t.Fatalf("valid options error = %v", err)
	}
}
