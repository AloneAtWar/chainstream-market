package biz

import (
	"context"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

const (
	launched  = "0x3333333333333333333333333333333333333333"
	curvePool = "0x5c952063c7fc8610ffdb798152d69f0b9550762b:0x3333333333333333333333333333333333333333"
)

// curveBuy buys 1,000,000 tokens for 0.1 WETH. The curve then holds 2 WETH
// and quotes 1e-7 WETH per token: 1e11 wei per 1e18 raw units.
func curveBuy() *streamv1.Trade {
	return &streamv1.Trade{
		TransactionHash: "0xtx-curve", Protocol: "fourmeme", PoolId: curvePool,
		Trader: "0xtrader", Action: streamv1.TradeAction_TRADE_ACTION_SWAP,
		Amounts: []*streamv1.Amount{
			{Asset: launched, Raw: "-1000000000000000000000000", Decimals: 18, DecimalsKnown: true},
			{Asset: weth, Raw: "100000000000000000", Decimals: 18, DecimalsKnown: true},
		},
		PoolState: &streamv1.PoolState{Curve: &streamv1.CurveState{
			Progress: "250000000000000000", BaseRemaining: "600000000000000000000000000",
			QuoteRaised: "2000000000000000000", Price: "100000000000",
		}},
	}
}

func TestProcessorPricesBondingCurveUntilMigration(t *testing.T) {
	processor := newTestProcessor(t) // MinLiquidityUSD 1000, curve threshold 0
	output, err := processor.Apply(context.Background(), testBlock(100, 0, wethSwap(), curveBuy()))
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	// 2 WETH is $6000 of liquidity, which passes the curve threshold of 0.
	price, ok := priceMap(output.Prices)[launched]
	if !ok {
		t.Fatalf("launched token has no price in %#v", output.Prices)
	}
	assertDecimal(t, "launched price", price.PriceUSD, "0.0003")
	assertDecimal(t, "launched liquidity", price.LiquidityUSD, "6000")
	if price.Hops != 2 {
		t.Fatalf("hops = %d, want 2", price.Hops)
	}
	var trade model.Trade
	for _, candidate := range output.Trades {
		if candidate.PoolID == curvePool {
			trade = candidate
		}
	}
	if trade.BaseToken != launched || trade.Side != model.SideBuy {
		t.Fatalf("curve trade = %#v", trade)
	}
	findCandle(t, output.Candles, model.CandleSubjectToken, launched, processor.options.Intervals[0])

	migrate := testBlock(101, 12)
	migrate.LaunchpadEvents = []*streamv1.LaunchpadEvent{{
		Type: streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_MIGRATED, PoolId: curvePool,
		Token: launched, Quote: weth, TargetPoolId: "0xpair",
	}}
	if _, err := processor.Apply(context.Background(), migrate); err != nil {
		t.Fatalf("Apply() migrate error = %v", err)
	}
	state, _ := processor.pools.get(curvePool)
	if !state.Curve || state.HasReserves || !state.HasPrice {
		t.Fatalf("migrated curve = %#v, want a price without reserves", state)
	}
	if _, ok := processor.bestQuote(101, launched); ok {
		t.Fatal("a migrated curve still prices its token")
	}
	if err := processor.Revert(context.Background(), 100); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	if state, _ := processor.pools.get(curvePool); !state.HasReserves {
		t.Fatal("revert did not restore the curve reserves")
	}
}

func TestProcessorHoldsCurvesToTheirOwnThreshold(t *testing.T) {
	processor := newTestProcessor(t)
	processor.options.CurveMinLiquidityUSD = processor.options.MinLiquidityUSD.Mul(processor.options.MinLiquidityUSD)
	output, err := processor.Apply(context.Background(), testBlock(100, 0, wethSwap(), curveBuy()))
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if _, ok := priceMap(output.Prices)[launched]; ok {
		t.Fatal("a curve below its threshold priced its token")
	}
}

func migratedEvent() *streamv1.LaunchpadEvent {
	return &streamv1.LaunchpadEvent{
		Type: streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_MIGRATED, PoolId: curvePool,
		Token: launched, Quote: weth, TargetPoolId: "0xpair",
	}
}

// A curve that migrates before its token's decimals are known stays migrated:
// the held curve trade replayed once the decimals arrive must not bring the
// curve's reserves back.
func TestProcessorKeepsMigrationOfCurveWithoutState(t *testing.T) {
	processor := newTestProcessor(t)
	processor.options.LateTradeBlocks, processor.options.LateTradesPerToken = 16, 16
	held := curveBuy()
	held.Amounts[0].Decimals, held.Amounts[0].DecimalsKnown = 0, false
	if _, err := processor.Apply(context.Background(), testBlock(100, 0, wethSwap(), held)); err != nil {
		t.Fatalf("Apply(100) error = %v", err)
	}
	if _, ok := processor.pools.get(curvePool); ok {
		t.Fatal("a curve trade without decimals created a pool")
	}

	migrate := testBlock(101, 12)
	migrate.LaunchpadEvents = []*streamv1.LaunchpadEvent{migratedEvent()}
	if _, err := processor.Apply(context.Background(), migrate); err != nil {
		t.Fatalf("Apply(101) error = %v", err)
	}
	if state, ok := processor.pools.get(curvePool); !ok || !state.Migrated || state.HasReserves || state.HasPrice {
		t.Fatalf("placeholder = %#v (exists %v), want migrated without reserves or price", state, ok)
	}

	// The DEX pair brings the decimals: 1,000,000 tokens against 1 WETH.
	pair := swap("0xpair",
		leg{launched, "-1000000000000000000", "1000000000000000000000000", 18},
		leg{weth, "1000000", "1000000000000000000", 18},
	)
	output, err := processor.Apply(context.Background(), testBlock(102, 24, pair))
	if err != nil {
		t.Fatalf("Apply(102) error = %v", err)
	}
	state, _ := processor.pools.get(curvePool)
	if !state.Migrated || state.HasReserves || !state.HasPrice {
		t.Fatalf("seeded curve = %#v, want migrated with a price and no reserves", state)
	}
	replayed := false
	for _, trade := range output.Trades {
		replayed = replayed || trade.PoolID == curvePool
	}
	if !replayed {
		t.Fatal("the held curve trade was not replayed")
	}
	if price, ok := priceMap(output.Prices)[launched]; ok && price.LiquidityUSD.GreaterThan(decimal.NewFromInt(3000)) {
		t.Fatalf("launched price %#v still draws on the migrated curve", price)
	}

	if err := processor.Revert(context.Background(), 100); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	if _, ok := processor.pools.get(curvePool); ok {
		t.Fatal("revert kept the migration placeholder")
	}
}

// The trade that fills a curve and its migration share a block. The trade
// happened before the migration, so its token candle is judged against the
// curve's liquidity then, not the cleared reserves.
func TestProcessorJudgesMigratingCurveTradesBeforeTheMigration(t *testing.T) {
	processor := newTestProcessor(t)
	processor.options.CurveMinLiquidityUSD = decimal.NewFromInt(1000)
	block := testBlock(100, 0, wethSwap(), curveBuy())
	block.LaunchpadEvents = []*streamv1.LaunchpadEvent{migratedEvent()}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	findCandle(t, output.Candles, model.CandleSubjectPool, curvePool, processor.options.Intervals[0])
	findCandle(t, output.Candles, model.CandleSubjectToken, launched, processor.options.Intervals[0])
	if state, _ := processor.pools.get(curvePool); state.HasReserves || !state.Migrated {
		t.Fatalf("curve after the block = %#v, want migrated without reserves", state)
	}
	if _, ok := processor.bestQuote(100, launched); ok {
		t.Fatal("a migrated curve still prices its token at the block's close")
	}
}
