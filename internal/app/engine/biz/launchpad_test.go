package biz

import (
	"context"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
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
