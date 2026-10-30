package biz

import (
	"context"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

const (
	usdx   = "0x3333333333333333333333333333333333333333"
	xyz    = "0x4444444444444444444444444444444444444444"
	waUSDC = "0x5555555555555555555555555555555555555555"

	stablePool   = "0xpool-usdc-usdx-stable"
	weightedPool = "0xpool-meme-weth-weighted"
	cryptoPool   = "0xpool-xyz-weth-crypto"
	boostedPool  = "0xpool-wausdc-usdx-boosted"
)

// teach records token decimals below the tested blocks.
func teach(processor *Processor, decimals map[string]uint32) {
	for token, value := range decimals {
		processor.decimals.set(99, token, value)
	}
}

func stableSnapshot(usdcRaw, usdxRaw string) *streamv1.PoolBalanceSnapshot {
	return &streamv1.PoolBalanceSnapshot{
		PoolId: stablePool, Protocol: "curve_stableswap_ng", Tokens: []string{usdc, usdx},
		Balances: []string{usdcRaw, usdxRaw}, Curve: model.CurveStable, Fee: "100000000000000",
		Amp: "20000", AmpPrecision: "100", Rates: []string{"1000000000000000000000000000000", "1000000000000000000"},
	}
}

func TestBalancePoolPricesThroughStableCurve(t *testing.T) {
	processor := newTestProcessor(t)
	teach(processor, map[string]uint32{usdc: 6, usdx: 18})
	block := testBlock(100, 0)
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{stableSnapshot("1000000000000", "1000000000000000000000000")}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	price := priceMap(output.Prices)[usdx]
	assertDecimal(t, "USDX price", price.PriceUSD, "1")
	assertDecimal(t, "USDX liquidity", price.LiquidityUSD, "1000000")
	if price.Hops != 1 || price.Source != model.PriceSourceGraph {
		t.Fatalf("USDX price = %+v", price)
	}

	// With USDX three times as plentiful it trades below a dollar, and the
	// pool's USDC side alone counts as its liquidity.
	block = testBlock(101, 12)
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{stableSnapshot("500000000000", "1500000000000000000000000")}
	output, err = processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	price = priceMap(output.Prices)[usdx]
	if !price.PriceUSD.LessThan(decimal.NewFromInt(1)) || price.PriceUSD.LessThan(decimal.RequireFromString("0.95")) {
		t.Fatalf("imbalanced USDX price = %s", price.PriceUSD)
	}
	assertDecimal(t, "USDX liquidity", price.LiquidityUSD, "500000")

	record, _ := processor.balances.get(stablePool)
	narrow := processor.edge(stablePool, record, 1, 0)
	if !narrow.hasDepth || !narrow.depth.IsPositive() || narrow.depth.GreaterThan(decimal.NewFromInt(500_000)) {
		t.Fatalf("USDX→USDC edge = %+v", narrow)
	}
}

func TestBalancePoolPricesThroughWeightedCurve(t *testing.T) {
	processor := newTestProcessor(t)
	teach(processor, map[string]uint32{meme: 18})
	block := testBlock(100, 0, wethSwap())
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{{
		PoolId: weightedPool, Protocol: "balancer_v2", Tokens: []string{meme, weth},
		Balances: []string{"4000000000000000000000000", "100000000000000000000"}, Curve: model.CurveWeighted,
		Fee: "3000000000000000", Weights: []string{"800000000000000000", "200000000000000000"},
	}}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	price := priceMap(output.Prices)[meme]
	assertDecimal(t, "MEME price", price.PriceUSD, "0.3")
	assertDecimal(t, "MEME liquidity", price.LiquidityUSD, "300000")
	if internal, _ := processor.prices.get(meme); price.Hops != 2 || internal.Via != weth || internal.Pool != weightedPool {
		t.Fatalf("MEME price = %+v via %+v", price, internal)
	}
	record, _ := processor.balances.get(weightedPool)
	edge := processor.edge(weightedPool, record, 0, 1)
	// ±2% for a 0.3% fee: 100·(1 − 1.02^(−0.8)) ≈ 1.5718 WETH.
	if !edge.hasDepth || edge.depth.Sub(decimal.RequireFromString("1.5718")).Abs().GreaterThan(decimal.RequireFromString("0.001")) {
		t.Fatalf("MEME→WETH depth = %+v", edge)
	}
}

func TestBalancePoolPricesThroughCryptoLadder(t *testing.T) {
	processor := newTestProcessor(t)
	teach(processor, map[string]uint32{xyz: 18})
	block := testBlock(100, 0, wethSwap())
	snapshot := &streamv1.PoolBalanceSnapshot{
		PoolId: cryptoPool, Protocol: "curve_twocrypto_ng", SnapshotHeight: 100, Tokens: []string{xyz, weth},
		Balances: []string{"100000000000000000000000", "1000000000000000000000"}, Curve: model.CurveCrypto,
		Fee: "10000000000000000",
		Ladders: []*streamv1.QuoteLadder{{
			TokenIn: 0, TokenOut: 1,
			Amounts: []string{"100000000000000000000", "500000000000000000000", "1000000000000000000000"},
			Quotes:  []string{"990000000000000000", "4900000000000000000", "9700000000000000000"},
		}},
	}
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{snapshot}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	// 0.99 WETH for 100 XYZ after a 1% fee is 0.01 WETH, $30, an XYZ.
	assertDecimal(t, "XYZ price", priceMap(output.Prices)[xyz].PriceUSD, "30")

	// A snapshot without ladders keeps the last ones.
	snapshot.Ladders, snapshot.SnapshotHeight = nil, 101
	snapshot.Balances = []string{"100100000000000000000000", "999000000000000000000"}
	block = testBlock(101, 12)
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{snapshot}
	output, err = processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	record, _ := processor.balances.get(cryptoPool)
	if len(record.Ladders) != 1 || record.LadderHeight != 100 || record.SnapshotHeight != 101 {
		t.Fatalf("ladders = %+v at %d", record.Ladders, record.LadderHeight)
	}
	assertDecimal(t, "XYZ price", priceMap(output.Prices)[xyz].PriceUSD, "30")
}

func TestDerivedTokenPricedByRate(t *testing.T) {
	processor := newTestProcessor(t)
	teach(processor, map[string]uint32{usdc: 6, usdx: 18, waUSDC: 6})
	block := testBlock(100, 0)
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{{
		PoolId: boostedPool, Protocol: "balancer_v3", Tokens: []string{waUSDC, usdx},
		Balances: []string{"2000000000000", "2000000000000000000000000"}, Curve: model.CurveStable,
		Fee: "100000000000000", Amp: "1000000", AmpPrecision: "1000",
		Rates:   []string{"1050000000000000000000000000000", "1000000000000000000"},
		Derived: []*streamv1.DerivedToken{{Token: waUSDC, Underlyings: []string{usdc}, Rate: "1050000000000000000"}},
	}}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	prices := priceMap(output.Prices)
	wrapped := prices[waUSDC]
	assertDecimal(t, "waUSDC price", wrapped.PriceUSD, "1.05")
	assertDecimal(t, "waUSDC liquidity", wrapped.LiquidityUSD, "2100000")
	if internal, _ := processor.prices.get(waUSDC); internal.Via != usdc || internal.Pool != boostedPool {
		t.Fatalf("waUSDC price = %+v", internal)
	}
	// USDX is priced against waUSDC through the stable curve, which weighs
	// waUSDC by its rate: 2M waUSDC worth 2.1M USDC against 2M USDX.
	if usdxPrice, _ := processor.prices.get(usdx); usdxPrice.Via != waUSDC || !usdxPrice.USD.GreaterThan(decimal.NewFromInt(1)) {
		t.Fatalf("USDX price = %+v", usdxPrice)
	}
}

func TestUnknownBalancePoolUsesLatestSwapPrice(t *testing.T) {
	processor := newTestProcessor(t)
	trade := swap("0xpool-unknown",
		leg{usdx, "-2000000000000000000", "", 18},
		leg{usdc, "1000000", "", 6},
	)
	trade.PoolState = nil
	block := testBlock(100, 0, trade)
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{{
		PoolId: "0xpool-unknown", Protocol: "balancer_v3", Tokens: []string{usdx, usdc},
		Balances: []string{"4000000000000000000000000", "2000000000000"}, Curve: model.CurveUnknown,
		Reason: "hook adjusts swap amounts",
	}}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	price := priceMap(output.Prices)[usdx]
	assertDecimal(t, "USDX price", price.PriceUSD, "0.5")
	assertDecimal(t, "USDX liquidity", price.LiquidityUSD, "2000000")
}

func TestBalancePoolRevertAndRestore(t *testing.T) {
	processor := newTestProcessor(t)
	teach(processor, map[string]uint32{usdc: 6, usdx: 18})
	block := testBlock(100, 0)
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{stableSnapshot("1000000000000", "1000000000000000000000000")}
	if _, err := processor.Apply(context.Background(), block); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	block = testBlock(101, 12)
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{stableSnapshot("500000000000", "1500000000000000000000000")}
	if _, err := processor.Apply(context.Background(), block); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	delta, err := processor.TakeDelta()
	if err != nil {
		t.Fatalf("TakeDelta() error = %v", err)
	}
	restored := newTestProcessor(t)
	state := State{PrunedThrough: delta.PrunedThrough}
	for _, entry := range delta.Entries {
		if entry.Value != nil {
			state.Entries = append(state.Entries, entry)
		}
	}
	state.Undo = delta.Undo
	if err := restored.Restore(state); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	for _, target := range []*Processor{processor, restored} {
		if err := target.Revert(context.Background(), 100); err != nil {
			t.Fatalf("Revert() error = %v", err)
		}
		record, ok := target.balances.get(stablePool)
		if !ok || record.Balances[0] != "1000000000000" {
			t.Fatalf("record after revert = %+v, %v", record, ok)
		}
		if price, ok := target.balancePrice(stablePool, record, 1, 0); !ok || !price.Equal(decimal.NewFromInt(1)) {
			t.Fatalf("USDX price after revert = %s, %v", price, ok)
		}
		if _, ok := target.derivedQuote(100, usdx); ok {
			t.Fatal("USDX has a derived price")
		}
		if len(target.tokenBalancePools[usdx]) != 1 {
			t.Fatalf("USDX balance pools = %v", target.tokenBalancePools[usdx])
		}
	}
}
