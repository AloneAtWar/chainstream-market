package biz

import (
	"context"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

func TestQuoteCandidateRanking(t *testing.T) {
	candidate := func(pool string, liquidity, depth int64, known bool) quoteCandidate {
		return quoteCandidate{
			price:    tokenPrice{Pool: pool, LiquidityUSD: decimal.NewFromInt(liquidity)},
			depthUSD: decimal.NewFromInt(depth), hasDepth: known,
		}
	}
	for _, test := range []struct {
		name           string
		left, right    quoteCandidate
		leftBeatsRight bool
	}{
		{"known depth over unknown", candidate("b", 10, 1, true), candidate("a", 1_000, 0, false), true},
		{"deeper wins", candidate("b", 10, 20, true), candidate("a", 1_000, 10, true), true},
		{"equal depth, larger balance", candidate("b", 20, 10, true), candidate("a", 10, 10, true), true},
		{"unknown depths by balance", candidate("b", 20, 0, false), candidate("a", 10, 0, false), true},
		{"full tie, lower pool ID", candidate("a", 10, 10, true), candidate("b", 10, 10, true), true},
		{"full tie, higher pool ID", candidate("b", 10, 10, true), candidate("a", 10, 10, true), false},
	} {
		if got := test.left.beats(test.right); got != test.leftBeatsRight {
			t.Errorf("%s: beats = %v", test.name, got)
		}
	}
}

// A StableSwap pool absorbs far more near the peg than a constant-product
// pool with a larger balance, so it prices USDX.
func TestDeeperPoolPricesAcrossProtocols(t *testing.T) {
	processor := newTestProcessor(t)
	teach(processor, map[string]uint32{usdc: 6, usdx: 18})
	block := testBlock(100, 0, swap("0xpool-usdx-usdc-v2",
		leg{usdx, "-1000000000000000000", "1500000000000000000000000", 18},
		leg{usdc, "1000000", "1500000000000", 6},
	))
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{stableSnapshot("1000000000000", "1000000000000000000000000")}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	price := priceMap(output.Prices)[usdx]
	assertDecimal(t, "USDX liquidity", price.LiquidityUSD, "1000000")
	if internal, _ := processor.prices.get(usdx); internal.Pool != stablePool {
		t.Fatalf("USDX priced through %s, want the stable pool", internal.Pool)
	}
}

// A pool whose depth is unknown only prices a token no pool with a known
// depth can.
func TestUnknownDepthRanksLast(t *testing.T) {
	processor := newTestProcessor(t)
	teach(processor, map[string]uint32{usdc: 6, usdx: 18})
	trade := swap("0xpool-unknown", leg{usdx, "-2000000000000000000", "", 18}, leg{usdc, "1000000", "", 6})
	trade.PoolState = nil
	block := testBlock(100, 0, trade, swap("0xpool-usdx-usdc-v2",
		leg{usdx, "-1000000000000000000", "100000000000000000000000", 18},
		leg{usdc, "1000000", "100000000000", 6},
	))
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{{
		PoolId: "0xpool-unknown", Protocol: "balancer_v3", Tokens: []string{usdx, usdc},
		Balances: []string{"40000000000000000000000000", "20000000000000"}, Curve: model.CurveUnknown,
	}}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	price := priceMap(output.Prices)[usdx]
	assertDecimal(t, "USDX price", price.PriceUSD, "1")
	assertDecimal(t, "USDX liquidity", price.LiquidityUSD, "100000")
}
