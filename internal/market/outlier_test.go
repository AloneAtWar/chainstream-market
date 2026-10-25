package market

import (
	"context"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

const thinWETH = "0xpool-weth-usdc-thin"

func newOutlierProcessor(t *testing.T, deviation string) *Processor {
	t.Helper()
	processor, err := NewProcessor(Options{
		Catalog:               DefaultCatalog(1),
		MinLiquidityUSD:       decimal.NewFromInt(1000),
		CandleMinLiquidityUSD: decimal.NewFromInt(10000),
		MaxHops:               3,
		Intervals:             []time.Duration{time.Minute},
		HistoryDepth:          16,
		OutlierDeviation:      decimal.RequireFromString(deviation),
	})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	return processor
}

// thinSwap buys 0.1 WETH at price USDC in a pool holding 10 WETH and
// 10*price USDC: enough for token candles, far too little to set WETH's price
// against the 3,000,000 USDC pool.
// It is logged after the main pool's swap.
func thinSwap(price int64) *streamv1.Trade {
	trade := swap(thinWETH,
		leg{weth, "-100000000000000000", "10000000000000000000", 18},
		leg{usdc, decimal.NewFromInt(price * 100_000).String(), decimal.NewFromInt(price * 10_000_000).String(), 6},
	)
	trade.LogIndex = 1
	return trade
}

func tradeIn(t *testing.T, trades []storage.Trade, pool string) storage.Trade {
	t.Helper()
	for _, trade := range trades {
		if trade.PoolID == pool {
			return trade
		}
	}
	t.Fatalf("no trade in pool %s", pool)
	return storage.Trade{}
}

func TestOutlierSwapStaysOutOfTokenCandles(t *testing.T) {
	tests := map[string]struct {
		deviation string
		price     int64
		outlier   bool
	}{
		"double the reference price": {"0.5", 6000, true},
		"half the reference price":   {"0.5", 1500, true},
		"within the deviation":       {"0.5", 3600, false},
		"filtering off":              {"0", 6000, false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			processor := newOutlierProcessor(t, test.deviation)
			output, err := processor.Apply(context.Background(), testBlock(100, 0, wethSwap(), thinSwap(test.price)))
			if err != nil {
				t.Fatalf("Apply() error = %v", err)
			}
			assertDecimal(t, "WETH price", priceMap(output.Prices)[weth].PriceUSD, "3000")
			if reference := tradeIn(t, output.Trades, wethUSDC); reference.Outlier {
				t.Fatal("a swap in the reference pool was marked an outlier")
			}
			thin := tradeIn(t, output.Trades, thinWETH)
			assertDecimal(t, "thin trade price", thin.PriceUSD, decimal.NewFromInt(test.price).String())
			if thin.Outlier != test.outlier {
				t.Fatalf("thin trade outlier = %v, want %v", thin.Outlier, test.outlier)
			}
			token := findCandle(t, output.Candles, storage.CandleSubjectToken, weth, time.Minute)
			wantTrades := uint32(2)
			if test.outlier {
				wantTrades = 1
				assertDecimal(t, "token candle high", token.High, "3000")
				assertDecimal(t, "token candle low", token.Low, "3000")
			}
			if token.Trades != wantTrades {
				t.Fatalf("token candle trades = %d, want %d", token.Trades, wantTrades)
			}
			// The thin pool's own candle still shows its trade.
			pool := findCandle(t, output.Candles, storage.CandleSubjectPool, thinWETH, time.Minute)
			assertDecimal(t, "thin pool candle close", pool.Close, decimal.NewFromInt(test.price).String())
		})
	}
}

func TestOutlierDecisionFollowsReorg(t *testing.T) {
	ctx := context.Background()
	processor := newOutlierProcessor(t, "0.5")
	if _, err := processor.Apply(ctx, testBlock(100, 0, wethSwap())); err != nil {
		t.Fatalf("Apply(100) error = %v", err)
	}
	output, err := processor.Apply(ctx, testBlock(101, 12, thinSwap(6000)))
	if err != nil {
		t.Fatalf("Apply(101) error = %v", err)
	}
	if !tradeIn(t, output.Trades, thinWETH).Outlier {
		t.Fatal("thin trade at twice the reference price is not an outlier")
	}
	if err = processor.Revert(ctx, 100); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	// On the new fork the main pool moved to $6000 in the same block, so the
	// same thin trade matches the reference price.
	moved := swap(wethUSDC,
		leg{weth, "-1000000000000000000", "1000000000000000000000", 18},
		leg{usdc, "6000000000", "6000000000000", 6},
	)
	output, err = processor.Apply(ctx, testBlock(101, 12, moved, thinSwap(6000)))
	if err != nil {
		t.Fatalf("Apply(101') error = %v", err)
	}
	if tradeIn(t, output.Trades, thinWETH).Outlier {
		t.Fatal("thin trade is an outlier after the reorg moved the reference price")
	}
	token := findCandle(t, output.Candles, storage.CandleSubjectToken, weth, time.Minute)
	if token.Trades != 3 {
		t.Fatalf("token candle trades = %d, want 3", token.Trades)
	}
}
