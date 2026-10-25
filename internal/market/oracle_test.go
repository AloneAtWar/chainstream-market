package market

import (
	"context"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

const (
	wbtc         = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
	usdcFeed     = "0xfeed00000000000000000000000000000000usdc"
	ethFeed      = "0xfeed000000000000000000000000000000000eth"
	btcFeed      = "0xfeed000000000000000000000000000000000btc"
	untrustedFee = "0xbad0000000000000000000000000000000000bad"
)

func newOracleProcessor(t *testing.T) *Processor {
	t.Helper()
	processor, err := NewProcessor(Options{
		Catalog:               DefaultCatalog(1),
		MinLiquidityUSD:       decimal.NewFromInt(1000),
		CandleMinLiquidityUSD: decimal.NewFromInt(10000),
		MaxHops:               3,
		Intervals:             []time.Duration{time.Minute},
		HistoryDepth:          16,
		OracleDeviation:       decimal.RequireFromString("0.02"),
		Feeds: []Feed{
			{Address: usdcFeed, Token: usdc, Decimals: 8},
			{Address: ethFeed, Token: NativeAddress, Decimals: 8},
			{Address: btcFeed, Token: wbtc, Decimals: 8},
		},
	})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	return processor
}

func oracleUpdate(feed, answer string) *streamv1.OracleUpdate {
	return &streamv1.OracleUpdate{Feed: feed, Answer: answer, RoundId: "1", UpdatedAt: startTime}
}

func TestOracleAnswerReplacesStablecoinPeg(t *testing.T) {
	processor := newOracleProcessor(t)
	block := testBlock(100, 0, wethSwap())
	block.OracleUpdates = []*streamv1.OracleUpdate{oracleUpdate(usdcFeed, "99800000")} // $0.998
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	prices := priceMap(output.Prices)
	assertDecimal(t, "USDC", prices[usdc].PriceUSD, "0.998")
	assertDecimal(t, "WETH", prices[weth].PriceUSD, "2994")
	assertDecimal(t, "WETH trade value", output.Trades[0].ValueUSD, "2994")
}

func TestOracleGuardsGraphPrice(t *testing.T) {
	tests := map[string]struct {
		answer string
		want   string
		source storage.PriceSource
	}{
		"within deviation keeps the graph price": {"301000000000", "3000", storage.PriceSourceGraph},
		"beyond deviation uses the oracle":       {"250000000000", "2500", storage.PriceSourceAnchor},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			processor := newOracleProcessor(t)
			block := testBlock(100, 0, wethSwap())
			block.OracleUpdates = []*streamv1.OracleUpdate{oracleUpdate(ethFeed, test.answer)}
			output, err := processor.Apply(context.Background(), block)
			if err != nil {
				t.Fatalf("Apply() error = %v", err)
			}
			price := priceMap(output.Prices)[weth]
			assertDecimal(t, "WETH", price.PriceUSD, test.want)
			if price.Source != test.source {
				t.Fatalf("WETH source = %d, want %d", price.Source, test.source)
			}
		})
	}
}

func TestOracleOnlyTokenAndUntrustedFeeds(t *testing.T) {
	processor := newOracleProcessor(t)
	block := testBlock(100, 0)
	block.OracleUpdates = []*streamv1.OracleUpdate{
		oracleUpdate(btcFeed, "6500000000000"),
		oracleUpdate(untrustedFee, "100000000"),
		oracleUpdate(usdcFeed, "-1"),
	}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	prices := priceMap(output.Prices)
	if len(prices) != 1 {
		t.Fatalf("prices = %+v, want only WBTC", prices)
	}
	assertDecimal(t, "WBTC", prices[wbtc].PriceUSD, "65000")
	if prices[wbtc].Source != storage.PriceSourceAnchor {
		t.Fatalf("WBTC source = %d", prices[wbtc].Source)
	}
}

func TestOracleAnswersRevert(t *testing.T) {
	ctx := context.Background()
	processor := newOracleProcessor(t)
	first := testBlock(100, 0)
	first.OracleUpdates = []*streamv1.OracleUpdate{oracleUpdate(usdcFeed, "99900000")}
	second := testBlock(101, 12)
	second.OracleUpdates = []*streamv1.OracleUpdate{oracleUpdate(usdcFeed, "95000000")}
	for _, block := range []*streamv1.BlockEvent{first, second} {
		if _, err := processor.Apply(ctx, block); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
	}
	if err := processor.Revert(ctx, 100); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	price, _ := processor.anchorPrice(usdc)
	assertDecimal(t, "USDC after revert", price, "0.999")
}

func TestNewProcessorRejectsBadFeeds(t *testing.T) {
	base := Options{MaxHops: 3, Intervals: []time.Duration{time.Minute}, HistoryDepth: 1}
	duplicate := base
	duplicate.Catalog = DefaultCatalog(1)
	// The native pseudo-address is an alias of WETH.
	duplicate.Feeds = []Feed{{Address: ethFeed, Token: weth}, {Address: ethFeed, Token: NativeAddress}}
	missing := base
	missing.Feeds = []Feed{{Address: ethFeed}}
	negative := base
	negative.OracleDeviation = decimal.NewFromInt(-1)
	negativeOutlier := base
	negativeOutlier.OutlierDeviation = decimal.NewFromInt(-1)
	for name, options := range map[string]Options{
		"duplicate": duplicate, "missing token": missing, "negative": negative, "negative outlier": negativeOutlier,
	} {
		if _, err := NewProcessor(options); err == nil {
			t.Fatalf("%s feeds accepted", name)
		}
	}
}

func TestOneFeedPricesSeveralTokens(t *testing.T) {
	const bridgedUSDC = "0x3333333333333333333333333333333333333333"
	processor, err := NewProcessor(Options{
		Catalog: NewCatalog(CatalogSpec{Anchors: []string{usdc, bridgedUSDC}}), MaxHops: 3,
		Intervals: []time.Duration{time.Minute}, HistoryDepth: 16,
		Feeds: []Feed{{Address: usdcFeed, Token: usdc, Decimals: 8}, {Address: usdcFeed, Token: bridgedUSDC, Decimals: 8}},
	})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	block := testBlock(100, 0)
	block.OracleUpdates = []*streamv1.OracleUpdate{oracleUpdate(usdcFeed, "99700000")}
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	prices := priceMap(output.Prices)
	assertDecimal(t, "USDC", prices[usdc].PriceUSD, "0.997")
	assertDecimal(t, "bridged USDC", prices[bridgedUSDC].PriceUSD, "0.997")
}
