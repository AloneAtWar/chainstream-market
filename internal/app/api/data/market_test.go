package data

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
)

// testOutput is one MEME trade, price and hourly candle at height, as
// market-engine writes them.
func testOutput(height int64, closePrice string) model.BlockOutput {
	blockTime := time.Unix(1_700_000_000+height*12, 0).UTC()
	price := decimal.RequireFromString(closePrice)
	return model.BlockOutput{
		ChainID: 1, Height: height, Hash: fmt.Sprintf("0x%d", height), BlockTime: blockTime,
		Trades: []model.Trade{{
			ChainID: 1, BlockHeight: height, BlockTime: blockTime, TransactionHash: "0xtx",
			LogIndex: 1, Protocol: "uniswap_v2", PoolID: "0xpool", Trader: "0xtrader", Side: model.SideBuy,
			BaseToken: "0xmeme", QuoteToken: "0xweth", BaseAmount: decimal.NewFromInt(100),
			QuoteAmount: decimal.RequireFromString("0.001"), PriceQuote: decimal.RequireFromString("0.00001"),
			PriceUSD: price, ValueUSD: decimal.NewFromInt(3),
		}},
		Prices: []model.TokenPrice{{
			ChainID: 1, Token: "0xmeme", BlockHeight: height, BlockTime: blockTime, PriceUSD: price,
			Source: model.PriceSourceGraph, Hops: 2, LiquidityUSD: decimal.NewFromInt(30000),
		}},
		Candles: []model.Candle{{
			ChainID: 1, Subject: model.CandleSubjectToken, Key: "0xmeme", Interval: time.Hour,
			OpenTime: blockTime.Truncate(time.Hour), Open: decimal.RequireFromString("0.03"),
			High: price, Low: decimal.RequireFromString("0.03"), Close: price,
			VolumeBase: decimal.NewFromInt(100), VolumeUSD: decimal.NewFromInt(3), Trades: uint32(height - 99),
			UpdateHeight: height,
		}},
	}
}

func assertDecimalEqual(t *testing.T, got decimal.Decimal, want string) {
	t.Helper()
	if !got.Equal(decimal.RequireFromString(want)) {
		t.Fatalf("value = %s, want %s", got, want)
	}
}

func TestMarketReaderReadsLatestValues(t *testing.T) {
	ctx := context.Background()
	db := openClickHouse(t)
	store := &marketReader{db: db}
	outlier := testOutput(101, "0.06")
	outlier.Trades[0].Outlier = true
	for _, block := range []model.BlockOutput{testOutput(100, "0.03"), outlier} {
		if err := chdb.InsertBlock(ctx, db, block); err != nil {
			t.Fatalf("InsertBlock(%d) error = %v", block.Height, err)
		}
	}

	price, ok, err := store.LatestPrice(ctx, 1, "0xmeme")
	if err != nil || !ok {
		t.Fatalf("LatestPrice() = %v, %v", ok, err)
	}
	if price.BlockHeight != 101 || price.Source != model.PriceSourceGraph || price.Hops != 2 {
		t.Fatalf("price = %+v", price)
	}
	assertDecimalEqual(t, price.PriceUSD, "0.06")
	if _, found, missingErr := store.LatestPrice(ctx, 1, "0xmissing"); missingErr != nil || found {
		t.Fatalf("missing LatestPrice() = %v, %v", found, missingErr)
	}

	candles, err := store.Candles(ctx, model.CandleQuery{
		ChainID: 1, Subject: model.CandleSubjectToken, Key: "0xmeme", Interval: time.Hour,
		From: time.Unix(0, 0), To: time.Unix(2_000_000_000, 0), Limit: 10,
	})
	if err != nil {
		t.Fatalf("Candles() error = %v", err)
	}
	// Both blocks fall in one hour bucket; only its newest version is read.
	if len(candles) != 1 || candles[0].UpdateHeight != 101 || candles[0].Trades != 2 {
		t.Fatalf("candles = %+v", candles)
	}
	assertDecimalEqual(t, candles[0].Close, "0.06")

	trades, err := store.Trades(ctx, model.TradeQuery{ChainID: 1, BaseToken: "0xmeme", Limit: 10})
	if err != nil {
		t.Fatalf("Trades() error = %v", err)
	}
	if len(trades) != 2 || trades[0].BlockHeight != 101 || trades[0].Side != model.SideBuy || !trades[0].Outlier || trades[1].Outlier {
		t.Fatalf("trades = %+v", trades)
	}
	paged, err := store.Trades(ctx, model.TradeQuery{ChainID: 1, PoolID: "0xpool", BeforeHeight: 101, Limit: 10})
	if err != nil {
		t.Fatalf("paged Trades() error = %v", err)
	}
	if len(paged) != 1 || paged[0].BlockHeight != 100 {
		t.Fatalf("paged trades = %+v", paged)
	}
	if _, err := store.Trades(ctx, model.TradeQuery{ChainID: 1, Limit: 10}); err == nil {
		t.Fatal("Trades() without a filter error = nil")
	}
}
