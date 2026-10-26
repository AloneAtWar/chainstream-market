package chstore

import (
	"context"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

func TestStoreReadsLatestValues(t *testing.T) {
	ctx := context.Background()
	store, _ := openTestStore(t)
	outlier := testOutput(101, "0.06")
	outlier.Trades[0].Outlier = true
	for _, block := range []model.BlockOutput{testOutput(100, "0.03"), outlier} {
		if err := store.WriteBlock(ctx, block); err != nil {
			t.Fatalf("WriteBlock(%d) error = %v", block.Height, err)
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
