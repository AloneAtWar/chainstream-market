package market

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

func newLateProcessor(t *testing.T, blocks int64, perToken int) *Processor {
	t.Helper()
	processor, err := NewProcessor(Options{
		Catalog:               asset.DefaultCatalog(1),
		MinLiquidityUSD:       decimal.NewFromInt(1000),
		CandleMinLiquidityUSD: decimal.NewFromInt(10000),
		MaxHops:               3,
		Intervals:             []time.Duration{time.Minute, 5 * time.Minute},
		HistoryDepth:          16,
		LateTradeBlocks:       blocks,
		LateTradesPerToken:    perToken,
	})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	return processor
}

// undecided marks token's amounts in trade as having unknown decimals, the
// way chainstream-evm publishes them before it has read the metadata.
func undecided(trade *streamv1.Trade, token string, logIndex uint32) *streamv1.Trade {
	for _, amount := range trade.GetAmounts() {
		if amount.GetAsset() == token {
			amount.Decimals, amount.DecimalsKnown = 0, false
		}
	}
	trade.LogIndex = logIndex
	return trade
}

func assetFact(token, recognition string, decimals uint8) *streamv1.AssetFact {
	return &streamv1.AssetFact{
		Address: token, Standard: "erc20", Recognition: recognition, MetadataComplete: true,
		Type:         streamv1.AssetFactType_ASSET_FACT_TYPE_OBSERVED,
		MetadataJson: fmt.Appendf(nil, `{"Address":%q,"Decimals":{"Value":%d,"Known":true}}`, token, decimals),
	}
}

func apply(t *testing.T, processor *Processor, block *streamv1.BlockEvent) storage.BlockOutput {
	t.Helper()
	output, err := processor.Apply(context.Background(), block)
	if err != nil {
		t.Fatalf("Apply(%d) error = %v", block.GetBlockHeight(), err)
	}
	return output
}

func TestLateTradeReplayedWhenMetadataArrives(t *testing.T) {
	processor := newLateProcessor(t, 100, 10)
	held := apply(t, processor, testBlock(100, 0, wethSwap(), undecided(memeSell(), meme, 1)))
	if len(held.Trades) != 1 || held.Trades[0].PoolID != wethUSDC {
		t.Fatalf("block 100 trades = %+v, want only the WETH trade", held.Trades)
	}
	if _, ok := priceMap(held.Prices)[meme]; ok {
		t.Fatal("MEME priced without decimals")
	}

	block := testBlock(101, 12)
	block.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
	output := apply(t, processor, block)
	if len(output.Trades) != 1 {
		t.Fatalf("block 101 trades = %d, want the replayed trade", len(output.Trades))
	}
	trade := output.Trades[0]
	if trade.BlockHeight != 100 || !trade.BlockTime.Equal(time.Unix(startTime, 0)) ||
		trade.Side != storage.SideSell || trade.BaseToken != meme || trade.QuoteToken != weth {
		t.Fatalf("replayed trade = %+v", trade)
	}
	// Valued at block 100's WETH price.
	assertDecimal(t, "price usd", trade.PriceUSD, "0.03")
	assertDecimal(t, "value usd", trade.ValueUSD, "3")

	// The held trade's pool state seeds the graph, so MEME is priced now.
	price, ok := priceMap(output.Prices)[meme]
	if !ok || price.Source != storage.PriceSourceGraph {
		t.Fatalf("MEME price = %+v, %v", price, ok)
	}
	assertDecimal(t, "meme price", price.PriceUSD, "0.03")

	poolCandle := findCandle(t, output.Candles, storage.CandleSubjectPool, memeWETH, time.Minute)
	if !poolCandle.OpenTime.Equal(time.Unix(startTime, 0).Truncate(time.Minute)) ||
		poolCandle.UpdateHeight != 101 || poolCandle.Trades != 1 {
		t.Fatalf("pool candle = %+v", poolCandle)
	}
	assertDecimal(t, "pool close", poolCandle.Close, "0.03")
	findCandle(t, output.Candles, storage.CandleSubjectToken, meme, time.Minute)
	if len(processor.late.values) != 0 || len(processor.lateByToken) != 0 {
		t.Fatalf("held trades remain: %d", len(processor.late.values))
	}
}

func TestLateTradesReplayAheadOfTheBlock(t *testing.T) {
	processor := newLateProcessor(t, 100, 10)
	apply(t, processor, testBlock(100, 0, wethSwap(), undecided(memeSell(), meme, 1)))
	// 100 MEME for 0.002 WETH: $0.06, in the next minute bucket.
	pricier := swap(memeWETH,
		leg{meme, "100000000000000000000", "1000000000000000000000000", 18},
		leg{weth, "-2000000000000000", "10000000000000000000", 18},
	)
	apply(t, processor, testBlock(106, 72, undecided(pricier, meme, 1)))
	// The block's own MEME trade brings the decimals.
	output := apply(t, processor, testBlock(107, 84, memeSell()))

	if len(output.Trades) != 3 {
		t.Fatalf("trades = %d, want 2 replayed and 1 own", len(output.Trades))
	}
	for index, want := range []int64{100, 106, 107} {
		if output.Trades[index].BlockHeight != want {
			t.Fatalf("trade %d height = %d, want %d", index, output.Trades[index].BlockHeight, want)
		}
	}
	seen := make(map[string]bool)
	var minutes []storage.Candle
	for _, candle := range output.Candles {
		id := fmt.Sprintf("%d/%s/%s/%d", candle.Subject, candle.Key, candle.Interval, candle.OpenTime.Unix())
		if seen[id] {
			t.Fatalf("bucket %s emitted twice", id)
		}
		seen[id] = true
		if candle.Subject == storage.CandleSubjectPool && candle.Interval == time.Minute {
			minutes = append(minutes, candle)
		}
	}
	if len(minutes) != 2 {
		t.Fatalf("pool minute buckets = %+v, want 2", minutes)
	}
	first, second := minutes[0], minutes[1]
	if first.Trades != 1 || second.Trades != 2 || !second.OpenTime.After(first.OpenTime) {
		t.Fatalf("buckets = %+v / %+v", first, second)
	}
	assertDecimal(t, "second open", second.Open, "0.03")
	assertDecimal(t, "second high", second.High, "0.06")
	assertDecimal(t, "second close", second.Close, "0.03")
	current, _ := processor.candles.get(candleKey{Subject: storage.CandleSubjectPool, Key: memeWETH, Interval: time.Minute})
	if !current.OpenTime.Equal(second.OpenTime) || current.Trades != 2 {
		t.Fatalf("current bucket = %+v", current)
	}
	fiveMinutes := findCandle(t, output.Candles, storage.CandleSubjectPool, memeWETH, 5*time.Minute)
	if fiveMinutes.Trades != 3 {
		t.Fatalf("5m bucket trades = %d, want 3", fiveMinutes.Trades)
	}
}

func TestLateTradeReplayReverts(t *testing.T) {
	processor := newLateProcessor(t, 100, 10)
	apply(t, processor, testBlock(100, 0, wethSwap(), undecided(memeSell(), meme, 1)))
	block := testBlock(101, 12)
	block.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
	if got := apply(t, processor, block); len(got.Trades) != 1 {
		t.Fatalf("replayed trades = %d", len(got.Trades))
	}
	if err := processor.Revert(context.Background(), 100); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	if _, ok := processor.decimals.get(meme); ok || len(processor.late.values) != 1 || len(processor.lateByToken[meme]) != 1 {
		t.Fatalf("after revert: held %d, index %d", len(processor.late.values), len(processor.lateByToken[meme]))
	}
	if _, ok := processor.pools.get(memeWETH); ok {
		t.Fatal("seeded pool survived the revert")
	}
	if got := apply(t, processor, testBlock(101, 12)); len(got.Trades) != 0 {
		t.Fatalf("fork block 101 trades = %d", len(got.Trades))
	}
	block = testBlock(102, 24)
	block.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
	output := apply(t, processor, block)
	if len(output.Trades) != 1 || output.Trades[0].BlockHeight != 100 {
		t.Fatalf("trades after fork = %+v", output.Trades)
	}
}

func TestLateTradesExpireAndAreCapped(t *testing.T) {
	processor := newLateProcessor(t, 3, 2)
	apply(t, processor, testBlock(100, 0, wethSwap(),
		undecided(memeSell(), meme, 1), undecided(memeSell(), meme, 2), undecided(memeSell(), meme, 3)))
	if len(processor.late.values) != 2 {
		t.Fatalf("held = %d, want the cap of 2", len(processor.late.values))
	}
	apply(t, processor, testBlock(101, 12))
	apply(t, processor, testBlock(102, 24))
	if len(processor.late.values) != 2 {
		t.Fatalf("held at 102 = %d, want 2", len(processor.late.values))
	}
	apply(t, processor, testBlock(103, 36))
	if len(processor.late.values) != 0 || len(processor.lateOrder) != 0 || len(processor.lateByToken) != 0 {
		t.Fatalf("held at 103 = %d, want none", len(processor.late.values))
	}
	block := testBlock(104, 48)
	block.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
	if output := apply(t, processor, block); len(output.Trades) != 0 {
		t.Fatalf("expired trades replayed: %d", len(output.Trades))
	}
}

func TestUnrecognizedMetadataIsIgnored(t *testing.T) {
	processor := newLateProcessor(t, 100, 10)
	apply(t, processor, testBlock(100, 0, wethSwap(), undecided(memeSell(), meme, 1)))
	block := testBlock(101, 12)
	block.Assets = []*streamv1.AssetFact{assetFact(meme, "not_recognized", 18)}
	if output := apply(t, processor, block); len(output.Trades) != 0 {
		t.Fatalf("trades = %d, want none", len(output.Trades))
	}
	if _, ok := processor.decimals.get(meme); ok || len(processor.late.values) != 1 {
		t.Fatalf("decimals learned from unrecognized metadata; held = %d", len(processor.late.values))
	}
}

// A trade published without decimals is valued at once when the processor
// already knows them.
func TestKnownDecimalsFillUnknownAmounts(t *testing.T) {
	processor := newLateProcessor(t, 100, 10)
	block := testBlock(100, 0, wethSwap(), undecided(memeSell(), meme, 1))
	block.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
	output := apply(t, processor, block)
	if len(output.Trades) != 2 || len(processor.late.values) != 0 {
		t.Fatalf("trades = %d, held = %d", len(output.Trades), len(processor.late.values))
	}
	assertDecimal(t, "price usd", output.Trades[1].PriceUSD, "0.03")
}

// lateChainBlock holds a MEME trade every block until its metadata arrives
// at height 110.
func lateChainBlock(height, variant int64) *streamv1.BlockEvent {
	block := chainBlock(height, variant)
	if height < 110 {
		block.Trades[1] = undecided(block.Trades[1], meme, 1)
	}
	if height == 110 {
		block.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
		block.Trades = block.Trades[:1]
	}
	return block
}

func TestRestoredProcessorReplaysLateTrades(t *testing.T) {
	for name, open := range stateStores(t) {
		t.Run(name, func(t *testing.T) {
			reference := &harness{t: t, processor: newLateProcessor(t, 100, 100)}
			persisted := &harness{t: t, processor: newLateProcessor(t, 100, 100), store: open(t)}
			for height := int64(100); height < 108; height++ {
				reference.apply(lateChainBlock(height, 0))
				persisted.apply(lateChainBlock(height, 0))
			}
			persisted.restart()
			assertSameValues(t, persisted, reference)
			// The reorg drops held trades from 106 and 107; the fork holds others.
			reference.revert(105)
			persisted.revert(105)
			persisted.restart()
			assertSameValues(t, persisted, reference)
			for height := int64(106); height < 115; height++ {
				want := reference.apply(lateChainBlock(height, 1))
				if got := persisted.apply(lateChainBlock(height, 1)); got != want {
					t.Fatalf("block %d after restart:\n got %s\nwant %s", height, got, want)
				}
			}
			assertSameValues(t, persisted, reference)
			if len(persisted.processor.late.values) != 0 {
				t.Fatalf("held = %d after metadata", len(persisted.processor.late.values))
			}
		})
	}
}
