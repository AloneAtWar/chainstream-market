package live

import (
	"context"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/shopspring/decimal"
)

func TestBlockUpdatesSplitLargeBlocks(t *testing.T) {
	output := storage.BlockOutput{ChainID: 1, Height: 100, BlockTime: time.Unix(1_700_000_000, 0)}
	for index := range 700 {
		output.Trades = append(output.Trades, storage.Trade{LogIndex: uint32(index), PriceUSD: decimal.NewFromInt(1)})
	}
	for range 400 {
		output.Candles = append(output.Candles, storage.Candle{Subject: storage.CandleSubjectPool, Interval: time.Minute})
	}
	updates := BlockUpdates(output, 90)
	if len(updates) != 3 {
		t.Fatalf("updates = %d, want 3", len(updates))
	}
	trades, candles := 0, 0
	for index, update := range updates {
		if update.Part != index || update.Height != 100 || update.FinalizedHeight != 90 || update.BlockTime != 1_700_000_000 {
			t.Fatalf("update %d header = %+v", index, update)
		}
		if items := len(update.Trades) + len(update.Candles); items > maxItemsPerRecord {
			t.Fatalf("update %d has %d items", index, items)
		}
		for _, trade := range update.Trades {
			if trade.RevisionHeight != 100 || trade.LogIndex != uint32(trades) {
				t.Fatalf("trade %d = %+v", trades, trade)
			}
			trades++
		}
		candles += len(update.Candles)
	}
	if trades != 700 || candles != 400 {
		t.Fatalf("trades = %d, candles = %d", trades, candles)
	}

	if empty := BlockUpdates(storage.BlockOutput{ChainID: 1, Height: 101}, 90); len(empty) != 1 || empty[0].Type != UpdateBlock {
		t.Fatalf("empty block updates = %+v", empty)
	}
}

func TestRevertUpdates(t *testing.T) {
	latest := storage.Candle{Interval: time.Hour, OpenTime: time.Unix(3600, 0), Close: decimal.NewFromInt(2)}
	revert := storage.Revert{ChainID: 1, RollbackTo: 99, Candles: []storage.CandleReset{
		{Subject: storage.CandleSubjectToken, Key: "0xa", Interval: time.Hour, Latest: &latest},
	}}
	for range 600 {
		revert.Candles = append(revert.Candles, storage.CandleReset{Subject: storage.CandleSubjectPool, Key: "0xb", Interval: time.Minute})
	}
	updates := RevertUpdates(revert)
	if len(updates) != 2 || updates[1].Part != 1 || len(updates[0].Resets)+len(updates[1].Resets) != 601 {
		t.Fatalf("updates = %d", len(updates))
	}
	first := updates[0].Resets[0]
	if first.Subject != "token" || first.Interval != "1h" || first.Latest == nil || first.Latest.Close != "2" || first.Latest.OpenTime != 3600 {
		t.Fatalf("first reset = %+v", first)
	}
	if updates[0].Resets[1].Latest != nil {
		t.Fatal("empty series has a latest bucket")
	}
}

// TestPublisherSkipsOldBlocks never reaches a broker: blocks behind MaxLag
// and chains without a topic are not produced.
func TestPublisherSkipsOldBlocks(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	publisher, err := NewPublisher(PublisherOptions{
		Brokers: []string{"127.0.0.1:1"}, Topics: map[uint64]string{1: "eth-live"},
		MaxLag: time.Minute, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	defer publisher.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := publisher.NotifyBlock(ctx, storage.BlockOutput{ChainID: 1, BlockTime: now.Add(-2 * time.Minute)}, 0); err != nil {
		t.Fatalf("NotifyBlock(old) error = %v", err)
	}
	if err := publisher.NotifyBlock(ctx, storage.BlockOutput{ChainID: 56, BlockTime: now}, 0); err != nil {
		t.Fatalf("NotifyBlock(other chain) error = %v", err)
	}
	if err := publisher.NotifyRevert(ctx, storage.Revert{ChainID: 56}); err != nil {
		t.Fatalf("NotifyRevert(other chain) error = %v", err)
	}
}
