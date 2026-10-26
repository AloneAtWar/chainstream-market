package market

import (
	"context"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// TestRevertedCandlesMatchReplay checks that the resets a revert reports are
// exactly the series the reverted blocks changed, each at the value a
// processor that never saw those blocks holds.
func TestRevertedCandlesMatchReplay(t *testing.T) {
	blocks := func() []*streamv1.BlockEvent {
		metadata := testBlock(102, 72)
		metadata.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
		return []*streamv1.BlockEvent{
			testBlock(100, 0, wethSwap(), undecided(memeSell(), meme, 1)),
			testBlock(101, 60, wethSwap()),
			// MEME's metadata replays the held trade into new MEME buckets.
			metadata,
			testBlock(103, 130, wethSwap(), memeSell()),
		}
	}
	reorged := newLateProcessor(t, 100, 10)
	changed := make(map[candleKey]struct{})
	for _, block := range blocks() {
		output := apply(t, reorged, block)
		if block.GetBlockHeight() <= 101 {
			continue
		}
		for _, candle := range output.Candles {
			changed[candleKey{candle.Subject, candle.Key, candle.Interval}] = struct{}{}
		}
	}
	resets := reorged.RevertedCandles(101)
	if err := reorged.Revert(context.Background(), 101); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	replayed := newLateProcessor(t, 100, 10)
	for _, block := range blocks()[:2] {
		apply(t, replayed, block)
	}

	if len(resets) != len(changed) {
		t.Fatalf("resets = %d series, want the %d the reverted blocks changed", len(resets), len(changed))
	}
	memeSeries := 0
	for _, reset := range resets {
		key := candleKey{reset.Subject, reset.Key, reset.Interval}
		if _, ok := changed[key]; !ok {
			t.Fatalf("reset %+v for a series the reverted blocks did not change", key)
		}
		want, exists := replayed.candles.get(key)
		current, _ := reorged.candles.get(key)
		switch {
		case reset.Latest == nil:
			if exists {
				t.Fatalf("reset %+v is empty, replay has %+v", key, want)
			}
			memeSeries++
		case !exists:
			t.Fatalf("reset %+v has %+v, replay has none", key, *reset.Latest)
		case !candlesEqual(*reset.Latest, want) || !candlesEqual(*reset.Latest, current):
			t.Fatalf("reset %+v = %+v, replay %+v, reverted %+v", key, *reset.Latest, want, current)
		}
	}
	// MEME's pool and token series, at both intervals, did not exist at 101.
	if memeSeries != 4 {
		t.Fatalf("empty resets = %d, want 4", memeSeries)
	}
}

func candlesEqual(left, right model.Candle) bool {
	return left.OpenTime.Equal(right.OpenTime) && left.Open.Equal(right.Open) && left.High.Equal(right.High) &&
		left.Low.Equal(right.Low) && left.Close.Equal(right.Close) && left.VolumeUSD.Equal(right.VolumeUSD) &&
		left.Trades == right.Trades && left.UpdateHeight == right.UpdateHeight
}
