package biz

import (
	"context"
	"errors"
	"testing"

	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

func TestPruningKeepsAtMostTheDepth(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name      string
		finalized int64
		// lowest is the lowest rollback height still accepted.
		lowest int64
	}{
		{"finalized unknown", 0, 96},
		{"finalized lags less than the depth", 98, 98},
		{"finalized lags more than the depth", 90, 96},
	} {
		registry, err := New(Options{ChainID: 1, Repo: nopRepo{}, HistoryDepth: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Apply(ctx, &streamv1.BlockEvent{BlockHeight: 100, FinalizedHeight: test.finalized}); err != nil {
			t.Fatal(err)
		}
		if err := registry.Revert(ctx, test.lowest); err != nil {
			t.Errorf("%s: Revert(%d) error = %v", test.name, test.lowest, err)
		}
		if err := registry.Revert(ctx, test.lowest-1); !errors.Is(err, ErrReorgTooDeep) {
			t.Errorf("%s: Revert(%d) error = %v, want ErrReorgTooDeep", test.name, test.lowest-1, err)
		}
	}
}
