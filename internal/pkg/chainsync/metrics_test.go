package chainsync

import (
	"context"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// laggingSource is a fakeSource whose topic holds more records than it
// returns.
type laggingSource struct {
	fakeSource
	end int64
}

func (lagging *laggingSource) HighWatermark() int64 { return lagging.end }

func TestEngineExportsMetrics(t *testing.T) {
	const chain = "metrics-test"
	messages := encode(t, 0,
		block(100, "a100", "a99"),
		block(101, "a101", "a100"),
		block(102, "a102", "a101"),
		revert(100),
		block(101, "b101", "a100"),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Unix(1_700_000_000+101+30, 0)
	engine, err := New(Options{
		ChainID: testChainID, Chain: chain, Handler: newHeaderHandler(), Now: func() time.Time { return now },
		Open: func(int64) (source.Source, error) {
			return &laggingSource{fakeSource: fakeSource{messages: messages, cancel: cancel}, end: 8}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := engine.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"blocks", testutil.ToFloat64(metrics.Records.WithLabelValues(chain, "block")), 4},
		{"reverts", testutil.ToFloat64(metrics.Records.WithLabelValues(chain, "revert")), 1},
		{"reorgs", testutil.ToFloat64(metrics.Reorgs.WithLabelValues(chain)), 1},
		{"height", testutil.ToFloat64(metrics.Height.WithLabelValues(chain)), 101},
		{"block lag", testutil.ToFloat64(metrics.BlockLag.WithLabelValues(chain)), 30},
		// Offsets 0-4 are committed and the topic ends before offset 8.
		{"consumer lag", testutil.ToFloat64(metrics.ConsumerLag.WithLabelValues(chain)), 3},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %v, want %v", check.name, check.got, check.want)
		}
	}
	if count := testutil.CollectAndCount(metrics.ReorgDepth); count == 0 {
		t.Error("reorg depth histogram has no series")
	}
}
