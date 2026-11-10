package chainsync

import "testing"

func TestPruneThrough(t *testing.T) {
	for _, test := range []struct {
		name                     string
		height, finalized, depth int64
		want                     int64
	}{
		{"finalized unknown keeps the depth", 100, 0, 4, 96},
		{"finalized lags less than the depth", 100, 98, 4, 98},
		{"finalized lags exactly the depth", 100, 96, 4, 96},
		{"finalized lags more than the depth", 100, 90, 4, 96},
		{"head younger than the depth", 3, 0, 4, 0},
	} {
		if got := PruneThrough(test.height, test.finalized, test.depth); got != test.want {
			t.Errorf("%s: PruneThrough(%d, %d, %d) = %d, want %d",
				test.name, test.height, test.finalized, test.depth, got, test.want)
		}
	}
}
