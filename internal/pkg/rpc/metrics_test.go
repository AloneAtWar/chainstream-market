package rpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc/rpctest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestClientCountsRequestsAndFailures(t *testing.T) {
	const chain = "rpc-metrics-test"
	node := rpctest.NewNode()
	defer node.Close()
	node.SetBalance(blockA, token, alice, 1)
	node.Missing(missing)
	client, err := New(Options{URL: node.URL, Chain: chain})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()
	if _, err := client.BalancesAt(ctx, blockA, []BalanceQuery{{Token: token, Holder: alice}}); err != nil {
		t.Fatalf("BalancesAt() error = %v", err)
	}
	// A node without the block's state answers with a call error.
	if _, err := client.BalancesAt(ctx, missing, []BalanceQuery{{Token: token, Holder: alice}}); err == nil {
		t.Fatal("BalancesAt() without state error = nil")
	}
	down := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusBadGateway)
	}))
	defer down.Close()
	broken, _ := New(Options{URL: down.URL, Chain: chain})
	if _, err := broken.CallLatest(ctx, []Call{{To: token, Data: "0x"}}); err == nil {
		t.Fatal("CallLatest() against a failing node error = nil")
	}

	if got := testutil.ToFloat64(metrics.RPCRequests.WithLabelValues(chain, "ok")); got != 2 {
		t.Errorf("ok requests = %v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.RPCRequests.WithLabelValues(chain, "error")); got != 1 {
		t.Errorf("failed requests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.RPCFailures.WithLabelValues(chain)); got != 2 {
		t.Errorf("failures = %v, want 2", got)
	}
}
