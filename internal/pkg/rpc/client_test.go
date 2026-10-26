package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc/rpctest"
)

const (
	token   = "0x1111111111111111111111111111111111111111"
	broken  = "0x2222222222222222222222222222222222222222"
	alice   = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob     = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	blockA  = "0xa"
	missing = "0xdead"
)

func TestBalancesAtBatchesAndDecodes(t *testing.T) {
	node := rpctest.NewNode()
	defer node.Close()
	node.SetBalance(blockA, token, alice, 42)
	node.Revert(broken)
	client, err := New(Options{URL: node.URL, BatchSize: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	balances, err := client.BalancesAt(context.Background(), blockA, []BalanceQuery{
		{Token: token, Holder: alice}, {Token: token, Holder: bob}, {Token: broken, Holder: alice},
	})
	if err != nil {
		t.Fatalf("BalancesAt() error = %v", err)
	}
	if balances[0].Int64() != 42 || balances[1].Sign() != 0 || balances[2] != nil {
		t.Fatalf("balances = %v", balances)
	}
	if node.Calls() != 3 {
		t.Fatalf("calls = %d", node.Calls())
	}
}

func TestBalancesAtFailsWithoutState(t *testing.T) {
	node := rpctest.NewNode()
	defer node.Close()
	node.Missing(missing)
	client, _ := New(Options{URL: node.URL})
	if _, err := client.BalancesAt(context.Background(), missing, []BalanceQuery{{Token: token, Holder: alice}}); err == nil {
		t.Fatal("BalancesAt() without block state error = nil")
	}
	if _, err := client.BalancesAt(context.Background(), blockA, []BalanceQuery{{Token: token, Holder: "0x12"}}); err == nil {
		t.Fatal("BalancesAt() with a bad holder error = nil")
	}
}

func TestBalancesAtReportsBatchErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"batch too large"}}`)) //nolint:errcheck // test server
	}))
	defer server.Close()
	client, _ := New(Options{URL: server.URL})
	_, err := client.BalancesAt(context.Background(), blockA, []BalanceQuery{{Token: token, Holder: alice}})
	var rpcErr *rpcError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32600 {
		t.Fatalf("BalancesAt() error = %v", err)
	}
}

func TestDecodeUint256(t *testing.T) {
	if decodeUint256([]byte(`"0x"`)) != nil || decodeUint256([]byte(`"0x12"`)) != nil {
		t.Fatal("short results must decode to nil")
	}
	if _, err := New(Options{}); err == nil {
		t.Fatal("New() without url error = nil")
	}
}

func TestCallLatestDecodesRevertsAndEmptyCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var calls []struct {
			ID     int   `json:"id"`
			Params []any `json:"params"`
		}
		_ = json.NewDecoder(request.Body).Decode(&calls) //nolint:errcheck // test server
		replies := make([]map[string]any, len(calls))
		for index, call := range calls {
			reply := map[string]any{"jsonrpc": "2.0", "id": call.ID}
			to := call.Params[0].(map[string]any)["to"]
			switch {
			case call.Params[1] != "latest":
				reply["error"] = map[string]any{"code": -32602, "message": "want latest"}
			case to == broken:
				reply["error"] = map[string]any{"code": 3, "message": "execution reverted"}
			case to == alice:
				reply["result"] = "0x"
			default:
				reply["result"] = "0x00ff"
			}
			replies[index] = reply
		}
		_ = json.NewEncoder(writer).Encode(replies) //nolint:errcheck // test server
	}))
	defer server.Close()
	client, _ := New(Options{URL: server.URL, BatchSize: 2})
	results, err := client.CallLatest(context.Background(), []Call{{To: token, Data: "0x01"}, {To: broken}, {To: alice}})
	if err != nil {
		t.Fatalf("CallLatest() error = %v", err)
	}
	if len(results[0]) != 2 || results[0][1] != 0xff || results[1] != nil || results[2] != nil {
		t.Fatalf("results = %x", results)
	}
}
