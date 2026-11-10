package rpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AloneAtWar/oh-my-blockchain/chain/evm"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc/rpctest"
)

const (
	token   = "0x1111111111111111111111111111111111111111"
	broken  = "0x2222222222222222222222222222222222222222"
	alice   = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob     = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	blockA  = "0x000000000000000000000000000000000000000000000000000000000000000a"
	missing = "0x000000000000000000000000000000000000000000000000000000000000dead"
)

func TestBalancesBatchesAndDecodes(t *testing.T) {
	node := rpctest.NewNode()
	defer node.Close()
	node.SetBalance(blockA, token, alice, 42)
	node.Revert(broken)
	client, err := New(Options{URL: node.URL, BatchSize: 2, Exact: true})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	balances, err := client.Balances(context.Background(), Block{Height: 10, Hash: blockA}, []BalanceQuery{
		{Token: token, Holder: alice}, {Token: token, Holder: bob}, {Token: broken, Holder: alice},
	})
	if err != nil {
		t.Fatalf("Balances() error = %v", err)
	}
	if balances[0].Amount.Int64() != 42 || balances[1].Amount.Sign() != 0 || balances[2].Amount != nil {
		t.Fatalf("balances = %v", balances)
	}
	for _, balance := range balances {
		if balance.Height != 10 {
			t.Fatalf("exact balance height = %d, want 10", balance.Height)
		}
	}
	if node.Calls() != 3 {
		t.Fatalf("calls = %d", node.Calls())
	}
}

func TestBalancesFailsWithoutState(t *testing.T) {
	node := rpctest.NewNode()
	defer node.Close()
	node.Missing(missing)
	client, _ := New(Options{URL: node.URL, Exact: true})
	if _, err := client.Balances(context.Background(), Block{Height: 10, Hash: missing}, []BalanceQuery{{Token: token, Holder: alice}}); err == nil {
		t.Fatal("Balances() without block state error = nil")
	}
	if _, err := client.Balances(context.Background(), Block{Height: 10, Hash: blockA}, []BalanceQuery{{Token: token, Holder: "0x12"}}); err == nil {
		t.Fatal("Balances() with a bad holder error = nil")
	}
}

func TestBalancesReportsBatchErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"batch too large"}}`)) //nolint:errcheck // test server
	}))
	defer server.Close()
	client, _ := New(Options{URL: server.URL, Exact: true})
	if _, err := client.Balances(context.Background(), Block{Height: 10, Hash: blockA}, []BalanceQuery{{Token: token, Holder: alice}}); err == nil {
		t.Fatal("Balances() against a node rejecting the batch error = nil")
	}
	if _, err := client.Balances(context.Background(), Block{Height: 10, Hash: "0xa"}, []BalanceQuery{{Token: token, Holder: alice}}); err == nil {
		t.Fatal("Balances() with a short block hash error = nil")
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
	results, err := client.CallLatest(context.Background(), []Call{{To: token, Data: []byte{1}}, {To: broken}, {To: alice}})
	if err != nil {
		t.Fatalf("CallLatest() error = %v", err)
	}
	if len(results[0]) != 2 || results[0][1] != 0xff || results[1] != nil || results[2] != nil {
		t.Fatalf("results = %x", results)
	}
}

func TestBalancesReadsNativeBalancesWithGetEthBalance(t *testing.T) {
	const multicall = "0xca11bde05977b3631167028862be2a173976ca11"
	node := rpctest.NewNode()
	defer node.Close()
	node.SetBalance(blockA, token, alice, 42)
	node.SetBalance(blockA, multicall, alice, 7)
	client, err := New(Options{URL: node.URL, NativeBalanceContract: multicall, Exact: true})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	balances, err := client.Balances(context.Background(), Block{Height: 10, Hash: blockA}, []BalanceQuery{
		{Token: token, Holder: alice}, {Token: NativeToken, Holder: alice}, {Token: NativeToken, Holder: bob},
	})
	if err != nil {
		t.Fatalf("Balances() error = %v", err)
	}
	if balances[0].Amount.Int64() != 42 || balances[1].Amount.Int64() != 7 || balances[2].Amount.Sign() != 0 {
		t.Fatalf("balances = %v", balances)
	}
}

func TestNativeBalancesNeedAMulticallContract(t *testing.T) {
	plain, _ := New(Options{URL: "http://127.0.0.1:1"})
	if plain.NativeBalances() {
		t.Fatal("plain node NativeBalances() = true")
	}
	if _, err := plain.Balances(context.Background(), Block{Height: 10, Hash: blockA}, []BalanceQuery{{Token: NativeToken, Holder: alice}}); err == nil {
		t.Fatal("Balances() native without multicall error = nil")
	}
	for _, chainID := range []uint64{1, 56, 8453} {
		if client, _ := New(Options{URL: "http://127.0.0.1:1", ChainID: chainID}); !client.NativeBalances() {
			t.Fatalf("chain %d NativeBalances() = false", chainID)
		}
	}
}

// Latest reads run at the latest block no older than the applied one and
// report the block they ran at; a head behind the block is an error.
func TestBalancesLatestReportsTheReadBlock(t *testing.T) {
	const blockB = "0x000000000000000000000000000000000000000000000000000000000000000b"
	node := rpctest.NewNode()
	defer node.Close()
	node.SetBalance(blockB, token, alice, 50)
	node.SetHead(11, blockB)
	client, err := New(Options{URL: node.URL})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	balances, err := client.Balances(context.Background(), Block{Height: 10, Hash: blockA}, []BalanceQuery{
		{Token: token, Holder: alice}, {Token: token, Holder: bob},
	})
	if err != nil {
		t.Fatalf("Balances() error = %v", err)
	}
	if balances[0].Amount.Int64() != 50 || balances[0].Height != 11 || balances[1].Amount.Sign() != 0 || balances[1].Height != 11 {
		t.Fatalf("balances = %+v, want alice 50 read at 11", balances)
	}
	if _, err := client.Balances(context.Background(), Block{Height: 12, Hash: blockA}, []BalanceQuery{
		{Token: token, Holder: alice},
	}); err == nil {
		t.Fatal("Balances() for a block above the head error = nil")
	}
}

func TestTronHoldersAreBase58AndExactIsRefused(t *testing.T) {
	if _, err := New(Options{URL: "http://127.0.0.1:1", ChainID: uint64(evm.TRON), Exact: true}); err == nil {
		t.Fatal("New() exact on TRON error = nil")
	}
	tron, err := New(Options{URL: "http://127.0.0.1:1", ChainID: uint64(evm.TRON)})
	if err != nil {
		t.Fatalf("New() TRON error = %v", err)
	}
	// TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t is USDT on TRON, 0xa614f803b6fd780986a42c78ec9c7f77e6ded13c.
	holder, err := tron.hexAddress("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
	if err != nil || holder.Hex() != "0xa614f803B6FD780986A42c78Ec9c7f77e6DeD13C" {
		t.Fatalf("hexAddress() = %s, %v", holder.Hex(), err)
	}
	if _, err := tron.hexAddress("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6T"); err == nil {
		t.Fatal("hexAddress() with a bad checksum error = nil")
	}
	plain, _ := New(Options{URL: "http://127.0.0.1:1"})
	if _, err := plain.hexAddress("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"); err == nil {
		t.Fatal("hexAddress() base58 off TRON error = nil")
	}
}
