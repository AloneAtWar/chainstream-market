package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

type fakeRegistry struct {
	tokens map[string]model.Token
	pools  map[string]model.Pool
	query  string
	limit  int
	err    error
}

func (fake *fakeRegistry) Token(_ context.Context, _ uint64, address string) (model.Token, bool, error) {
	fake.query = address
	token, ok := fake.tokens[address]
	return token, ok, fake.err
}

func (fake *fakeRegistry) Pool(_ context.Context, _ uint64, poolID string) (model.Pool, bool, error) {
	fake.query = poolID
	pool, ok := fake.pools[poolID]
	return pool, ok, fake.err
}

func (fake *fakeRegistry) TokenPools(_ context.Context, _ uint64, token string, limit int) ([]model.Pool, error) {
	fake.query, fake.limit = token, limit
	var pools []model.Pool
	for _, pool := range fake.pools {
		pools = append(pools, pool)
	}
	return pools, fake.err
}

func newFakeRegistry() *fakeRegistry {
	name, symbol, decimals := "USD Coin", "USDC", uint8(6)
	return &fakeRegistry{
		tokens: map[string]model.Token{
			"0xaa": {
				Address: "0xaa", Standard: "erc20", Recognition: "recognized", MetadataComplete: true,
				Name: &name, Symbol: &symbol, Decimals: &decimals, Metadata: []byte(`{"Status": 2}`), Created: true,
				Deployment:   &model.Deployment{BlockHeight: 5, BlockTime: 50, TransactionHash: "0xd"},
				SourceHeight: 7, FirstHeight: 5, UpdatedHeight: 7,
			},
			"0xbb": {Address: "0xbb", Standard: "erc20", Recognition: "unknown", SourceHeight: 3, FirstHeight: 3, UpdatedHeight: 3},
		},
		pools: map[string]model.Pool{
			"0xcc": {
				PoolID: "0xcc", Protocol: "uniswap_v2", DexAddress: "0xf", Tokens: []string{"0xaa", "0xbb"},
				SourceHeight: 8, SourceTime: 80, FirstHeight: 8, UpdatedHeight: 8, Metadata: []byte(`{"fee": 3000}`),
			},
		},
	}
}

func serveRegistry(t *testing.T, server *Server, path string) (int, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	return recorder.Code, recorder.Body.String()
}

func TestRegistryRoutes(t *testing.T) {
	registry := newFakeRegistry()
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithRegistry(registry)
	tests := []struct {
		path string
		want string
	}{
		{
			path: "/v1/chains/1/tokens/0xAA",
			want: `{"chain_id":1,"address":"0xaa","standard":"erc20","recognition":"recognized","metadata_complete":true,` +
				`"name":"USD Coin","symbol":"USDC","decimals":6,"created":true,"deployment":{"block_height":5,"block_time":50,` +
				`"transaction_index":0,"transaction_hash":"0xd","trace_id":""},"source_height":7,"first_height":5,` +
				`"updated_height":7,"metadata":{"Status":2}}`,
		},
		{
			path: "/v1/chains/1/tokens/0xbb",
			want: `{"chain_id":1,"address":"0xbb","standard":"erc20","recognition":"unknown","metadata_complete":false,` +
				`"name":null,"symbol":null,"decimals":null,"created":false,"source_height":3,"first_height":3,"updated_height":3}`,
		},
		{
			path: "/v1/chains/1/pools/0xCC",
			want: `{"chain_id":1,"pool_id":"0xcc","protocol":"uniswap_v2","dex_address":"0xf","tokens":["0xaa","0xbb"],` +
				`"created":false,"source_height":8,"source_time":80,"first_height":8,"updated_height":8,"metadata":{"fee":3000}}`,
		},
	}
	for _, test := range tests {
		code, body := serveRegistry(t, server, test.path)
		if code != http.StatusOK || body != test.want+"\n" {
			t.Fatalf("GET %s = %d %s", test.path, code, body)
		}
	}

	code, body := serveRegistry(t, server, "/v1/chains/1/tokens/0xAA/pools?limit=7")
	if code != http.StatusOK || registry.query != "0xaa" || registry.limit != 7 ||
		body != `{"pools":[`+tests[2].want+`]}`+"\n" {
		t.Fatalf("pools = %d %s (query %s %d)", code, body, registry.query, registry.limit)
	}
}

func TestRegistryRouteErrors(t *testing.T) {
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil)
	if code, _ := serveRegistry(t, server, "/v1/chains/1/pools/0xcc"); code != http.StatusNotFound {
		t.Fatalf("without a registry status = %d", code)
	}
	server.WithRegistry(newFakeRegistry())
	for path, want := range map[string]int{
		"/v1/chains/1/tokens/0xdd":                  http.StatusNotFound,
		"/v1/chains/1/pools/0xdd":                   http.StatusNotFound,
		"/v1/chains/x/tokens/0xaa":                  http.StatusBadRequest,
		"/v1/chains/1/tokens/0xaa/pools?limit=5000": http.StatusBadRequest,
	} {
		if code, body := serveRegistry(t, server, path); code != want {
			t.Fatalf("GET %s = %d %s, want %d", path, code, body, want)
		}
	}
	failing := newFakeRegistry()
	failing.err = errors.New("down")
	server = New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithRegistry(failing)
	if code, _ := serveRegistry(t, server, "/v1/chains/1/tokens/0xaa"); code != http.StatusInternalServerError {
		t.Fatalf("store error status = %d", code)
	}
}

func TestTokenHoldersWithDecimals(t *testing.T) {
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithHolders(&fakeHolders{}).WithRegistry(newFakeRegistry())
	code, body := serveRegistry(t, server, "/v1/chains/1/tokens/0xaa/holders")
	want := `{"chain_id":1,"token":"0xaa","decimals":6,"holder_count":3,"holders":[{"holder":"0xaa","balance_raw":"1200","balance":"0.0012"}]}` + "\n"
	if code != http.StatusOK || body != want {
		t.Fatalf("holders = %d %s", code, body)
	}
	// Without known decimals only raw balances are returned.
	code, body = serveRegistry(t, server, "/v1/chains/1/tokens/0xbb/holders")
	want = `{"chain_id":1,"token":"0xbb","holder_count":3,"holders":[{"holder":"0xaa","balance_raw":"1200"}]}` + "\n"
	if code != http.StatusOK || body != want {
		t.Fatalf("holders = %d %s", code, body)
	}
}
