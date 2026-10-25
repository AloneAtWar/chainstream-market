package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/market"
)

const (
	proxy      = "0x00000000000000000000000000000000000000aa"
	aggregator = "0x00000000000000000000000000000000000000bb"
	weth       = "0x4200000000000000000000000000000000000006"
)

// proxyNode answers eth_call for one plain AggregatorProxy whose
// description is "ETH / USD"; any other call reverts.
func proxyNode(t *testing.T) *httptest.Server {
	t.Helper()
	text := fmt.Sprintf("%x", "ETH / USD")
	answers := map[string]string{
		"0x313ce567": fmt.Sprintf("%064x", 8),
		"0x7284e416": fmt.Sprintf("%064x%064x", 32, len(text)/2) + text + strings.Repeat("0", 64-len(text)),
		"0x245a7bfc": strings.Repeat("0", 24) + strings.TrimPrefix(aggregator, "0x"),
	}
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var calls []struct {
			ID     int               `json:"id"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(request.Body).Decode(&calls); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		replies := make([]map[string]any, len(calls))
		for index, call := range calls {
			var target struct{ To, Data string }
			_ = json.Unmarshal(call.Params[0], &target) //nolint:errcheck // malformed calls revert
			reply := map[string]any{"jsonrpc": "2.0", "id": call.ID}
			if answer, ok := answers[target.Data]; ok && target.To == proxy {
				reply["result"] = "0x" + answer
			} else {
				reply["error"] = map[string]any{"code": 3, "message": "execution reverted"}
			}
			replies[index] = reply
		}
		_ = json.NewEncoder(writer).Encode(replies) //nolint:errcheck // test server
	}))
}

func TestResolveFeedsReadsProxies(t *testing.T) {
	node := proxyNode(t)
	defer node.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	chain := conf.Chain{Name: "base", RPCURL: node.URL, Feeds: []conf.Feed{
		{Proxy: proxy, Token: weth, Description: "eth / usd"},
		{Proxy: proxy, Token: market.NativeAddress, Decimals: 8},
		{Address: "0xcc", Token: "0xdd", Decimals: 18},
	}}
	feeds, err := resolveFeeds(context.Background(), chain, logger)
	if err != nil {
		t.Fatalf("resolveFeeds() error = %v", err)
	}
	want := []market.Feed{
		{Address: aggregator, Token: weth, Decimals: 8},
		{Address: aggregator, Token: market.NativeAddress, Decimals: 8},
		{Address: "0xcc", Token: "0xdd", Decimals: 18},
	}
	if !reflect.DeepEqual(feeds, want) {
		t.Fatalf("resolveFeeds() = %+v, want %+v", feeds, want)
	}

	for name, feed := range map[string]conf.Feed{
		"wrong decimals":    {Proxy: proxy, Token: weth, Decimals: 18},
		"wrong description": {Proxy: proxy, Token: weth, Description: "BTC / USD"},
		"not a proxy":       {Proxy: "0x00000000000000000000000000000000000000ee", Token: weth},
	} {
		chain.Feeds = []conf.Feed{feed}
		if _, err := resolveFeeds(context.Background(), chain, logger); err == nil {
			t.Fatalf("resolveFeeds() with %s error = nil", name)
		}
	}
}
