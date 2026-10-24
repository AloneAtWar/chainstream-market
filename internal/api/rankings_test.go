package api

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/shopspring/decimal"
)

type fakeRankings struct {
	query  storage.TokenListQuery
	tokens []storage.RankedToken
	err    error
}

func (fake *fakeRankings) TokenList(_ context.Context, query storage.TokenListQuery) ([]storage.RankedToken, error) {
	fake.query = query
	return fake.tokens, fake.err
}

func TestTokenList(t *testing.T) {
	symbol, decimals, height, created := "PEPE", uint8(18), int64(120), int64(1_700_000_000)
	price := decimal.RequireFromString("0.00012")
	rankings := &fakeRankings{tokens: []storage.RankedToken{
		{
			Token: "0xaa", Symbol: &symbol, Decimals: &decimals, Ranked: true, PriceUSD: &price, PriceHeight: &height,
			PriceChange24h: decimal.RequireFromString("-0.25"), Volume24hUSD: decimal.NewFromInt(1500), Trades24h: 9,
			LiquidityUSD: decimal.NewFromInt(40000), Holders: 321, CreatedHeight: &height, CreatedTime: &created,
			RefreshedAt: time.Unix(1_700_000_060, 0),
		},
		{Token: "0xbb", PriceChange24h: decimal.Zero, Volume24hUSD: decimal.Zero, LiquidityUSD: decimal.Zero},
	}}
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithRankings(rankings)
	code, body := serveRegistry(t, server,
		"/v1/chains/56/tokens?sort=holders&order=asc&q=+pe+&min_liquidity_usd=1000.5&limit=20&offset=40")
	want := `{"chain_id":56,"order":"asc","refreshed_at":1700000060,"sort":"holders","tokens":[` +
		`{"token":"0xaa","name":null,"symbol":"PEPE","decimals":18,"ranked":true,"price_usd":"0.00012",` +
		`"price_height":120,"price_change_24h":"-0.25","volume_24h_usd":"1500","trades_24h":9,"liquidity_usd":"40000",` +
		`"holders":321,"created_height":120,"created_time":1700000000},` +
		`{"token":"0xbb","name":null,"symbol":null,"decimals":null,"ranked":false,"price_usd":null,"price_height":null,` +
		`"price_change_24h":"0","volume_24h_usd":"0","trades_24h":0,"liquidity_usd":"0","holders":0,` +
		`"created_height":null,"created_time":null}]}` + "\n"
	if code != http.StatusOK || body != want {
		t.Fatalf("GET tokens = %d %s", code, body)
	}
	query := rankings.query
	if query.ChainID != 56 || query.Sort != storage.SortHolders || !query.Ascending || query.Search != "pe" ||
		!query.MinLiquidityUSD.Equal(decimal.RequireFromString("1000.5")) || query.Limit != 20 || query.Offset != 40 {
		t.Fatalf("query = %+v", query)
	}

	rankings.tokens = nil
	code, body = serveRegistry(t, server, "/v1/chains/1/tokens")
	if code != http.StatusOK || body != `{"chain_id":1,"order":"desc","refreshed_at":null,"sort":"volume_24h","tokens":[]}`+"\n" {
		t.Fatalf("GET default tokens = %d %s", code, body)
	}
	if rankings.query.Sort != storage.SortVolume24h || rankings.query.Ascending || rankings.query.Limit != defaultLimit {
		t.Fatalf("default query = %+v", rankings.query)
	}
}

func TestTokenListErrors(t *testing.T) {
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil)
	if code, _ := serveRegistry(t, server, "/v1/chains/1/tokens"); code != http.StatusNotFound {
		t.Fatalf("without rankings status = %d", code)
	}
	rankings := &fakeRankings{}
	server.WithRankings(rankings)
	for _, path := range []string{
		"/v1/chains/0/tokens",
		"/v1/chains/1/tokens?sort=mcap",
		"/v1/chains/1/tokens?order=up",
		"/v1/chains/1/tokens?min_liquidity_usd=-1",
		"/v1/chains/1/tokens?min_liquidity_usd=lots",
		"/v1/chains/1/tokens?limit=0",
		"/v1/chains/1/tokens?offset=-1",
		"/v1/chains/1/tokens?offset=10001",
	} {
		if code, body := serveRegistry(t, server, path); code != http.StatusBadRequest {
			t.Fatalf("GET %s = %d %s", path, code, body)
		}
	}
	rankings.err = errors.New("down")
	if code, _ := serveRegistry(t, server, "/v1/chains/1/tokens"); code != http.StatusInternalServerError {
		t.Fatalf("store error status = %d", code)
	}
}
