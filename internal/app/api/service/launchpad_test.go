package service

import (
	"context"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

type fakeLaunchpad struct {
	query     model.LaunchpadQuery
	listings  []model.LaunchpadListing
	token     string
	targets   model.LaunchpadTargets
	protocols []string
}

func (fake *fakeLaunchpad) LaunchpadList(_ context.Context, query model.LaunchpadQuery) ([]model.LaunchpadListing, error) {
	fake.query = query
	return fake.listings, nil
}

func (fake *fakeLaunchpad) LaunchpadToken(
	_ context.Context, _ uint64, token string, targets model.LaunchpadTargets,
) (model.LaunchpadListing, bool, error) {
	fake.token, fake.targets = token, targets
	if len(fake.listings) == 0 {
		return model.LaunchpadListing{}, false, nil
	}
	return fake.listings[0], true, nil
}

func (fake *fakeLaunchpad) LaunchpadProtocols(context.Context, uint64) ([]string, error) {
	return fake.protocols, nil
}

var testTargets = model.LaunchpadTargets{Default: 69_000, ByProtocol: map[string]float64{"poolstrade": 50_000}}

func TestLaunchpadList(t *testing.T) {
	symbol, decimals, lastTrade, price := "DOGE", uint8(18), int64(1_700_000_050), "0.0001"
	progress, _ := new(big.Int).SetString("250000000000000000", 10)
	launchpad := &fakeLaunchpad{listings: []model.LaunchpadListing{{
		Token: "0xaa", Protocol: "fourmeme", PoolID: "0xm:0xaa", Manager: "0xm", Quote: "0xbb", QuoteDecimals: &decimals,
		Symbol: &symbol, Status: model.LaunchpadBonding, LaunchHeight: 7, LaunchTime: 1_700_000_000, LaunchSeen: true,
		Progress: progress, QuoteRaised: big.NewInt(42), Price: big.NewInt(9), Trades: 3, Buys: 2, Sells: 1,
		LastTradeTime: &lastTrade, PriceUSD: &price, HeadTime: 1_700_000_060,
	}}}
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithLaunchpad(launchpad, testTargets)
	code, body := serveRegistry(t, server,
		"/v1/chains/56/launchpad/tokens?column=bonding&protocol=FLAP&max_age=6h&stale=600&min_progress=0.5&limit=20")
	want := `{"chain_id":56,"column":"bonding","head_time":1700000060,"tokens":[{"token":"0xaa","protocol":"fourmeme",` +
		`"pool_id":"0xm:0xaa","manager":"0xm","quote":"0xbb","quote_symbol":null,"quote_decimals":18,"creator":null,"name":null,` +
		`"symbol":"DOGE","decimals":null,"meta_uri":null,"status":"bonding","launch_height":7,` +
		`"launch_time":1700000000,"launch_seen":true,"progress":"0.25","base_remaining":null,"quote_raised":"42",` +
		`"price":"9","trades":3,"buys":2,"sells":1,"last_trade_time":1700000050,"completed_time":null,` +
		`"migrated_time":null,"target_pool_id":null,"price_usd":"0.0001","volume_24h_usd":null,"holders":null,` +
		`"direct":false,"trading_pool_id":null,"launch_start":null,"launch_end":null}]}` + "\n"
	if code != http.StatusOK || body != want {
		t.Fatalf("GET launchpad tokens = %d %s", code, body)
	}
	query := launchpad.query
	if query.ChainID != 56 || query.Column != model.LaunchpadColumnBonding || query.Protocol != "flap" ||
		query.MaxAge != 6*3600 || query.Stale != 600 || query.MigratedAge != 72*3600 ||
		query.MinProgress.String() != "500000000000000000" || query.Limit != 20 || query.Targets.Default != 69_000 {
		t.Fatalf("query = %+v", query)
	}

	code, body = serveRegistry(t, server, "/v1/chains/56/launchpad/tokens/0xAA")
	if code != http.StatusOK || launchpad.token != "0xaa" || launchpad.targets.ByProtocol["poolstrade"] != 50_000 ||
		body[:len(`{"chain_id":56,"head_time":1700000060,"token":{"token":"0xaa"`)] !=
			`{"chain_id":56,"head_time":1700000060,"token":{"token":"0xaa"` {
		t.Fatalf("GET launchpad token = %d %s", code, body)
	}

	launchpad.listings = nil
	code, body = serveRegistry(t, server, "/v1/chains/56/launchpad/tokens?column=new")
	if code != http.StatusOK || body != `{"chain_id":56,"column":"new","head_time":null,"tokens":[]}`+"\n" {
		t.Fatalf("GET empty list = %d %s", code, body)
	}
	if launchpad.query.Limit != launchpadDefaultLimit || launchpad.query.MaxAge != 24*3600 || launchpad.query.MinProgress != nil {
		t.Fatalf("default query = %+v", launchpad.query)
	}
	if code, _ = serveRegistry(t, server, "/v1/chains/56/launchpad/tokens/0xcc"); code != http.StatusNotFound {
		t.Fatalf("GET unknown launchpad token status = %d", code)
	}
}

func TestLaunchpadListErrors(t *testing.T) {
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil)
	if code, _ := serveRegistry(t, server, "/v1/chains/56/launchpad/tokens?column=new"); code != http.StatusNotFound {
		t.Fatalf("without launchpad status = %d", code)
	}
	server.WithLaunchpad(&fakeLaunchpad{}, testTargets)
	for _, path := range []string{
		"/v1/chains/0/launchpad/tokens?column=new",
		"/v1/chains/56/launchpad/tokens",
		"/v1/chains/56/launchpad/tokens?column=hot",
		"/v1/chains/56/launchpad/tokens?column=new&max_age=0",
		"/v1/chains/56/launchpad/tokens?column=new&max_age=forever",
		"/v1/chains/56/launchpad/tokens?column=new&stale=-5",
		"/v1/chains/56/launchpad/tokens?column=new&migrated_age=1000h",
		"/v1/chains/56/launchpad/tokens?column=bonding&min_progress=1.5",
		"/v1/chains/56/launchpad/tokens?column=bonding&min_progress=lots",
		"/v1/chains/56/launchpad/tokens?column=new&limit=0",
	} {
		if code, body := serveRegistry(t, server, path); code != http.StatusBadRequest {
			t.Errorf("GET %s = %d %s, want 400", path, code, body)
		}
	}
}

func TestLaunchpadProtocols(t *testing.T) {
	launchpad := &fakeLaunchpad{}
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithLaunchpad(launchpad, testTargets)
	code, body := serveRegistry(t, server, "/v1/chains/4663/launchpad/protocols")
	if code != http.StatusOK || body != `{"chain_id":4663,"protocols":[]}`+"\n" {
		t.Fatalf("GET empty protocols = %d %s", code, body)
	}
	launchpad.protocols = []string{"bankr", "clanker"}
	code, body = serveRegistry(t, server, "/v1/chains/4663/launchpad/protocols")
	if code != http.StatusOK || body != `{"chain_id":4663,"protocols":["bankr","clanker"]}`+"\n" {
		t.Fatalf("GET protocols = %d %s", code, body)
	}
}
