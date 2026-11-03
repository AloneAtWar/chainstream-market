package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
)

type fakeWallets struct {
	statsQuery    model.WalletStatsQuery
	positionQuery model.PositionQuery
	tradeQuery    model.WalletTradeQuery
	labeled       []string
	statsCalls    int
}

var refreshed = time.Unix(1790000000, 0).UTC()

func (fake *fakeWallets) WalletStats(_ context.Context, query model.WalletStatsQuery) ([]model.WalletStats, error) {
	fake.statsQuery = query
	fake.statsCalls++
	return []model.WalletStats{
		{ChainID: 1, WindowDays: 30, Wallet: "0xaa", RealizedUSD: decimal.RequireFromString("120"),
			BoughtUSD: decimal.RequireFromString("400"), Wins: 3, Counted: 4, Tokens: 2, Closes: 2, HoldSeconds: 600, RefreshedAt: refreshed},
		{ChainID: 1, WindowDays: 7, Wallet: "0xaa", RealizedUSD: decimal.RequireFromString("20"), RefreshedAt: refreshed},
	}, nil
}

func (fake *fakeWallets) Positions(_ context.Context, query model.PositionQuery) ([]model.PricedPosition, error) {
	fake.positionQuery = query
	price, symbol := decimal.RequireFromString("3"), "PEPE"
	return []model.PricedPosition{{
		Position: model.Position{
			Wallet: "0xaa", Token: "0xtt", Quantity: decimal.RequireFromString("10"), CostUSD: decimal.RequireFromString("20"),
			RealizedUSD: decimal.RequireFromString("5"), FirstBuyHeight: 7, LastTradeTime: refreshed,
		},
		PriceUSD: &price, Symbol: &symbol,
	}}, nil
}

func (fake *fakeWallets) WalletTrades(_ context.Context, query model.WalletTradeQuery) ([]model.WalletTrade, error) {
	fake.tradeQuery = query
	return []model.WalletTrade{{
		BlockHeight: 9, BlockTime: refreshed, Wallet: query.Wallet, Token: "0xtt", Side: model.SideSell,
		Event: model.WalletEventClose, Amount: decimal.RequireFromString("10"), RealizedUSD: decimal.RequireFromString("5"),
		Counted: true, HoldSeconds: 60,
	}}, nil
}

func (fake *fakeWallets) LabeledWallets(context.Context, uint64, string) ([]string, error) {
	return fake.labeled, nil
}

func (fake *fakeWallets) WalletLabels(_ context.Context, _ uint64, wallets []string) (map[string][]string, error) {
	labels := make(map[string][]string)
	for _, wallet := range wallets {
		if wallet == "0xaa" {
			labels[wallet] = []string{model.LabelSmartMoney}
		}
	}
	return labels, nil
}

type fakeLabels map[string][]string

func (labels fakeLabels) WalletLabels(_ uint64, wallet string) []string { return labels[wallet] }

func serveWallets(t *testing.T, wallets *fakeWallets, path string) (int, map[string]any) {
	t.Helper()
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithWallets(wallets).
		WithLabels(fakeLabels{"0xaa": {model.LabelSmartMoney}})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s: %v", recorder.Body.String(), err)
	}
	return recorder.Code, body
}

func TestWalletProfile(t *testing.T) {
	wallets := &fakeWallets{}
	code, body := serveWallets(t, wallets, "/v1/chains/1/wallets/0xAA")
	if code != http.StatusOK || wallets.statsQuery.WindowDays != 0 || wallets.statsQuery.Wallets[0] != "0xaa" {
		t.Fatalf("code %d query %+v", code, wallets.statsQuery)
	}
	stats := body["stats"].([]any)
	first := stats[0].(map[string]any)
	if first["window_days"].(float64) != 7 || body["labels"].([]any)[0] != "smart_money" || body["refreshed_at"].(float64) != 1790000000 {
		t.Fatalf("body %v", body)
	}
	thirty := stats[1].(map[string]any)
	if thirty["win_rate"].(float64) != 0.75 || thirty["roi"] != "0.3" || thirty["average_hold_seconds"].(float64) != 300 {
		t.Fatalf("30d %v", thirty)
	}
}

func TestWalletListByLabel(t *testing.T) {
	wallets := &fakeWallets{labeled: []string{"0xaa", "0xbb"}}
	code, body := serveWallets(t, wallets, "/v1/chains/1/wallets?window=7&label=smart_money&sort=winrate&limit=5")
	query := wallets.statsQuery
	if code != http.StatusOK || query.WindowDays != 7 || query.Sort != "winrate" || query.Limit != 5 || len(query.Wallets) != 2 {
		t.Fatalf("code %d query %+v", code, query)
	}
	first := body["wallets"].([]any)[0].(map[string]any)
	if first["wallet"] != "0xaa" || first["labels"].([]any)[0] != "smart_money" || first["realized_usd"] != "120" {
		t.Fatalf("wallet %v", first)
	}

	// 没有钱包带这个标签时不查统计。
	empty := &fakeWallets{}
	code, body = serveWallets(t, empty, "/v1/chains/1/wallets?label=smart_money")
	if code != http.StatusOK || empty.statsCalls != 0 || len(body["wallets"].([]any)) != 0 {
		t.Fatalf("code %d calls %d body %v", code, empty.statsCalls, body)
	}
}

func TestWalletListRejectsBadParameters(t *testing.T) {
	for _, query := range []string{"window=0", "window=91", "sort=pnl", "limit=0"} {
		if code, _ := serveWallets(t, &fakeWallets{}, "/v1/chains/1/wallets?"+query); code != http.StatusBadRequest {
			t.Errorf("%s: status %d", query, code)
		}
	}
}

func TestWalletPositions(t *testing.T) {
	wallets := &fakeWallets{}
	code, body := serveWallets(t, wallets, "/v1/chains/1/wallets/0xaa/positions?status=open")
	if code != http.StatusOK || wallets.positionQuery.Open == nil || !*wallets.positionQuery.Open {
		t.Fatalf("code %d query %+v", code, wallets.positionQuery)
	}
	position := body["positions"].([]any)[0].(map[string]any)
	// 10 个，成本 20（均价 2），现价 3：未实现 10。
	if position["status"] != "open" || position["average_cost_usd"] != "2" || position["unrealized_usd"] != "10" ||
		position["symbol"] != "PEPE" || position["first_buy_height"].(float64) != 7 || position["opened_at"] != nil {
		t.Fatalf("position %v", position)
	}
	if code, _ := serveWallets(t, wallets, "/v1/chains/1/wallets/0xaa/positions?status=all"); code != http.StatusBadRequest {
		t.Fatalf("bad status: %d", code)
	}
}

func TestWalletTrades(t *testing.T) {
	wallets := &fakeWallets{}
	code, body := serveWallets(t, wallets, "/v1/chains/1/wallets/0xaa/trades?token=0xTT&before_height=100&limit=10")
	query := wallets.tradeQuery
	if code != http.StatusOK || query.Token != "0xtt" || query.BeforeHeight != 100 || query.Limit != 10 {
		t.Fatalf("code %d query %+v", code, query)
	}
	trade := body["trades"].([]any)[0].(map[string]any)
	if trade["event"] != "close" || trade["counted"] != true || trade["hold_seconds"].(float64) != 60 {
		t.Fatalf("trade %v", trade)
	}
}

func TestTokenTraders(t *testing.T) {
	wallets := &fakeWallets{}
	code, body := serveWallets(t, wallets, "/v1/chains/1/tokens/0xTT/traders?limit=3")
	if code != http.StatusOK || wallets.positionQuery.Token != "0xtt" || wallets.positionQuery.Limit != 3 {
		t.Fatalf("code %d query %+v", code, wallets.positionQuery)
	}
	trader := body["traders"].([]any)[0].(map[string]any)
	if trader["wallet"] != "0xaa" || trader["labels"].([]any)[0] != "smart_money" || trader["realized_usd"] != "5" {
		t.Fatalf("trader %v", trader)
	}
}

// tradersReader 返回两个交易者的成交。
type tradersReader struct{ fakeReader }

func (*tradersReader) Trades(context.Context, model.TradeQuery) ([]model.Trade, error) {
	return []model.Trade{{Trader: "0xaa"}, {Trader: "0xbb"}}, nil
}

func TestTradesCarryTraderLabels(t *testing.T) {
	server := New(&tradersReader{}, []time.Duration{time.Minute}, nil).
		WithLabels(fakeLabels{"0xaa": {model.LabelSmartMoney}})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/chains/1/tokens/0xtt/trades", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, `"trader":"0xaa"`) || strings.Count(body, `"trader_labels":["smart_money"]`) != 1 {
		t.Fatalf("body %s", body)
	}
}

func TestWalletRoutesNeedReader(t *testing.T) {
	if code := serveHolders(t, nil, "/v1/chains/1/wallets/0xaa"); code != http.StatusNotFound {
		t.Fatalf("status %d", code)
	}
}
