package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/shopspring/decimal"
)

const weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

type fakeReader struct {
	price       model.TokenPrice
	hasPrice    bool
	candleQuery model.CandleQuery
	tradeQuery  model.TradeQuery
	err         error
}

func (fake *fakeReader) LatestPrice(_ context.Context, _ uint64, token string) (model.TokenPrice, bool, error) {
	fake.price.Token = token
	return fake.price, fake.hasPrice, fake.err
}

func (fake *fakeReader) Candles(_ context.Context, query model.CandleQuery) ([]model.Candle, error) {
	fake.candleQuery = query
	return []model.Candle{{
		OpenTime: time.Unix(1_700_000_000, 0), Open: decimal.NewFromInt(1), High: decimal.NewFromInt(2),
		Low: decimal.NewFromInt(1), Close: decimal.RequireFromString("1.5"), Trades: 3,
	}}, fake.err
}

func (fake *fakeReader) Trades(_ context.Context, query model.TradeQuery) ([]model.Trade, error) {
	fake.tradeQuery = query
	return []model.Trade{{BlockHeight: 99, Side: model.SideSell, PriceUSD: decimal.NewFromInt(3000)}}, fake.err
}

func serve(t *testing.T, reader storage.Reader, path string) (int, map[string]any) {
	t.Helper()
	server := New(reader, []time.Duration{time.Minute, time.Hour, 24 * time.Hour}, nil)
	server.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	var body map[string]any
	if recorder.Code != http.StatusNoContent {
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode %s: %v", recorder.Body.String(), err)
		}
	}
	return recorder.Code, body
}

func TestPriceMapsNativeToWrappedToken(t *testing.T) {
	reader := &fakeReader{hasPrice: true, price: model.TokenPrice{
		PriceUSD: decimal.NewFromInt(3000), Source: model.PriceSourceGraph, Hops: 1, BlockTime: time.Unix(10, 0),
	}}
	status, body := serve(t, reader, "/v1/chains/1/tokens/0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE/price")
	if status != http.StatusOK || body["token"] != weth || body["price_usd"] != "3000" || body["source"] != "graph" {
		t.Fatalf("status %d body %v", status, body)
	}
	reader.hasPrice = false
	if status, _ := serve(t, reader, "/v1/chains/1/tokens/0xabc/price"); status != http.StatusNotFound {
		t.Fatalf("missing price status = %d", status)
	}
}

func TestCandlesParseQuery(t *testing.T) {
	reader := &fakeReader{}
	status, body := serve(t, reader, "/v1/chains/56/pools/0xPOOL/candles?interval=1h&from=100&to=200&limit=5")
	if status != http.StatusOK || body["interval"] != "1h" {
		t.Fatalf("status %d body %v", status, body)
	}
	query := reader.candleQuery
	if query.ChainID != 56 || query.Subject != model.CandleSubjectPool || query.Key != "0xpool" ||
		query.Interval != time.Hour || query.From.Unix() != 100 || query.To.Unix() != 200 || query.Limit != 5 {
		t.Fatalf("query = %+v", query)
	}
	candles := body["candles"].([]any)
	if first := candles[0].(map[string]any); first["close"] != "1.5" || first["open_time"] != float64(1_700_000_000) {
		t.Fatalf("candle = %v", first)
	}

	// Defaults: everything up to one interval past now.
	if status, _ := serve(t, reader, "/v1/chains/1/tokens/0xabc/candles?interval=1d"); status != http.StatusOK {
		t.Fatalf("default status = %d", status)
	}
	if got := reader.candleQuery; got.Limit != defaultLimit || got.To.Unix() != 1_700_000_000+86400 {
		t.Fatalf("default query = %+v", got)
	}
}

func TestTradesQuery(t *testing.T) {
	reader := &fakeReader{}
	status, body := serve(t, reader, "/v1/chains/1/tokens/0xABC/trades?before_height=100&limit=20")
	if status != http.StatusOK {
		t.Fatalf("status %d body %v", status, body)
	}
	if got := reader.tradeQuery; got.BaseToken != "0xabc" || got.PoolID != "" || got.BeforeHeight != 100 || got.Limit != 20 {
		t.Fatalf("query = %+v", got)
	}
	trade := body["trades"].([]any)[0].(map[string]any)
	if trade["side"] != "sell" || trade["price_usd"] != "3000" {
		t.Fatalf("trade = %v", trade)
	}
	serve(t, reader, "/v1/chains/1/pools/0xPool/trades")
	if got := reader.tradeQuery; got.PoolID != "0xpool" || got.BaseToken != "" {
		t.Fatalf("pool query = %+v", got)
	}
}

func TestBadRequestsAndErrors(t *testing.T) {
	reader := &fakeReader{}
	for _, path := range []string{
		"/v1/chains/x/tokens/0xabc/price",
		"/v1/chains/0/tokens/0xabc/price",
		"/v1/chains/1/tokens/0xabc/candles",
		"/v1/chains/1/tokens/0xabc/candles?interval=7m",
		"/v1/chains/1/tokens/0xabc/candles?interval=1m&limit=5000",
		"/v1/chains/1/tokens/0xabc/candles?interval=1m&from=200&to=100",
		"/v1/chains/1/tokens/0xabc/trades?before_height=-1",
	} {
		if status, _ := serve(t, reader, path); status != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", path, status)
		}
	}
	reader.err = errors.New("boom")
	status, body := serve(t, reader, "/v1/chains/1/tokens/0xabc/trades")
	if status != http.StatusInternalServerError || body["error"] != "internal error" {
		t.Fatalf("status %d body %v", status, body)
	}
	if status, _ := serve(t, reader, "/healthz"); status != http.StatusNoContent {
		t.Fatalf("healthz status = %d", status)
	}
}

func TestIntervalName(t *testing.T) {
	for interval, want := range map[time.Duration]string{
		30 * time.Second: "30s", time.Minute: "1m", 15 * time.Minute: "15m", 4 * time.Hour: "4h", 24 * time.Hour: "1d",
	} {
		if got := IntervalName(interval); got != want {
			t.Fatalf("IntervalName(%s) = %q, want %q", interval, got, want)
		}
	}
}

func TestStreamRouteNeedsLive(t *testing.T) {
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/stream", nil)
	recorder := httptest.NewRecorder()
	New(&fakeReader{}, []time.Duration{time.Minute}, nil).Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status without live = %d, want 404", recorder.Code)
	}
	stream := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusTeapot) })
	recorder = httptest.NewRecorder()
	New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithLive(stream).Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("status with live = %d, want the stream handler's", recorder.Code)
	}
}
