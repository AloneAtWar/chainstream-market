package metrics

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestInstrumentLabelsByRoutePattern(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/chains/{chain}/tokens/{token}/price", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNotFound)
	})
	handler := Instrument(mux)
	for _, path := range []string{"/v1/chains/eth/tokens/0x1/price", "/v1/chains/bsc/tokens/0x2/price", "/nope"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	}
	if got := testutil.ToFloat64(HTTPRequests.WithLabelValues("GET /v1/chains/{chain}/tokens/{token}/price", "404")); got != 2 {
		t.Errorf("price requests = %v, want 2", got)
	}
	if got := testutil.ToFloat64(HTTPRequests.WithLabelValues("unmatched", "404")); got != 1 {
		t.Errorf("unmatched requests = %v, want 1", got)
	}
}

func TestServerServesMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewServer(addr, logger)
	served := make(chan error, 1)
	go func() { served <- server.Start(ctx) }()
	Reorgs.WithLabelValues("start-test").Inc()
	var body string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/metrics", nil)
		response, getErr := http.DefaultClient.Do(request)
		if getErr != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		raw, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		body = string(raw)
		break
	}
	for _, want := range []string{`chainstream_market_engine_reorgs_total{chain="start-test"} 1`, "go_goroutines"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if err := server.Stop(ctx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}
	if err := <-served; err != nil {
		t.Errorf("Start() error = %v", err)
	}
}

func TestInstrumentKeepsWebSocketUpgrades(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/stream", func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	server := httptest.NewServer(Instrument(mux))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/stream", nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	if response.Body != nil {
		_ = response.Body.Close()
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("status = %d, want %d", response.StatusCode, http.StatusSwitchingProtocols)
	}
	_ = conn.CloseNow()
}
