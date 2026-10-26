// Package metrics holds the Prometheus metrics the services export and
// serves them at /metrics. Each service runs as its own process, so the
// scrape job tells them apart; the chain label tells chains apart.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "chainstream_market"

// Registry holds every metric below plus the Go runtime and process
// collectors.
var Registry = prometheus.NewRegistry()

var (
	// Records counts the topic records an engine committed, by kind: block
	// or revert.
	Records = counterVec("engine_records_total", "Topic records applied and committed.", "chain", "kind")
	// RecordSeconds is how long one record took to apply and commit.
	RecordSeconds = histogramVec("engine_record_seconds", "Time to apply and commit one topic record.",
		prometheus.ExponentialBuckets(0.001, 2, 16), "chain", "kind")
	// Reorgs counts reverts that undid at least one applied block.
	Reorgs = counterVec("engine_reorgs_total", "Reverts that undid applied blocks.", "chain")
	// ReorgDepth is how many applied blocks each of those reverts undid.
	ReorgDepth = histogramVec("engine_reorg_depth_blocks", "Applied blocks undone by one revert.",
		[]float64{1, 2, 3, 5, 8, 13, 21, 34, 64, 128, 256}, "chain")
	// Height is the last applied block height.
	Height = gaugeVec("engine_height", "Last applied block height.", "chain")
	// BlockLag is how far the last applied block's time is behind the clock.
	BlockLag = gaugeVec("engine_block_lag_seconds", "Clock time minus the last applied block's time.", "chain")
	// ConsumerLag is how many records of the topic are not committed yet.
	ConsumerLag = gaugeVec("engine_consumer_lag_records", "Records in the topic after the last committed one.", "chain")

	// RPCRequests counts JSON-RPC HTTP requests, by result: ok or error.
	RPCRequests = counterVec("rpc_requests_total", "JSON-RPC requests sent, by result.", "chain", "result")
	// RPCSeconds is how long each JSON-RPC HTTP request took.
	RPCSeconds = histogramVec("rpc_request_seconds", "JSON-RPC request latency.", prometheus.DefBuckets, "chain")
	// RPCFailures counts failed JSON-RPC requests and calls: transport,
	// HTTP and decoding errors, and node errors other than a revert.
	RPCFailures = counterVec("rpc_failures_total", "Failed JSON-RPC requests and calls.", "chain")

	// RankingsRefreshSeconds is how long one rankings refresh took.
	RankingsRefreshSeconds = histogramVec("rankings_refresh_seconds", "Time to refresh one chain's rankings.",
		prometheus.ExponentialBuckets(0.05, 2, 12), "chain")
	// RankingsRefreshFailures counts failed rankings refreshes.
	RankingsRefreshFailures = counterVec("rankings_refresh_failures_total", "Failed rankings refreshes.", "chain")

	// HTTPRequests counts market-api requests by route pattern and status.
	HTTPRequests = counterVec("http_requests_total", "market-api requests, by route and status code.", "route", "code")
	// HTTPSeconds is market-api request latency by route pattern.
	HTTPSeconds = histogramVec("http_request_seconds", "market-api request latency, by route.", prometheus.DefBuckets, "route")
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

func counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	vec := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help}, labels)
	Registry.MustRegister(vec)
	return vec
}

func gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help}, labels)
	Registry.MustRegister(vec)
	return vec
}

func histogramVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: name, Help: help, Buckets: buckets,
	}, labels)
	Registry.MustRegister(vec)
	return vec
}

// Handler serves Registry in the Prometheus text format.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry})
}

// Start serves /metrics on addr until ctx is done. An empty addr turns it
// off. It fails at once when addr cannot be bound.
func Start(ctx context.Context, addr string, logger *slog.Logger) error {
	if addr == "" {
		return nil
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", Handler())
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if serveErr := server.Serve(listener); !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("metrics server stopped", "error", serveErr)
		}
	}()
	logger.Info("serving metrics", "addr", listener.Addr().String())
	return nil
}

// Instrument counts and times the requests handler serves, by the
// ServeMux pattern that matched them. Unmatched requests count as route
// "unmatched" so stray paths cannot grow the label set.
func Instrument(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: writer, status: http.StatusOK}
		handler.ServeHTTP(recorder, request)
		route := request.Pattern
		if route == "" {
			route = "unmatched"
		}
		HTTPRequests.WithLabelValues(route, fmt.Sprint(recorder.status)).Inc()
		HTTPSeconds.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (recorder *statusRecorder) WriteHeader(status int) {
	if !recorder.wroteHeader {
		recorder.status, recorder.wroteHeader = status, true
	}
	recorder.ResponseWriter.WriteHeader(status)
}

// Unwrap lets http.ResponseController and the WebSocket upgrade reach the
// underlying writer's Flush and Hijack.
func (recorder *statusRecorder) Unwrap() http.ResponseWriter { return recorder.ResponseWriter }
