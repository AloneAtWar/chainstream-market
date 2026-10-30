// Package transport holds the Kratos transport servers every command shares:
// an HTTP server, and a Runner that runs a long-lived function as a server.
//
// transport 包是各命令共用的 Kratos 服务：HTTP 服务器，以及运行长期任务（每条链一个
// 消费循环）的 Runner。
package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// HTTPServer serves an http.Handler as a Kratos transport.
type HTTPServer struct {
	server *http.Server
	logger *slog.Logger
}

// NewHTTPServer serves handler on addr.
func NewHTTPServer(addr string, handler http.Handler, logger *slog.Logger) *HTTPServer {
	return &HTTPServer{
		server: &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second},
		logger: logger,
	}
}

// Start serves until Stop.
func (server *HTTPServer) Start(ctx context.Context) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", server.server.Addr)
	if err != nil {
		return fmt.Errorf("http: listen %s: %w", server.server.Addr, err)
	}
	server.logger.Info("listening", "addr", server.server.Addr)
	if err := server.server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http: serve: %w", err)
	}
	return nil
}

// Stop shuts the server down, waiting at most ten seconds for requests.
func (server *HTTPServer) Stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return server.server.Shutdown(ctx)
}

// Runner is a transport.Server that runs a function until Stop. Kratos does
// not cancel the context it passes Start, so Stop cancels it and waits for
// the function to return. A function that fails, or returns before Stop,
// stops the application.
type Runner struct {
	name string
	run  func(context.Context) error

	mu       sync.Mutex
	cancel   context.CancelFunc
	stopping bool
	done     chan struct{}
}

// NewRunner runs run under name.
func NewRunner(name string, run func(context.Context) error) *Runner {
	return &Runner{name: name, run: run, done: make(chan struct{})}
}

func (runner *Runner) Start(ctx context.Context) error {
	defer close(runner.done)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runner.mu.Lock()
	runner.cancel = cancel
	if runner.stopping {
		cancel()
	}
	runner.mu.Unlock()

	err := runner.run(ctx)
	runner.mu.Lock()
	stopping := runner.stopping
	runner.mu.Unlock()
	switch {
	case stopping:
		return nil
	case err != nil:
		return fmt.Errorf("%s: %w", runner.name, err)
	default:
		return fmt.Errorf("%s stopped", runner.name)
	}
}

func (runner *Runner) Stop(ctx context.Context) error {
	runner.mu.Lock()
	runner.stopping = true
	if runner.cancel != nil {
		runner.cancel()
	}
	runner.mu.Unlock()
	select {
	case <-runner.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: stop: %w", runner.name, ctx.Err())
	}
}
