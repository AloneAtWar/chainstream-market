package rpc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
)

// requestMetrics records every JSON-RPC HTTP request the SDK sends: its
// latency, and whether it failed as a whole. Per-call node errors are
// counted by Client.call.
func requestMetrics(chain string) middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			start := time.Now()
			reply, err := next(ctx, request)
			metrics.RPCSeconds.WithLabelValues(chain).Observe(time.Since(start).Seconds())
			result := "ok"
			if err != nil {
				result = "error"
				metrics.RPCFailures.WithLabelValues(chain).Inc()
			}
			metrics.RPCRequests.WithLabelValues(chain, result).Inc()
			return reply, err
		}
	}
}

// kratosLogger sends the SDK's Kratos logs to slog.
type kratosLogger struct{ logger *slog.Logger }

func (adapter kratosLogger) Log(level log.Level, keyvals ...any) error {
	message := ""
	attrs := make([]any, 0, len(keyvals))
	for index := 0; index+1 < len(keyvals); index += 2 {
		if keyvals[index] == log.DefaultMessageKey {
			message = fmt.Sprint(keyvals[index+1])
			continue
		}
		attrs = append(attrs, fmt.Sprint(keyvals[index]), keyvals[index+1])
	}
	adapter.logger.Log(context.Background(), slogLevel(level), message, attrs...)
	return nil
}

func slogLevel(level log.Level) slog.Level {
	switch level {
	case log.LevelDebug:
		return slog.LevelDebug
	case log.LevelWarn:
		return slog.LevelWarn
	case log.LevelError, log.LevelFatal:
		return slog.LevelError
	case log.LevelInfo:
	}
	return slog.LevelInfo
}
