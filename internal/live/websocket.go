package live

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

const (
	writeTimeout = 10 * time.Second
	pingInterval = 30 * time.Second
	// readLimit bounds one client request; a subscribe naming the maximum
	// number of channels fits easily.
	readLimit = 64 << 10
)

// request is what clients send.
type request struct {
	Op       string   `json:"op"`
	Channels []string `json:"channels"`
}

// ServeHTTP upgrades the request to a WebSocket and serves one client until
// it leaves, falls too far behind, or the hub stops.
func (hub *Hub) ServeHTTP(writer http.ResponseWriter, httpRequest *http.Request) {
	conn, err := websocket.Accept(writer, httpRequest, &websocket.AcceptOptions{OriginPatterns: hub.options.OriginPatterns})
	if err != nil {
		// Accept has already written the HTTP error.
		return
	}
	conn.SetReadLimit(readLimit)
	client := hub.Connect()
	defer client.Close()

	ctx, cancel := context.WithCancel(httpRequest.Context())
	defer cancel()
	go func() {
		defer cancel()
		hub.readRequests(ctx, conn, client)
	}()

	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case payload := <-client.send:
			if err := write(ctx, conn, payload); err != nil {
				_ = conn.CloseNow() //nolint:errcheck // already failing
				return
			}
		case <-client.dropped:
			_ = conn.Close(websocket.StatusTryAgainLater, "client too slow") //nolint:errcheck // closing anyway
			return
		case <-hub.stopped:
			_ = conn.Close(websocket.StatusGoingAway, "server stopping") //nolint:errcheck // closing anyway
			return
		case <-ping.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pingCtx)
			pingCancel()
			if err != nil {
				_ = conn.CloseNow() //nolint:errcheck // already failing
				return
			}
		case <-ctx.Done():
			_ = conn.CloseNow() //nolint:errcheck // the reader already failed
			return
		}
	}
}

func write(ctx context.Context, conn *websocket.Conn, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, payload)
}

// readRequests applies subscribe and unsubscribe requests until the
// connection fails or closes.
func (hub *Hub) readRequests(ctx context.Context, conn *websocket.Conn, client *Client) {
	for {
		kind, data, err := conn.Read(ctx)
		if err != nil {
			var closeErr websocket.CloseError
			if !errors.As(err, &closeErr) && ctx.Err() == nil {
				hub.logger.Debug("live client read failed", "error", err)
			}
			return
		}
		if kind != websocket.MessageText {
			client.Fail("requests must be JSON text")
			continue
		}
		var body request
		if err := json.Unmarshal(data, &body); err != nil {
			client.Fail("request is not valid JSON")
			continue
		}
		switch body.Op {
		case "subscribe":
			if err := client.Subscribe(body.Channels); err != nil {
				client.Fail(err.Error())
			}
		case "unsubscribe":
			client.Unsubscribe(body.Channels)
		default:
			client.Fail(`op must be "subscribe" or "unsubscribe"`)
		}
	}
}
