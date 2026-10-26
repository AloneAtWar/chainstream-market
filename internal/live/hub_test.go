package live

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	pool  = "0xpool"
	token = "0x1111111111111111111111111111111111111111"
	weth  = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
)

func newTestHub(t *testing.T, buffer int) *Hub {
	t.Helper()
	hub, err := NewHub(HubOptions{
		Feed: idleFeed{}, Topics: map[uint64]string{1: "chainstream-eth-live"}, Intervals: []time.Duration{time.Minute, time.Hour},
		Throttle: time.Second, ClientBuffer: buffer, MaxSubscriptions: 4,
	})
	if err != nil {
		t.Fatalf("NewHub() error = %v", err)
	}
	return hub
}

// idleFeed delivers nothing; the tests dispatch updates themselves.
type idleFeed struct{}

func (idleFeed) Run(ctx context.Context, _ []string, _ func(string, []byte)) error {
	<-ctx.Done()
	return nil
}

// received drains the client's queue as "type channel" lines, with the
// candle's close or the rollback height where there is one.
func received(t *testing.T, client *Client) []string {
	t.Helper()
	var lines []string
	for {
		select {
		case payload := <-client.send:
			var message struct {
				Type       string `json:"type"`
				Channel    string `json:"channel"`
				Candle     *Candle
				RollbackTo int64 `json:"rollback_to"`
				Trades     []Trade
				Error      string
			}
			if err := json.Unmarshal(payload, &message); err != nil {
				t.Fatalf("Unmarshal(%s) error = %v", payload, err)
			}
			line := message.Type + " " + message.Channel
			switch message.Type {
			case "candle", "candle_reset":
				if message.Candle == nil {
					line += " null"
				} else {
					line += " " + message.Candle.Close
				}
			case "revert":
				line += " " + strconv.FormatInt(message.RollbackTo, 10)
			case "trades":
				line += " " + strconv.Itoa(len(message.Trades))
			case "error":
				line += " " + message.Error
			}
			lines = append(lines, strings.Join(strings.Fields(line), " "))
		default:
			return lines
		}
	}
}

func expect(t *testing.T, client *Client, want ...string) {
	t.Helper()
	got := received(t, client)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("messages:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func candle(subject, key, interval string, openTime int64, closePrice string, height int64) Candle {
	return Candle{Subject: subject, Key: key, Interval: interval, OpenTime: openTime, Close: closePrice, UpdateHeight: height}
}

func TestHubSendsTradesAtOnceAndCoalescesCandles(t *testing.T) {
	hub := newTestHub(t, 64)
	client := hub.Connect()
	if err := client.Subscribe([]string{"trades:1:pool:0xPOOL", "candles:1:pool:0xpool:1m"}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	other := hub.Connect()
	if err := other.Subscribe([]string{"trades:1:token:" + token}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	expect(t, client, "subscribed")
	expect(t, other, "subscribed")

	trades := []Trade{{PoolID: pool, BaseToken: token}, {PoolID: "0xother", BaseToken: token}}
	hub.dispatch(Update{Type: UpdateBlock, ChainID: 1, Height: 100, Trades: trades, Candles: []Candle{
		candle("pool", pool, "1m", 60, "1", 100),
		candle("pool", pool, "1h", 0, "1", 100),
	}})
	hub.dispatch(Update{Type: UpdateBlock, ChainID: 1, Height: 101, Candles: []Candle{
		candle("pool", pool, "1m", 60, "2", 101),
		candle("pool", pool, "1m", 120, "3", 101),
	}})
	expect(t, client, "trades trades:1:pool:0xpool 1")
	expect(t, other, "trades trades:1:token:"+token+" 2")

	hub.flush()
	expect(t, client,
		"candle candles:1:pool:0xpool:1m 2",
		"candle candles:1:pool:0xpool:1m 3",
		"head",
	)
	expect(t, other, "head")
	hub.flush()
	expect(t, client)
}

func TestHubRevertFlushesThenResets(t *testing.T) {
	hub := newTestHub(t, 64)
	client := hub.Connect()
	if err := client.Subscribe([]string{"candles:1:pool:0xpool:1m", "candles:1:token:" + token + ":1m"}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	expect(t, client, "subscribed")
	hub.dispatch(Update{Type: UpdateBlock, ChainID: 1, Height: 102, Candles: []Candle{candle("pool", pool, "1m", 120, "4", 102)}})
	latest := candle("pool", pool, "1m", 60, "2", 101)
	hub.dispatch(Update{Type: UpdateRevert, ChainID: 1, Height: 101, Resets: []Reset{
		{Subject: "pool", Key: pool, Interval: "1m", Latest: &latest},
		{Subject: "token", Key: token, Interval: "1m"},
		{Subject: "pool", Key: pool, Interval: "1h"},
	}})
	expect(t, client,
		"candle candles:1:pool:0xpool:1m 4",
		"head",
		"revert 101",
		"candle_reset candles:1:pool:0xpool:1m 2",
		"candle_reset candles:1:token:"+token+":1m null",
	)
	// A later part of the same revert carries resets only.
	hub.dispatch(Update{Type: UpdateRevert, ChainID: 1, Height: 101, Part: 1, Resets: []Reset{{Subject: "token", Key: token, Interval: "1m"}}})
	expect(t, client, "candle_reset candles:1:token:"+token+":1m null")
}

func TestHubDropsSlowClients(t *testing.T) {
	hub := newTestHub(t, 2)
	client := hub.Connect()
	if err := client.Subscribe([]string{"trades:1:pool:" + pool}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	for height := int64(100); height < 103; height++ {
		hub.dispatch(Update{Type: UpdateBlock, ChainID: 1, Height: height, Trades: []Trade{{PoolID: pool}}})
	}
	select {
	case <-client.dropped:
	default:
		t.Fatal("client was not dropped")
	}
	if len(hub.channels) != 0 || len(hub.chains) != 0 {
		t.Fatalf("dropped client still subscribed: %v %v", hub.channels, hub.chains)
	}
	if err := client.Subscribe([]string{"trades:1:pool:" + pool}); err == nil {
		t.Fatal("Subscribe() after drop succeeded")
	}
	client.Close()
}

func TestHubChannels(t *testing.T) {
	hub := newTestHub(t, 64)
	for name, want := range map[string]string{
		"trades:1:token:0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE":     "trades:1:token:" + weth,
		"candles:1:token:0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE:1h": "candles:1:token:" + weth + ":1h",
		"trades:1:pool:0xABC": "trades:1:pool:0xabc",
	} {
		channel, err := hub.parseChannel(name)
		if err != nil || channel.name != want {
			t.Fatalf("parseChannel(%q) = %q, %v; want %q", name, channel.name, err, want)
		}
	}
	for _, name := range []string{
		"trades:56:pool:0xabc", "candles:1:pool:0xabc:5m", "candles:1:pool:0xabc", "trades:1:pair:0xabc",
		"trades:x:pool:0xabc", "prices:1:token:0xabc", "trades:1:pool:",
	} {
		if _, err := hub.parseChannel(name); err == nil {
			t.Fatalf("parseChannel(%q) succeeded", name)
		}
	}

	client := hub.Connect()
	if err := client.Subscribe([]string{"trades:1:pool:0xa", "trades:1:pool:0xb", "trades:1:pool:0xc"}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if err := client.Subscribe([]string{"trades:1:pool:0xa", "trades:1:pool:0xd", "trades:1:pool:0xe"}); err == nil {
		t.Fatal("Subscribe() past the limit succeeded")
	}
	client.Unsubscribe([]string{"trades:1:pool:0xa", "trades:1:pool:0xzz"})
	if err := client.Subscribe([]string{"trades:1:pool:0xd", "trades:1:pool:0xe"}); err != nil {
		t.Fatalf("Subscribe() after unsubscribing error = %v", err)
	}
	client.Close()
	if len(hub.channels) != 0 || len(hub.chains) != 0 {
		t.Fatalf("closed client still subscribed: %v %v", hub.channels, hub.chains)
	}
}
