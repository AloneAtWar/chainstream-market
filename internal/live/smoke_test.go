package live

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/market"
	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/alicebob/miniredis/v2"
	"github.com/coder/websocket"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const (
	smokeTopic     = "chainstream-eth"
	smokeLiveTopic = "chainstream-eth-live"

	usdc     = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	wethUSDC = "0xpool-weth-usdc"
	memeWETH = "0xpool-meme-weth"
)

type leg struct{ token, delta, reserve string }

func swap(pool string, logIndex uint32, legs ...leg) *streamv1.Trade {
	trade := &streamv1.Trade{
		TransactionHash: "0xtx-" + pool, LogIndex: logIndex, Protocol: "uniswap_v2", PoolId: pool,
		Trader: "0xtrader", Action: streamv1.TradeAction_TRADE_ACTION_SWAP, PoolState: &streamv1.PoolState{},
	}
	for _, value := range legs {
		decimals := uint32(18)
		if value.token == usdc {
			decimals = 6
		}
		trade.Amounts = append(trade.Amounts, &streamv1.Amount{
			Asset: value.token, Raw: value.delta, Decimals: decimals, DecimalsKnown: true,
		})
		trade.PoolState.ReservesAfter = append(trade.PoolState.ReservesAfter, value.reserve)
	}
	return trade
}

// wethSwap buys one WETH for usd, leaving WETH at $3000 in the pool.
func wethSwap(logIndex uint32, usd string) *streamv1.Trade {
	return swap(wethUSDC, logIndex,
		leg{weth, "-1000000000000000000", "1000000000000000000000"},
		leg{usdc, usd + "000000", "3000000000000"},
	)
}

// memeSell prices MEME at 0.00001 WETH.
func memeSell(logIndex uint32) *streamv1.Trade {
	return swap(memeWETH, logIndex,
		leg{token, "100000000000000000000", "1000000000000000000000000"},
		leg{weth, "-1000000000000000", "10000000000000000000"},
	)
}

// streamModel is what a client holds after applying messages in order.
type streamModel struct {
	trades  map[string]map[string]Trade
	candles map[string]map[int64]Candle
	reverts int
	emptied int
}

func newStreamModel() *streamModel {
	return &streamModel{trades: make(map[string]map[string]Trade), candles: make(map[string]map[int64]Candle)}
}

func tradeID(trade Trade) string {
	return fmt.Sprintf("%d/%s/%d/%d", trade.BlockHeight, trade.TransactionHash, trade.LogIndex, trade.OperationIndex)
}

func (model *streamModel) apply(t *testing.T, payload []byte) {
	t.Helper()
	var message struct {
		Type       string  `json:"type"`
		Channel    string  `json:"channel"`
		Trades     []Trade `json:"trades"`
		Candle     *Candle `json:"candle"`
		RollbackTo int64   `json:"rollback_to"`
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatalf("Unmarshal(%s) error = %v", payload, err)
	}
	switch message.Type {
	case "trades":
		if model.trades[message.Channel] == nil {
			model.trades[message.Channel] = make(map[string]Trade)
		}
		for _, trade := range message.Trades {
			model.trades[message.Channel][tradeID(trade)] = trade
		}
	case "revert":
		model.reverts++
		for _, trades := range model.trades {
			maps.DeleteFunc(trades, func(_ string, trade Trade) bool { return trade.RevisionHeight > message.RollbackTo })
		}
	case "candle":
		if model.candles[message.Channel] == nil {
			model.candles[message.Channel] = make(map[int64]Candle)
		}
		model.candles[message.Channel][message.Candle.OpenTime] = *message.Candle
	case "candle_reset":
		buckets := model.candles[message.Channel]
		if message.Candle == nil {
			model.emptied++
			delete(model.candles, message.Channel)
			return
		}
		if buckets == nil {
			buckets = make(map[int64]Candle)
			model.candles[message.Channel] = buckets
		}
		maps.DeleteFunc(buckets, func(openTime int64, _ Candle) bool { return openTime > message.Candle.OpenTime })
		buckets[message.Candle.OpenTime] = *message.Candle
	case "subscribed", "head":
	default:
		t.Fatalf("unexpected message %s", payload)
	}
}

func (model *streamModel) dump() string {
	var lines []string
	for _, channel := range slices.Sorted(maps.Keys(model.trades)) {
		for _, id := range slices.Sorted(maps.Keys(model.trades[channel])) {
			trade := model.trades[channel][id]
			lines = append(lines, fmt.Sprintf("%s %s %s", channel, id, trade.PriceUSD))
		}
	}
	for _, channel := range slices.Sorted(maps.Keys(model.candles)) {
		for _, openTime := range slices.Sorted(maps.Keys(model.candles[channel])) {
			candle := model.candles[channel][openTime]
			lines = append(lines, fmt.Sprintf("%s %d o=%s c=%s n=%d", channel, openTime, candle.Open, candle.Close, candle.Trades))
		}
	}
	return strings.Join(lines, "\n")
}

// storedModel is the same view built from what market-engine stored.
func storedModel(memory *storage.Memory, channels []string) *streamModel {
	subscribed := make(map[string]bool, len(channels))
	for _, channel := range channels {
		subscribed[channel] = true
	}
	model := newStreamModel()
	for _, block := range memory.Blocks(1) {
		for _, update := range BlockUpdates(block, 0) {
			for _, trade := range update.Trades {
				for _, channel := range []string{TradeChannel(1, "pool", trade.PoolID), TradeChannel(1, "token", trade.BaseToken)} {
					if subscribed[channel] {
						if model.trades[channel] == nil {
							model.trades[channel] = make(map[string]Trade)
						}
						model.trades[channel][tradeID(trade)] = trade
					}
				}
			}
			for _, candle := range update.Candles {
				channel := CandleChannel(1, candle.Subject, candle.Key, candle.Interval)
				if subscribed[channel] {
					if model.candles[channel] == nil {
						model.candles[channel] = make(map[int64]Candle)
					}
					model.candles[channel][candle.OpenTime] = candle
				}
			}
		}
	}
	return model
}

// TestSmokeLiveThroughKafka runs market-engine over a kfake cluster with a
// live publisher on the same cluster, and a hub serving a real WebSocket
// client. A reorg replaces
// two blocks; the client, applying messages in order, must end with exactly
// the trades and candles market-engine stored.
func TestSmokeLiveThroughKafka(t *testing.T) {
	smokeLive(t, func(brokers []string) (Sink, Feed) {
		sink, err := NewKafkaSink(KafkaOptions{Brokers: brokers})
		if err != nil {
			t.Fatalf("NewKafkaSink() error = %v", err)
		}
		feed, err := NewKafkaFeed(KafkaOptions{Brokers: brokers, FetchMaxWait: 20 * time.Millisecond})
		if err != nil {
			t.Fatalf("NewKafkaFeed() error = %v", err)
		}
		return sink, feed
	})
}

// TestSmokeLiveThroughRedis is TestSmokeLiveThroughKafka with live updates
// carried over Redis Pub/Sub.
func TestSmokeLiveThroughRedis(t *testing.T) {
	server := miniredis.RunT(t)
	smokeLive(t, func([]string) (Sink, Feed) {
		options := RedisOptions{Addr: server.Addr()}
		sink, err := NewRedisSink(options)
		if err != nil {
			t.Fatalf("NewRedisSink() error = %v", err)
		}
		feed, err := NewRedisFeed(options)
		if err != nil {
			t.Fatalf("NewRedisFeed() error = %v", err)
		}
		return sink, feed
	})
}

func smokeLive(t *testing.T, transport func(brokers []string) (Sink, Feed)) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, smokeTopic, smokeLiveTopic))
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("producer error = %v", err)
	}
	defer producer.Close()
	produce := func(topic string, value []byte) {
		t.Helper()
		if produceErr := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: value}).FirstErr(); produceErr != nil {
			t.Fatalf("ProduceSync() error = %v", produceErr)
		}
	}

	sink, feed := transport(brokers)
	intervals := []time.Duration{time.Minute, time.Hour}
	hub, err := NewHub(HubOptions{
		Feed: feed, Topics: map[uint64]string{1: smokeLiveTopic},
		Intervals: intervals, Throttle: 10 * time.Millisecond, ClientBuffer: 1024, MaxSubscriptions: 10,
	})
	if err != nil {
		t.Fatalf("NewHub() error = %v", err)
	}
	hubDone := make(chan error, 1)
	go func() { hubDone <- hub.Run(ctx) }()
	server := httptest.NewServer(hub)
	defer server.Close()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	if response != nil && response.Body != nil {
		_ = response.Body.Close() //nolint:errcheck // nothing to read
	}
	defer conn.CloseNow() //nolint:errcheck // test cleanup
	channels := []string{
		TradeChannel(1, "pool", wethUSDC), TradeChannel(1, "token", token),
		CandleChannel(1, "pool", wethUSDC, "1m"), CandleChannel(1, "token", token, "1m"),
		CandleChannel(1, "token", weth, "1m"),
	}
	request, _ := json.Marshal(map[string]any{"op": "subscribe", "channels": channels}) //nolint:errcheck // plain data
	if err = conn.Write(ctx, websocket.MessageText, request); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	// A read that times out closes the connection, so one goroutine reads
	// and the test waits on its channel.
	messages := make(chan []byte, 1024)
	go func() {
		defer close(messages)
		for {
			_, payload, readErr := conn.Read(ctx)
			if readErr != nil {
				return
			}
			messages <- payload
		}
	}()
	read := func(timeout time.Duration) ([]byte, bool) {
		select {
		case payload, ok := <-messages:
			return payload, ok
		case <-time.After(timeout):
			return nil, false
		}
	}
	// The hub starts at the end of the live topic; probe until it follows.
	deadline := time.Now().Add(10 * time.Second)
	for probing := true; probing; {
		if time.Now().After(deadline) {
			t.Fatal("hub never consumed the probe")
		}
		if sendErr := sink.Send(ctx, smokeLiveTopic, [][]byte{[]byte(`{"type":"block","chain_id":1,"height":1}`)}); sendErr != nil {
			t.Fatalf("Send() error = %v", sendErr)
		}
		for {
			payload, ok := read(500 * time.Millisecond)
			if !ok {
				break
			}
			if strings.Contains(string(payload), `"type":"head"`) {
				probing = false
				break
			}
		}
	}

	start := time.Now().Truncate(time.Minute).Unix()
	block := func(height, offset int64, hash, parent string, trades ...*streamv1.Trade) {
		record := &streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Block{Block: &streamv1.BlockEvent{
			ChainId: 1, Chain: "eth", BlockHeight: height, BlockHash: hash, ParentHash: parent,
			BlockTime: start + offset, Trades: trades,
		}}}
		value, _ := proto.Marshal(record) //nolint:errcheck // plain data
		produce(smokeTopic, value)
	}
	block(100, 0, "0xa100", "0xa099", wethSwap(0, "3000"))
	block(101, 12, "0xa101", "0xa100", wethSwap(0, "3000"), memeSell(1))
	block(102, 72, "0xa102", "0xa101", wethSwap(0, "3000"))
	revert, _ := proto.Marshal(&streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Revert{ //nolint:errcheck // plain data
		Revert: &streamv1.BlockRevert{ChainId: 1, RollbackTo: 100},
	}})
	produce(smokeTopic, revert)
	block(101, 12, "0xb101", "0xa100", wethSwap(0, "3100"))
	block(102, 24, "0xb102", "0xb101")

	processor, err := market.NewProcessor(market.Options{
		Catalog: market.DefaultCatalog(1), MinLiquidityUSD: decimal.NewFromInt(1000),
		CandleMinLiquidityUSD: decimal.NewFromInt(10000), MaxHops: 3, Intervals: intervals, HistoryDepth: 16,
	})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	publisher, err := NewPublisher(PublisherOptions{Sink: sink, Topics: map[uint64]string{1: smokeLiveTopic}, MaxLag: -1})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	defer publisher.Close()
	memory := storage.NewMemory()
	kafka := source.KafkaOptions{Brokers: brokers, Topic: smokeTopic, FetchMaxBytes: 1 << 20, FetchMaxWait: 20 * time.Millisecond}
	chainEngine, err := engine.New(engine.Options{
		ChainID: 1, Chain: "eth", Processor: processor, Output: memory, Checkpoints: memory, Notifier: publisher,
		Open: func(start int64) (source.Source, error) { return source.NewKafka(kafka, start) },
	})
	if err != nil {
		t.Fatalf("engine.New() error = %v", err)
	}
	engineDone := make(chan error, 1)
	go func() { engineDone <- chainEngine.Run(ctx) }()

	for {
		checkpoint, _, _ := memory.Load(ctx, 1) //nolint:errcheck // memory never fails
		if checkpoint.Offset == 5 {
			break
		}
		select {
		case runErr := <-engineDone:
			t.Fatalf("engine stopped: %v", runErr)
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline.Add(10 * time.Second)) {
			t.Fatalf("engine committed through offset %d, want 5", checkpoint.Offset)
		}
	}

	want := storedModel(memory, channels)
	if len(want.trades[TradeChannel(1, "token", token)]) != 0 || len(want.trades[TradeChannel(1, "pool", wethUSDC)]) != 2 ||
		!strings.Contains(want.dump(), "c=3100 n=2") {
		t.Fatalf("stored trades:\n%s", want.dump())
	}
	model := newStreamModel()
	for model.dump() != want.dump() || model.reverts == 0 {
		payload, ok := read(5 * time.Second)
		if !ok {
			t.Fatalf("client state:\n%s\nwant:\n%s", model.dump(), want.dump())
		}
		model.apply(t, payload)
	}
	// MEME's pool and token series existed only in the reverted blocks.
	if model.emptied == 0 {
		t.Fatal("no series was reset to empty")
	}
	cancel()
	if runErr := <-engineDone; runErr != nil {
		t.Fatalf("engine Run() error = %v", runErr)
	}
	if runErr := <-hubDone; runErr != nil {
		t.Fatalf("hub Run() error = %v", runErr)
	}
}
