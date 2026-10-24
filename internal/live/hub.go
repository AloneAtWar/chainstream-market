package live

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/market"
	"github.com/twmb/franz-go/pkg/kgo"
)

type HubOptions struct {
	Brokers       []string
	ClientID      string
	FetchMaxBytes int32
	FetchMaxWait  time.Duration
	// Topics maps chain IDs to the live topics market-engine publishes.
	Topics    map[uint64]string
	Intervals []time.Duration
	// Throttle is how often coalesced candle and head messages are sent.
	// A bucket that changes several times in between is sent once, at its
	// latest value.
	Throttle time.Duration
	// ClientBuffer is how many messages may wait for a slow client before
	// it is disconnected.
	ClientBuffer     int
	MaxSubscriptions int
	// OriginPatterns lists the other origins browsers may connect from, as
	// host patterns like "localhost:3000" or "*.example.com".
	OriginPatterns []string
	Logger         *slog.Logger
}

// Hub fans the live topics out to WebSocket clients by channel.
//
// Trades are sent as soon as their block arrives. Candles and the chain head
// are coalesced and sent every Throttle. A revert first sends whatever is
// coalesced, then the revert and the reset of each affected series, so a
// client that applies messages in order always ends in the corrected state.
type Hub struct {
	options   HubOptions
	logger    *slog.Logger
	intervals map[string]struct{}

	mu       sync.Mutex
	channels map[string]map[*Client]struct{}
	// chains counts each client's subscriptions per chain; chain-wide
	// messages go to every client with at least one.
	chains  map[uint64]map[*Client]int
	pending map[uint64]*pendingChain

	stopped chan struct{}
}

type pendingChain struct {
	candles map[candleSlot]Candle
	head    *headMessage
}

type candleSlot struct {
	channel  string
	openTime int64
}

// Messages sent to clients.
type (
	tradesMessage struct {
		Type    string  `json:"type"`
		Channel string  `json:"channel"`
		Height  int64   `json:"height"`
		Trades  []Trade `json:"trades"`
	}
	candleMessage struct {
		Type    string  `json:"type"`
		Channel string  `json:"channel"`
		Candle  *Candle `json:"candle"`
	}
	headMessage struct {
		Type            string `json:"type"`
		ChainID         uint64 `json:"chain_id"`
		Height          int64  `json:"height"`
		FinalizedHeight int64  `json:"finalized_height"`
	}
	revertMessage struct {
		Type       string `json:"type"`
		ChainID    uint64 `json:"chain_id"`
		RollbackTo int64  `json:"rollback_to"`
	}
	replyMessage struct {
		Type     string   `json:"type"`
		Channels []string `json:"channels,omitempty"`
		Error    string   `json:"error,omitempty"`
	}
)

func NewHub(options HubOptions) (*Hub, error) {
	if len(options.Topics) == 0 || len(options.Intervals) == 0 {
		return nil, errors.New("live hub: topics and intervals are required")
	}
	if options.Throttle <= 0 || options.ClientBuffer <= 0 || options.MaxSubscriptions <= 0 {
		return nil, errors.New("live hub: throttle, client buffer and max subscriptions must be positive")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	intervals := make(map[string]struct{}, len(options.Intervals))
	for _, interval := range options.Intervals {
		intervals[IntervalName(interval)] = struct{}{}
	}
	return &Hub{
		options: options, logger: logger, intervals: intervals,
		channels: make(map[string]map[*Client]struct{}),
		chains:   make(map[uint64]map[*Client]int),
		pending:  make(map[uint64]*pendingChain),
		stopped:  make(chan struct{}),
	}, nil
}

// Run consumes the live topics from their end until ctx is done. Clients
// connected to the hub are closed when it returns.
func (hub *Hub) Run(ctx context.Context) error {
	defer close(hub.stopped)
	partitions := make(map[string]map[int32]kgo.Offset, len(hub.options.Topics))
	for _, topic := range hub.options.Topics {
		partitions[topic] = map[int32]kgo.Offset{Partition: kgo.NewOffset().AtEnd()}
	}
	clientOptions := []kgo.Opt{
		kgo.SeedBrokers(hub.options.Brokers...),
		kgo.ConsumePartitions(partitions),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	}
	if hub.options.FetchMaxBytes > 0 {
		clientOptions = append(clientOptions,
			kgo.FetchMaxBytes(hub.options.FetchMaxBytes), kgo.FetchMaxPartitionBytes(hub.options.FetchMaxBytes),
			kgo.BrokerMaxReadBytes(hub.options.FetchMaxBytes+1<<20))
	}
	if hub.options.FetchMaxWait > 0 {
		clientOptions = append(clientOptions, kgo.FetchMaxWait(hub.options.FetchMaxWait))
	}
	if clientID := strings.TrimSpace(hub.options.ClientID); clientID != "" {
		clientOptions = append(clientOptions, kgo.ClientID(clientID))
	}
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return fmt.Errorf("live hub: create client: %w", err)
	}
	defer client.Close()

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		ticker := time.NewTicker(hub.options.Throttle)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				hub.flush()
			}
		}
	})
	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		for _, fetchErr := range fetches.Errors() {
			hub.logger.Warn("live hub fetch failed", "topic", fetchErr.Topic, "error", fetchErr.Err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			var update Update
			if decodeErr := json.Unmarshal(record.Value, &update); decodeErr != nil {
				hub.logger.Warn("live hub skipped a bad record", "topic", record.Topic, "offset", record.Offset, "error", decodeErr)
				return
			}
			hub.dispatch(update)
		})
	}
}

// dispatch routes one live record to the subscribed clients.
func (hub *Hub) dispatch(update Update) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	switch update.Type {
	case UpdateBlock:
		hub.dispatchBlock(update)
	case UpdateRevert:
		hub.flushChain(update.ChainID)
		if update.Part == 0 {
			hub.sendChain(update.ChainID, revertMessage{Type: "revert", ChainID: update.ChainID, RollbackTo: update.Height})
		}
		for _, reset := range update.Resets {
			channel := CandleChannel(update.ChainID, reset.Subject, reset.Key, reset.Interval)
			hub.send(channel, candleMessage{Type: "candle_reset", Channel: channel, Candle: reset.Latest})
		}
	default:
		hub.logger.Warn("live hub skipped an unknown update", "type", update.Type)
	}
}

func (hub *Hub) dispatchBlock(update Update) {
	pending := hub.pendingFor(update.ChainID)
	if update.Part == 0 && (pending.head == nil || update.Height >= pending.head.Height) {
		pending.head = &headMessage{
			Type: "head", ChainID: update.ChainID, Height: update.Height, FinalizedHeight: update.FinalizedHeight,
		}
	}
	// A block's trades go out as one message per channel, in block order.
	var order []string
	grouped := make(map[string][]Trade)
	for _, trade := range update.Trades {
		for _, channel := range []string{
			TradeChannel(update.ChainID, "pool", trade.PoolID),
			TradeChannel(update.ChainID, "token", trade.BaseToken),
		} {
			if len(hub.channels[channel]) == 0 {
				continue
			}
			if _, seen := grouped[channel]; !seen {
				order = append(order, channel)
			}
			grouped[channel] = append(grouped[channel], trade)
		}
	}
	for _, channel := range order {
		hub.send(channel, tradesMessage{Type: "trades", Channel: channel, Height: update.Height, Trades: grouped[channel]})
	}
	for _, candle := range update.Candles {
		channel := CandleChannel(update.ChainID, candle.Subject, candle.Key, candle.Interval)
		if len(hub.channels[channel]) > 0 {
			pending.candles[candleSlot{channel: channel, openTime: candle.OpenTime}] = candle
		}
	}
}

func (hub *Hub) pendingFor(chainID uint64) *pendingChain {
	pending, ok := hub.pending[chainID]
	if !ok {
		pending = &pendingChain{candles: make(map[candleSlot]Candle)}
		hub.pending[chainID] = pending
	}
	return pending
}

// flush sends every coalesced candle and head.
func (hub *Hub) flush() {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	for _, chainID := range slices.Sorted(maps.Keys(hub.pending)) {
		hub.flushChain(chainID)
	}
}

func (hub *Hub) flushChain(chainID uint64) {
	pending, ok := hub.pending[chainID]
	if !ok {
		return
	}
	slots := slices.SortedFunc(maps.Keys(pending.candles), func(left, right candleSlot) int {
		return cmp.Or(cmp.Compare(left.channel, right.channel), cmp.Compare(left.openTime, right.openTime))
	})
	for _, slot := range slots {
		candle := pending.candles[slot]
		hub.send(slot.channel, candleMessage{Type: "candle", Channel: slot.channel, Candle: &candle})
	}
	clear(pending.candles)
	if pending.head != nil {
		hub.sendChain(chainID, *pending.head)
		pending.head = nil
	}
}

func (hub *Hub) send(channel string, message any) {
	clients := hub.channels[channel]
	if len(clients) == 0 {
		return
	}
	payload := hub.encode(message)
	for client := range clients {
		hub.enqueue(client, payload)
	}
}

func (hub *Hub) sendChain(chainID uint64, message any) {
	clients := hub.chains[chainID]
	if len(clients) == 0 {
		return
	}
	payload := hub.encode(message)
	for client := range clients {
		hub.enqueue(client, payload)
	}
}

func (hub *Hub) encode(message any) []byte {
	payload, err := json.Marshal(message)
	if err != nil {
		// Every message is plain data; this cannot happen.
		panic(fmt.Sprintf("live hub: encode %T: %v", message, err))
	}
	return payload
}

// enqueue queues payload for client, or drops a client that has fallen
// ClientBuffer messages behind. A dropped client would otherwise miss
// corrections, so it is disconnected and must load history again.
func (hub *Hub) enqueue(client *Client, payload []byte) {
	if client.gone {
		return
	}
	select {
	case client.send <- payload:
	default:
		hub.logger.Warn("live client too slow, disconnecting", "subscriptions", len(client.subscriptions))
		hub.remove(client)
		close(client.dropped)
	}
}

// remove forgets client's subscriptions. The caller holds hub.mu.
func (hub *Hub) remove(client *Client) {
	if client.gone {
		return
	}
	client.gone = true
	for channel, chainID := range client.subscriptions {
		hub.unsubscribe(client, channel, chainID)
	}
}

func (hub *Hub) unsubscribe(client *Client, channel string, chainID uint64) {
	delete(client.subscriptions, channel)
	if clients := hub.channels[channel]; clients != nil {
		delete(clients, client)
		if len(clients) == 0 {
			delete(hub.channels, channel)
		}
	}
	if counts := hub.chains[chainID]; counts != nil {
		counts[client]--
		if counts[client] <= 0 {
			delete(counts, client)
		}
		if len(counts) == 0 {
			delete(hub.chains, chainID)
		}
	}
}

// Client is one connection's subscriptions and outgoing queue.
type Client struct {
	hub  *Hub
	send chan []byte
	// dropped is closed when the hub disconnects the client for being slow.
	dropped chan struct{}
	// subscriptions maps channels to their chain. Guarded by hub.mu.
	subscriptions map[string]uint64
	gone          bool
}

// Connect registers a client with no subscriptions.
func (hub *Hub) Connect() *Client {
	return &Client{
		hub: hub, send: make(chan []byte, hub.options.ClientBuffer), dropped: make(chan struct{}),
		subscriptions: make(map[string]uint64),
	}
}

// Subscribe adds channels and queues a "subscribed" reply naming them as the
// hub spells them; messages for a channel use that spelling. Nothing is
// added when any channel is invalid.
func (client *Client) Subscribe(names []string) error {
	hub := client.hub
	parsed := make([]parsedChannel, 0, len(names))
	for _, name := range names {
		channel, err := hub.parseChannel(name)
		if err != nil {
			return err
		}
		parsed = append(parsed, channel)
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if client.gone {
		return errors.New("connection closed")
	}
	added := 0
	for _, channel := range parsed {
		if _, ok := client.subscriptions[channel.name]; !ok {
			added++
		}
	}
	if len(client.subscriptions)+added > hub.options.MaxSubscriptions {
		return fmt.Errorf("at most %d subscriptions per connection", hub.options.MaxSubscriptions)
	}
	canonical := make([]string, len(parsed))
	for index, channel := range parsed {
		canonical[index] = channel.name
		if _, ok := client.subscriptions[channel.name]; ok {
			continue
		}
		client.subscriptions[channel.name] = channel.chainID
		if hub.channels[channel.name] == nil {
			hub.channels[channel.name] = make(map[*Client]struct{})
		}
		hub.channels[channel.name][client] = struct{}{}
		if hub.chains[channel.chainID] == nil {
			hub.chains[channel.chainID] = make(map[*Client]int)
		}
		hub.chains[channel.chainID][client]++
	}
	hub.enqueue(client, hub.encode(replyMessage{Type: "subscribed", Channels: canonical}))
	return nil
}

// Unsubscribe removes channels and queues an "unsubscribed" reply. Unknown
// channels are ignored.
func (client *Client) Unsubscribe(names []string) {
	hub := client.hub
	canonical := make([]string, 0, len(names))
	for _, name := range names {
		if channel, err := hub.parseChannel(name); err == nil {
			canonical = append(canonical, channel.name)
		}
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	for _, name := range canonical {
		if chainID, ok := client.subscriptions[name]; ok {
			hub.unsubscribe(client, name, chainID)
		}
	}
	hub.enqueue(client, hub.encode(replyMessage{Type: "unsubscribed", Channels: canonical}))
}

// Fail queues an error reply.
func (client *Client) Fail(message string) {
	client.hub.mu.Lock()
	defer client.hub.mu.Unlock()
	client.hub.enqueue(client, client.hub.encode(replyMessage{Type: "error", Error: message}))
}

// Close unregisters the client.
func (client *Client) Close() {
	client.hub.mu.Lock()
	defer client.hub.mu.Unlock()
	client.hub.remove(client)
}

type parsedChannel struct {
	name    string
	chainID uint64
}

// parseChannel checks a channel name and returns it canonically spelled:
// addresses lowercased and token aliases, such as the native
// pseudo-address, mapped to the token that carries their market data.
func (hub *Hub) parseChannel(name string) (parsedChannel, error) {
	parts := strings.Split(strings.TrimSpace(name), ":")
	invalid := fmt.Errorf("invalid channel %q: want trades:<chain>:<pool|token>:<address> or candles:<chain>:<pool|token>:<address>:<interval>", name)
	if len(parts) < 4 {
		return parsedChannel{}, invalid
	}
	chainID, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return parsedChannel{}, invalid
	}
	if _, ok := hub.options.Topics[chainID]; !ok {
		return parsedChannel{}, fmt.Errorf("chain %d is not streamed", chainID)
	}
	subject, key := parts[2], strings.ToLower(parts[3])
	if (subject != "pool" && subject != "token") || key == "" {
		return parsedChannel{}, invalid
	}
	if subject == "token" {
		key = market.DefaultCatalog(chainID).Canonical(key)
	}
	switch {
	case parts[0] == "trades" && len(parts) == 4:
		return parsedChannel{name: TradeChannel(chainID, subject, key), chainID: chainID}, nil
	case parts[0] == "candles" && len(parts) == 5:
		if _, ok := hub.intervals[parts[4]]; !ok {
			return parsedChannel{}, fmt.Errorf("interval %q is not one of market.intervals", parts[4])
		}
		return parsedChannel{name: CandleChannel(chainID, subject, key, parts[4]), chainID: chainID}, nil
	default:
		return parsedChannel{}, invalid
	}
}
