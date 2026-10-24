package live

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Partition is the only partition of a live topic, so a chain's updates stay
// in order.
const Partition int32 = 0

// maxItemsPerRecord caps the trades, candles or resets in one record, which
// keeps records well under Kafka's default 1 MB message limit.
const maxItemsPerRecord = 500

type PublisherOptions struct {
	Brokers  []string
	ClientID string
	// Topics maps chain IDs to their live topics. Chains without one are not
	// published.
	Topics map[uint64]string
	// MaxLag skips blocks whose time is further than this behind the clock,
	// so catching up does not flood clients with history; reverts are always
	// published. Negative publishes every block.
	MaxLag time.Duration
	Now    func() time.Time
}

// Publisher writes market-engine's per-block output and reorg corrections to
// the live topics. Topics are created on first write when the broker allows
// it.
type Publisher struct {
	client *kgo.Client
	topics map[uint64]string
	maxLag time.Duration
	now    func() time.Time
}

func NewPublisher(options PublisherOptions) (*Publisher, error) {
	if len(options.Brokers) == 0 || len(options.Topics) == 0 {
		return nil, errors.New("live publisher: brokers and topics are required")
	}
	clientOptions := []kgo.Opt{
		kgo.SeedBrokers(options.Brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowAutoTopicCreation(),
	}
	if clientID := strings.TrimSpace(options.ClientID); clientID != "" {
		clientOptions = append(clientOptions, kgo.ClientID(clientID))
	}
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("live publisher: create client: %w", err)
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Publisher{client: client, topics: options.Topics, maxLag: options.MaxLag, now: now}, nil
}

func (publisher *Publisher) NotifyBlock(ctx context.Context, output storage.BlockOutput, finalizedHeight int64) error {
	topic, ok := publisher.topics[output.ChainID]
	if !ok || (publisher.maxLag >= 0 && publisher.now().Sub(output.BlockTime) > publisher.maxLag) {
		return nil
	}
	return publisher.produce(ctx, topic, BlockUpdates(output, finalizedHeight))
}

func (publisher *Publisher) NotifyRevert(ctx context.Context, revert storage.Revert) error {
	topic, ok := publisher.topics[revert.ChainID]
	if !ok {
		return nil
	}
	return publisher.produce(ctx, topic, RevertUpdates(revert))
}

func (publisher *Publisher) produce(ctx context.Context, topic string, updates []Update) error {
	records := make([]*kgo.Record, len(updates))
	for index, update := range updates {
		value, err := json.Marshal(update)
		if err != nil {
			return fmt.Errorf("live publisher: encode: %w", err)
		}
		records[index] = &kgo.Record{Topic: topic, Partition: Partition, Value: value}
	}
	if err := publisher.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("live publisher: produce to %s: %w", topic, err)
	}
	return nil
}

func (publisher *Publisher) Close() {
	publisher.client.Close()
}

// BlockUpdates splits a block's output into live records. Every record
// carries the block header, and the first one carries it even when the
// block derived nothing, so clients still learn the chain head.
func BlockUpdates(output storage.BlockOutput, finalizedHeight int64) []Update {
	header := Update{
		Type: UpdateBlock, ChainID: output.ChainID, Height: output.Height,
		BlockTime: output.BlockTime.Unix(), FinalizedHeight: finalizedHeight,
	}
	updates := []Update{header}
	add := func(fill func(*Update)) {
		last := &updates[len(updates)-1]
		if len(last.Trades)+len(last.Candles) >= maxItemsPerRecord {
			next := header
			next.Part = len(updates)
			updates = append(updates, next)
			last = &updates[len(updates)-1]
		}
		fill(last)
	}
	for _, trade := range output.Trades {
		add(func(update *Update) { update.Trades = append(update.Trades, tradeOf(trade, output.Height)) })
	}
	for _, candle := range output.Candles {
		add(func(update *Update) { update.Candles = append(update.Candles, candleOf(candle)) })
	}
	return updates
}

// RevertUpdates splits a revert into live records.
func RevertUpdates(revert storage.Revert) []Update {
	updates := []Update{{Type: UpdateRevert, ChainID: revert.ChainID, Height: revert.RollbackTo}}
	for _, reset := range revert.Candles {
		last := &updates[len(updates)-1]
		if len(last.Resets) >= maxItemsPerRecord {
			updates = append(updates, Update{
				Type: UpdateRevert, ChainID: revert.ChainID, Height: revert.RollbackTo, Part: len(updates),
			})
			last = &updates[len(updates)-1]
		}
		last.Resets = append(last.Resets, resetOf(reset))
	}
	return updates
}

var _ engine.Notifier = (*Publisher)(nil)
