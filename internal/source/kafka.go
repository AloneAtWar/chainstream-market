// Package source reads a chainstream-evm chain topic.
package source

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is one raw StreamRecord value and its position in the topic.
type Message struct {
	Offset int64
	Value  []byte
}

// Source yields a chain topic's records in offset order.
type Source interface {
	// Fetch blocks until at least one record is available or ctx is done.
	Fetch(ctx context.Context) ([]Message, error)
	Close()
}

type KafkaOptions struct {
	Brokers       []string
	ClientID      string
	Topic         string
	FetchMaxBytes int32
	FetchMaxWait  time.Duration
}

// Lagger is a Source that knows where its topic ends.
type Lagger interface {
	// HighWatermark returns the offset after the topic's last record, as of
	// the last fetch, or -1 before one returned it.
	HighWatermark() int64
}

// Partition is the only partition chainstream-evm writes to; total order
// across reverts and blocks depends on it.
const Partition int32 = 0

// Kafka consumes partition zero of one topic without a consumer group. The
// engine owns the offset through its checkpoint store.
type Kafka struct {
	client        *kgo.Client
	topic         string
	highWatermark int64
}

// NewKafka starts consuming at offset start.
func NewKafka(options KafkaOptions, start int64) (*Kafka, error) {
	topic := strings.TrimSpace(options.Topic)
	if len(options.Brokers) == 0 || topic == "" {
		return nil, errors.New("kafka source: brokers and topic are required")
	}
	if start < 0 {
		return nil, fmt.Errorf("kafka source: invalid start offset %d", start)
	}
	if options.FetchMaxBytes <= 0 || options.FetchMaxWait <= 0 {
		return nil, errors.New("kafka source: fetch limits must be positive")
	}
	clientOptions := []kgo.Opt{
		kgo.SeedBrokers(options.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {Partition: kgo.NewOffset().At(start)},
		}),
		// Never silently jump: a checkpoint outside the retained range must
		// fail rather than skip or replay blocks.
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.FetchMaxBytes(options.FetchMaxBytes),
		kgo.FetchMaxPartitionBytes(options.FetchMaxBytes),
		kgo.BrokerMaxReadBytes(options.FetchMaxBytes + 1<<20),
		kgo.FetchMaxWait(options.FetchMaxWait),
	}
	if clientID := strings.TrimSpace(options.ClientID); clientID != "" {
		clientOptions = append(clientOptions, kgo.ClientID(clientID))
	}
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("kafka source: create client: %w", err)
	}
	return &Kafka{client: client, topic: topic, highWatermark: -1}, nil
}

func (source *Kafka) Fetch(ctx context.Context) ([]Message, error) {
	fetches := source.client.PollFetches(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fetches.IsClientClosed() {
		return nil, errors.New("kafka source: client closed")
	}
	for _, fetchErr := range fetches.Errors() {
		if errors.Is(fetchErr.Err, context.Canceled) || errors.Is(fetchErr.Err, context.DeadlineExceeded) {
			continue
		}
		return nil, fmt.Errorf("kafka source: fetch %s/%d: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
	}
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		if partition.Topic == source.topic && partition.Partition == Partition && partition.HighWatermark >= 0 {
			source.highWatermark = partition.HighWatermark
		}
	})
	var messages []Message
	fetches.EachRecord(func(record *kgo.Record) {
		if record.Topic == source.topic && record.Partition == Partition {
			messages = append(messages, Message{Offset: record.Offset, Value: record.Value})
		}
	})
	return messages, nil
}

func (source *Kafka) HighWatermark() int64 {
	return source.highWatermark
}

func (source *Kafka) Close() {
	source.client.Close()
}

var (
	_ Source = (*Kafka)(nil)
	_ Lagger = (*Kafka)(nil)
)
