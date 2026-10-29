package live

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Sink carries market-engine's live records to market-api. Send writes
// values to topic, in order, and returns once the transport accepted them.
//
// Sink 是 market-engine 侧的发送端（Kafka 或 Redis Pub/Sub），按顺序写入并在被接受后返回。
type Sink interface {
	Send(ctx context.Context, topic string, values [][]byte) error
	Close()
}

// Feed delivers what is sent to topics from the moment Run starts, in order
// per topic, until ctx ends. Run returns nil when ctx ends.
//
// Feed 是 market-api 侧的接收端，只投递 Run 开始之后发布的内容（从 topic 末尾读，不用消费组）。
type Feed interface {
	Run(ctx context.Context, topics []string, deliver func(topic string, value []byte)) error
}

type KafkaOptions struct {
	Brokers  []string
	ClientID string
	// FetchMaxBytes and FetchMaxWait tune the feed; zero keeps the client's
	// defaults.
	FetchMaxBytes int32
	FetchMaxWait  time.Duration
	Logger        *slog.Logger
}

func (options KafkaOptions) clientOptions() []kgo.Opt {
	var clientOptions []kgo.Opt
	if clientID := strings.TrimSpace(options.ClientID); clientID != "" {
		clientOptions = append(clientOptions, kgo.ClientID(clientID))
	}
	return append(clientOptions, kgo.SeedBrokers(options.Brokers...))
}

// KafkaSink produces each topic's records to its partition zero. Topics are
// created on first write when the broker allows it.
type KafkaSink struct {
	client *kgo.Client
}

func NewKafkaSink(options KafkaOptions) (*KafkaSink, error) {
	if len(options.Brokers) == 0 {
		return nil, errors.New("live kafka sink: brokers are required")
	}
	client, err := kgo.NewClient(append(options.clientOptions(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowAutoTopicCreation(),
	)...)
	if err != nil {
		return nil, fmt.Errorf("live kafka sink: create client: %w", err)
	}
	return &KafkaSink{client: client}, nil
}

func (sink *KafkaSink) Send(ctx context.Context, topic string, values [][]byte) error {
	records := make([]*kgo.Record, len(values))
	for index, value := range values {
		records[index] = &kgo.Record{Topic: topic, Partition: Partition, Value: value}
	}
	if err := sink.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("live kafka sink: produce to %s: %w", topic, err)
	}
	return nil
}

func (sink *KafkaSink) Close() { sink.client.Close() }

// KafkaFeed consumes each topic's partition zero from its end.
type KafkaFeed struct {
	options KafkaOptions
	logger  *slog.Logger
}

func NewKafkaFeed(options KafkaOptions) (*KafkaFeed, error) {
	if len(options.Brokers) == 0 {
		return nil, errors.New("live kafka feed: brokers are required")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &KafkaFeed{options: options, logger: logger}, nil
}

func (feed *KafkaFeed) Run(ctx context.Context, topics []string, deliver func(topic string, value []byte)) error {
	partitions := make(map[string]map[int32]kgo.Offset, len(topics))
	for _, topic := range topics {
		partitions[topic] = map[int32]kgo.Offset{Partition: kgo.NewOffset().AtEnd()}
	}
	clientOptions := append(feed.options.clientOptions(),
		kgo.ConsumePartitions(partitions),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if fetchMaxBytes := feed.options.FetchMaxBytes; fetchMaxBytes > 0 {
		clientOptions = append(clientOptions,
			kgo.FetchMaxBytes(fetchMaxBytes), kgo.FetchMaxPartitionBytes(fetchMaxBytes),
			kgo.BrokerMaxReadBytes(fetchMaxBytes+1<<20))
	}
	if feed.options.FetchMaxWait > 0 {
		clientOptions = append(clientOptions, kgo.FetchMaxWait(feed.options.FetchMaxWait))
	}
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return fmt.Errorf("live kafka feed: create client: %w", err)
	}
	defer client.Close()
	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		for _, fetchErr := range fetches.Errors() {
			feed.logger.Warn("live hub fetch failed", "topic", fetchErr.Topic, "error", fetchErr.Err)
		}
		fetches.EachRecord(func(record *kgo.Record) { deliver(record.Topic, record.Value) })
	}
}

type RedisOptions struct {
	Addr        string
	Username    string
	Password    string
	DB          int
	DialTimeout time.Duration
}

func (options RedisOptions) client() (*redis.Client, error) {
	if strings.TrimSpace(options.Addr) == "" {
		return nil, errors.New("live redis: addr is required")
	}
	return redis.NewClient(&redis.Options{
		Addr: options.Addr, Username: options.Username, Password: options.Password,
		DB: options.DB, DialTimeout: options.DialTimeout,
	}), nil
}

// RedisSink publishes each topic's records to the Redis Pub/Sub channel of
// the same name. A pipeline keeps them in order.
type RedisSink struct {
	client *redis.Client
}

func NewRedisSink(options RedisOptions) (*RedisSink, error) {
	client, err := options.client()
	if err != nil {
		return nil, err
	}
	return &RedisSink{client: client}, nil
}

func (sink *RedisSink) Send(ctx context.Context, topic string, values [][]byte) error {
	pipeline := sink.client.Pipeline()
	for _, value := range values {
		pipeline.Publish(ctx, topic, value)
	}
	if _, err := pipeline.Exec(ctx); err != nil {
		return fmt.Errorf("live redis sink: publish to %s: %w", topic, err)
	}
	return nil
}

func (sink *RedisSink) Close() { _ = sink.client.Close() }

// RedisFeed subscribes to the Redis Pub/Sub channels named after the topics.
// The client resubscribes after a lost connection; what was published in
// between is not delivered.
type RedisFeed struct {
	options RedisOptions
}

func NewRedisFeed(options RedisOptions) (*RedisFeed, error) {
	if strings.TrimSpace(options.Addr) == "" {
		return nil, errors.New("live redis: addr is required")
	}
	return &RedisFeed{options: options}, nil
}

func (feed *RedisFeed) Run(ctx context.Context, topics []string, deliver func(topic string, value []byte)) error {
	client, err := feed.options.client()
	if err != nil {
		return err
	}
	defer client.Close() //nolint:errcheck // nothing to do on failure
	subscription := client.Subscribe(ctx, topics...)
	defer subscription.Close() //nolint:errcheck // nothing to do on failure
	// Wait for the subscription, so nothing published after Run returns
	// its first message is missed.
	if _, err := subscription.Receive(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("live redis feed: subscribe: %w", err)
	}
	messages := subscription.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-messages:
			if !ok {
				return errors.New("live redis feed: subscription closed")
			}
			deliver(message.Channel, []byte(message.Payload))
		}
	}
}

var (
	_ Sink = (*KafkaSink)(nil)
	_ Sink = (*RedisSink)(nil)
	_ Feed = (*KafkaFeed)(nil)
	_ Feed = (*RedisFeed)(nil)
)
