package source

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaFetchesFromStartOffset(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "chainstream-eth"))
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("producer error = %v", err)
	}
	defer producer.Close()
	for index := range 5 {
		record := &kgo.Record{Topic: "chainstream-eth", Partition: 0, Value: []byte(strconv.Itoa(index))}
		if produceErr := producer.ProduceSync(ctx, record).FirstErr(); produceErr != nil {
			t.Fatalf("ProduceSync() error = %v", produceErr)
		}
	}

	source, err := NewKafka(KafkaOptions{
		Brokers: cluster.ListenAddrs(), Topic: "chainstream-eth",
		FetchMaxBytes: 1 << 20, FetchMaxWait: 100 * time.Millisecond,
	}, 2)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	defer source.Close()

	var got []Message
	for len(got) < 3 {
		messages, err := source.Fetch(ctx)
		if err != nil {
			t.Fatalf("Fetch() error = %v", err)
		}
		got = append(got, messages...)
	}
	for index, message := range got {
		wantOffset := int64(index + 2)
		if message.Offset != wantOffset || string(message.Value) != strconv.Itoa(index+2) {
			t.Fatalf("message %d = %d/%s, want offset %d", index, message.Offset, message.Value, wantOffset)
		}
	}
	if end := source.HighWatermark(); end != 5 {
		t.Fatalf("HighWatermark() = %d, want 5", end)
	}
}

func TestNewKafkaValidatesOptions(t *testing.T) {
	valid := KafkaOptions{Brokers: []string{"b"}, Topic: "t", FetchMaxBytes: 1, FetchMaxWait: time.Millisecond}
	tests := map[string]struct {
		options KafkaOptions
		start   int64
	}{
		"no brokers":     {KafkaOptions{Topic: "t", FetchMaxBytes: 1, FetchMaxWait: time.Millisecond}, 0},
		"no topic":       {KafkaOptions{Brokers: []string{"b"}, FetchMaxBytes: 1, FetchMaxWait: time.Millisecond}, 0},
		"negative start": {valid, -1},
		"no fetch bytes": {KafkaOptions{Brokers: []string{"b"}, Topic: "t", FetchMaxWait: time.Millisecond}, 0},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewKafka(test.options, test.start); err == nil {
				t.Fatal("NewKafka() error = nil")
			}
		})
	}
}
