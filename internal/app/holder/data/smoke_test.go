package data

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const smokeTopic = "chainstream-eth"

// committedHandler reports how far the engine has committed, plus one.
type committedHandler struct {
	chainsync.Handler
	committed atomic.Int64
}

func (handler *committedHandler) Commit(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	if err := handler.Handler.Commit(ctx, checkpoint); err != nil {
		return err
	}
	handler.committed.Store(checkpoint.Offset + 1)
	return nil
}

func smokeBlock(height int64, fork string, touched []string, transfers ...*streamv1.TokenTransfer) *streamv1.StreamRecord {
	return &streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Block{Block: &streamv1.BlockEvent{
		ChainId: 1, Chain: "eth", BlockHeight: height, BlockHash: hashOf(height, fork),
		ParentHash: hashOf(height-1, ""), Transfers: transfers,
		NativeTransfers: []*streamv1.NativeTransfer{{From: alice, To: bob, RawAmount: "1"}},
		NativeTouched:   touched, FinalizedHeight: height - 20,
	}}}
}

// TestSmokeKafkaNativeAndTokenBalances runs holder-indexer's handler over a
// kfake topic into a real PostgreSQL: native and ERC-20 balances, then a
// reorg that replaces a block.
func TestSmokeKafkaNativeAndTokenBalances(t *testing.T) {
	f := newFixture(t, 0)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, smokeTopic))
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer cluster.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("producer error = %v", err)
	}
	defer producer.Close()

	f.node.SetBalance(hashOf(100, ""), multicall, alice, 1000)
	f.node.SetBalance(hashOf(100, ""), token, alice, 50)
	f.node.SetBalance(hashOf(101, "a"), multicall, alice, 700)
	f.node.SetBalance(hashOf(101, "a"), multicall, bob, 290)
	f.node.SetBalance(hashOf(101, "b"), multicall, alice, 900)
	f.node.SetBalance(hashOf(101, "b"), multicall, carol, 90)
	f.node.SetBalance(hashOf(101, "b"), token, carol, 5)
	records := []*streamv1.StreamRecord{
		smokeBlock(100, "", []string{alice}, transfer(token, zeroAddress, alice, 50)),
		smokeBlock(101, "a", []string{alice, bob}, transfer(token, alice, bob, 20)),
		{Payload: &streamv1.StreamRecord_Revert{Revert: &streamv1.BlockRevert{ChainId: 1, RollbackTo: 100}}},
		smokeBlock(101, "b", []string{alice, carol}, transfer(token, alice, carol, 5)),
	}
	for _, record := range records {
		value, marshalErr := proto.Marshal(record)
		if marshalErr != nil {
			t.Fatalf("Marshal() error = %v", marshalErr)
		}
		if produceErr := producer.ProduceSync(t.Context(), &kgo.Record{Topic: smokeTopic, Partition: 0, Value: value}).FirstErr(); produceErr != nil {
			t.Fatalf("ProduceSync() error = %v", produceErr)
		}
	}

	kafka := source.KafkaOptions{
		Brokers: cluster.ListenAddrs(), Topic: smokeTopic, FetchMaxBytes: 1 << 20, FetchMaxWait: 50 * time.Millisecond,
	}
	handler := &committedHandler{Handler: f.indexer}
	engine, err := chainsync.New(chainsync.Options{
		ChainID: 1, Chain: "eth", Handler: handler,
		Open: func(start int64) (source.Source, error) { return source.NewKafka(kafka, start) },
	})
	if err != nil {
		t.Fatalf("chainsync.New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	deadline := time.Now().Add(20 * time.Second)
	for handler.committed.Load() < int64(len(records)) {
		select {
		case runErr := <-done:
			cancel()
			t.Fatalf("Run() stopped early: %v", runErr)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("committed %d of %d records", handler.committed.Load(), len(records))
		}
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatalf("Run() error = %v", runErr)
	}

	assertBalances(t, f.balances(native), map[string]int64{alice: 900, carol: 90})
	assertBalances(t, f.balances(token), map[string]int64{alice: 45, carol: 5})
}
