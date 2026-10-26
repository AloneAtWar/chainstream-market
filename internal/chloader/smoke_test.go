package chloader

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chraw"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const (
	smokeTopic = "chainstream-eth"
	smokeToken = "0x1111111111111111111111111111111111111111"
	smokeAlice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

type smoke struct {
	t        *testing.T
	cluster  *kfake.Cluster
	producer *kgo.Client
	options  chstore.Options
	store    *chraw.Store
	offset   int64
}

func newSmoke(t *testing.T) *smoke {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set")
	}
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, smokeTopic))
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("producer error = %v", err)
	}
	t.Cleanup(producer.Close)
	options := chstore.Options{
		Addr:     strings.Split(addr, ","),
		Database: fmt.Sprintf("loader_test_%d", time.Now().UnixNano()),
		Username: os.Getenv("CLICKHOUSE_USER"), Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
	store, err := chraw.Open(context.Background(), options)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		db, connErr := chstore.Connect(context.Background(), options)
		if connErr == nil {
			connErr = errors.Join(db.Exec("DROP DATABASE IF EXISTS "+options.Database).Error, chstore.Close(db))
		}
		if connErr != nil {
			t.Errorf("drop database: %v", connErr)
		}
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return &smoke{t: t, cluster: cluster, producer: producer, options: options, store: store, offset: -1}
}

func (s *smoke) produce(records ...*streamv1.StreamRecord) {
	s.t.Helper()
	for _, record := range records {
		value, err := proto.Marshal(record)
		if err != nil {
			s.t.Fatalf("Marshal() error = %v", err)
		}
		if err := s.producer.ProduceSync(s.t.Context(), &kgo.Record{Topic: smokeTopic, Partition: 0, Value: value}).FirstErr(); err != nil {
			s.t.Fatalf("ProduceSync() error = %v", err)
		}
		s.offset++
	}
}

// run consumes until the checkpoint reaches the last produced record, then
// stops the engine the way ch-loader does.
func (s *smoke) run(options Options) {
	s.t.Helper()
	options.ChainID, options.Store = 1, s.store
	loader, err := New(options)
	if err != nil {
		s.t.Fatalf("New() error = %v", err)
	}
	kafka := source.KafkaOptions{
		Brokers: s.cluster.ListenAddrs(), Topic: smokeTopic, FetchMaxBytes: 1 << 20, FetchMaxWait: 50 * time.Millisecond,
	}
	handler := &observed{Loader: loader}
	chainEngine, err := chainsync.New(chainsync.Options{
		ChainID: 1, Chain: "eth", Handler: handler,
		Open: func(start int64) (source.Source, error) { return source.NewKafka(kafka, start) },
	})
	if err != nil {
		s.t.Fatalf("chainsync.New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(s.t.Context())
	done := make(chan error, 1)
	go func() { done <- chainEngine.Run(ctx) }()
	deadline := time.Now().Add(20 * time.Second)
	for handler.committed.Load() < s.offset+1 {
		select {
		case runErr := <-done:
			cancel()
			s.t.Fatalf("Run() stopped early: %v", runErr)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			cancel()
			s.t.Fatalf("committed through offset %d, want %d", handler.committed.Load()-1, s.offset)
		}
	}
	cancel()
	if runErr := <-done; runErr != nil {
		s.t.Fatalf("Run() error = %v", runErr)
	}
	if err := loader.Flush(context.Background()); err != nil {
		s.t.Fatalf("Flush() error = %v", err)
	}
}

// observed reports how far the engine has committed, plus one.
type observed struct {
	*Loader
	committed atomic.Int64
}

func (handler *observed) Commit(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	if err := handler.Loader.Commit(ctx, checkpoint); err != nil {
		return err
	}
	handler.committed.Store(checkpoint.Offset + 1)
	return nil
}

func (s *smoke) transfers() string {
	s.t.Helper()
	transfers, err := s.store.Transfers(context.Background(), storage.TransferQuery{ChainID: 1, Token: smokeToken, Limit: 100})
	if err != nil {
		s.t.Fatalf("Transfers() error = %v", err)
	}
	keys := make([]string, len(transfers))
	for index, transfer := range transfers {
		keys[index] = fmt.Sprintf("%d:%s", transfer.BlockHeight, transfer.Amount)
	}
	return strings.Join(keys, " ")
}

func smokeBlock(height int64, hash, parent string, amount int, blockTime time.Time) *streamv1.StreamRecord {
	return &streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Block{Block: &streamv1.BlockEvent{
		ChainId: 1, Chain: "eth", BlockHeight: height, BlockHash: hash, ParentHash: parent, BlockTime: blockTime.Unix(),
		Transfers: []*streamv1.TokenTransfer{{
			Token: smokeToken, From: smokeAlice, To: "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			RawAmount: fmt.Sprint(amount), TransactionHash: fmt.Sprintf("0x%x", height), LogIndex: 1,
		}},
		Trades:        []*streamv1.Trade{{PoolId: "0xpool", Action: streamv1.TradeAction_TRADE_ACTION_SWAP}},
		OracleUpdates: []*streamv1.OracleUpdate{{Feed: "0xfeed", Answer: "1", RoundId: "1"}},
	}}}
}

func smokeRevert(rollbackTo int64) *streamv1.StreamRecord {
	return &streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Revert{Revert: &streamv1.BlockRevert{ChainId: 1, RollbackTo: rollbackTo}}}
}

// TestSmokeKafkaToClickHouse runs the engine over a kfake topic into a real
// ClickHouse: a reorg, a restart that replays a half-written block, and a
// batched catch-up.
func TestSmokeKafkaToClickHouse(t *testing.T) {
	s := newSmoke(t)
	head := time.Now()
	s.produce(
		smokeBlock(100, "0xa100", "0xa099", 1, head), smokeBlock(101, "0xa101", "0xa100", 2, head),
		smokeBlock(102, "0xa102", "0xa101", 3, head), smokeRevert(100),
		smokeBlock(101, "0xb101", "0xa100", 20, head), smokeBlock(102, "0xb102", "0xb101", 30, head),
	)
	s.run(Options{})
	if got := s.transfers(); got != "102:30 101:20 100:1" {
		t.Fatalf("after reorg transfers = %s", got)
	}

	// A crash after writing block 103's facts but before its checkpoint.
	partial := smokeBlock(103, "0xb103", "0xb102", 40, head).GetBlock()
	raw, err := convert(partial)
	if err != nil {
		t.Fatalf("convert() error = %v", err)
	}
	if err = s.store.WriteRaw(context.Background(), []storage.RawBlock{raw}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	s.produce(&streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Block{Block: partial}})
	s.run(Options{})
	if got := s.transfers(); got != "103:40 102:30 101:20 100:1" {
		t.Fatalf("after replay transfers = %s", got)
	}

	// Old blocks are batched; stopping flushes the remainder.
	old := head.Add(-time.Hour)
	parent := "0xb103"
	for height := int64(104); height < 110; height++ {
		hash := fmt.Sprintf("0xb%d", height)
		s.produce(smokeBlock(height, hash, parent, int(height), old))
		parent = hash
	}
	s.run(Options{BatchLag: time.Minute, FlushRows: 7, FlushInterval: time.Hour})
	if got := s.transfers(); !strings.HasPrefix(got, "109:109 108:108 107:107 106:106 105:105 104:104 103:40") {
		t.Fatalf("after catch-up transfers = %s", got)
	}
	checkpoint, ok, err := s.store.LoadRawCheckpoint(context.Background(), 1)
	if err != nil || !ok || checkpoint.Height != 109 || checkpoint.Offset != s.offset || checkpoint.Hash != "0xb109" {
		t.Fatalf("checkpoint = %+v, %v, %v", checkpoint, ok, err)
	}
}
