package market

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/chloader"
	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chraw"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const smokeTopic = "chainstream-eth"

// countingState reports how far the engine has committed, plus one.
type countingState struct {
	storage.StateStore
	committed atomic.Int64
}

func (state *countingState) Commit(ctx context.Context, checkpoint storage.Checkpoint, delta storage.StateDelta) error {
	if err := state.StateStore.Commit(ctx, checkpoint, delta); err != nil {
		return err
	}
	state.committed.Store(checkpoint.Offset + 1)
	return nil
}

type marketSmoke struct {
	t        *testing.T
	cluster  *kfake.Cluster
	producer *kgo.Client
	options  chstore.Options
	output   *chstore.Store
	state    *countingState
	// raw, when set, is ch-loader's store: run then loads the topic into it
	// alongside market-engine.
	raw    *chraw.Store
	offset int64
	// processor builds the engine's processor on every run.
	processor func(t *testing.T) *Processor
}

func newMarketSmoke(t *testing.T) *marketSmoke {
	t.Helper()
	addr, dsn := os.Getenv("CLICKHOUSE_ADDR"), os.Getenv("POSTGRES_DSN")
	if addr == "" || dsn == "" {
		t.Skip("CLICKHOUSE_ADDR or POSTGRES_DSN not set")
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
		Database: fmt.Sprintf("market_smoke_%d", time.Now().UnixNano()),
		Username: os.Getenv("CLICKHOUSE_USER"), Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
	output, err := chstore.Open(context.Background(), options)
	if err != nil {
		t.Fatalf("chstore.Open() error = %v", err)
	}
	t.Cleanup(func() {
		conn, connErr := chstore.Connect(context.Background(), options)
		if connErr == nil {
			connErr = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+options.Database)
		}
		if connErr != nil {
			t.Errorf("drop database: %v", connErr)
		}
		if closeErr := output.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})
	state, err := pgstore.Open(context.Background(), pgstore.Options{
		DSN: dsn, Schema: fmt.Sprintf("market_smoke_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("pgstore.Open() error = %v", err)
	}
	t.Cleanup(state.Close)
	return &marketSmoke{
		t: t, cluster: cluster, producer: producer, options: options, output: output,
		state: &countingState{StateStore: state}, offset: -1,
		processor: func(t *testing.T) *Processor { return newLateProcessor(t, 100, 100) },
	}
}

func (s *marketSmoke) produce(records ...*streamv1.StreamRecord) {
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

// run starts market-engine from its persisted state and stops it once the
// last produced record is committed.
func (s *marketSmoke) run() {
	s.t.Helper()
	processor := s.processor(s.t)
	kafka := source.KafkaOptions{
		Brokers: s.cluster.ListenAddrs(), Topic: smokeTopic, FetchMaxBytes: 1 << 20, FetchMaxWait: 50 * time.Millisecond,
	}
	chainEngine, err := engine.New(engine.Options{
		ChainID: 1, Chain: "eth", Processor: processor, Output: s.output, State: s.state,
		Open: func(start int64) (source.Source, error) { return source.NewKafka(kafka, start) },
	})
	if err != nil {
		s.t.Fatalf("engine.New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(s.t.Context())
	if s.raw != nil {
		loaded := s.load(ctx, kafka)
		defer func() {
			if loadErr := <-loaded; loadErr != nil {
				s.t.Fatalf("ch-loader Run() error = %v", loadErr)
			}
		}()
	}
	done := make(chan error, 1)
	go func() { done <- chainEngine.Run(ctx) }()
	deadline := time.Now().Add(20 * time.Second)
	for s.state.committed.Load() < s.offset+1 {
		select {
		case runErr := <-done:
			cancel()
			s.t.Fatalf("Run() stopped early: %v", runErr)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			cancel()
			s.t.Fatalf("committed through offset %d, want %d", s.state.committed.Load()-1, s.offset)
		}
	}
	cancel()
	if runErr := <-done; runErr != nil {
		s.t.Fatalf("Run() error = %v", runErr)
	}
}

// load runs ch-loader over the topic until ctx ends, writing every block on
// its own commit.
func (s *marketSmoke) load(ctx context.Context, kafka source.KafkaOptions) <-chan error {
	s.t.Helper()
	loader, err := chloader.New(chloader.Options{ChainID: 1, Store: s.raw})
	if err != nil {
		s.t.Fatalf("chloader.New() error = %v", err)
	}
	loaderEngine, err := engine.New(engine.Options{
		ChainID: 1, Chain: "eth", Handler: loader,
		Open: func(start int64) (source.Source, error) { return source.NewKafka(kafka, start) },
	})
	if err != nil {
		s.t.Fatalf("engine.New() error = %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- loaderEngine.Run(ctx) }()
	return done
}

// memeTrades lists MEME trades newest first as height:log.
func (s *marketSmoke) memeTrades() string {
	s.t.Helper()
	trades, err := s.output.Trades(context.Background(), storage.TradeQuery{ChainID: 1, BaseToken: meme, Limit: 100})
	if err != nil {
		s.t.Fatalf("Trades() error = %v", err)
	}
	keys := make([]string, len(trades))
	for index, trade := range trades {
		keys[index] = fmt.Sprintf("%d:%d:%s", trade.BlockHeight, trade.LogIndex, trade.PriceUSD)
	}
	return strings.Join(keys, " ")
}

func (s *marketSmoke) memeCandleTrades() uint32 {
	s.t.Helper()
	return s.candleTrades(meme)
}

// candleTrades sums the trades in token's 5m candles.
func (s *marketSmoke) candleTrades(token string) uint32 {
	s.t.Helper()
	candles, err := s.output.Candles(context.Background(), storage.CandleQuery{
		ChainID: 1, Subject: storage.CandleSubjectToken, Key: token, Interval: 5 * time.Minute,
		From: time.Unix(0, 0), To: time.Unix(startTime+3600, 0), Limit: 100,
	})
	if err != nil {
		s.t.Fatalf("Candles() error = %v", err)
	}
	var trades uint32
	for _, candle := range candles {
		trades += candle.Trades
	}
	return trades
}

func smokeRecord(height int64, hash, parent string, assets bool, trades ...*streamv1.Trade) *streamv1.StreamRecord {
	block := testBlock(height, (height-100)*12, trades...)
	block.BlockHash, block.ParentHash = hash, parent
	if assets {
		block.Assets = []*streamv1.AssetFact{assetFact(meme, recognized, 18)}
	}
	return &streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Block{Block: block}}
}

// TestSmokeLateTradesThroughKafka runs market-engine over a kfake topic into
// real ClickHouse and PostgreSQL: MEME trades arrive before MEME's decimals,
// are replayed when its metadata lands, the replay is reorged away and
// replayed again on the fork, and a restart resumes from persisted state.
func TestSmokeLateTradesThroughKafka(t *testing.T) {
	s := newMarketSmoke(t)
	s.produce(
		smokeRecord(100, "0xa100", "0xa099", false, wethSwap(), undecided(memeSell(), meme, 1)),
		smokeRecord(101, "0xa101", "0xa100", false, undecided(memeSell(), meme, 2)),
	)
	s.run()
	if got := s.memeTrades(); got != "" {
		t.Fatalf("held trades stored: %s", got)
	}

	s.produce(smokeRecord(102, "0xa102", "0xa101", true))
	s.run()
	if got := s.memeTrades(); got != "101:2:0.03 100:1:0.03" {
		t.Fatalf("after metadata trades = %s", got)
	}
	if got := s.memeCandleTrades(); got != 2 {
		t.Fatalf("after metadata candle trades = %d, want 2", got)
	}

	// The block that brought the metadata is reorged out; the fork brings it
	// one block later.
	s.produce(
		&streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Revert{Revert: &streamv1.BlockRevert{ChainId: 1, RollbackTo: 101}}},
		smokeRecord(102, "0xb102", "0xa101", false),
	)
	s.run()
	if got := s.memeTrades(); got != "" {
		t.Fatalf("after reorg trades = %s", got)
	}
	if got := s.memeCandleTrades(); got != 0 {
		t.Fatalf("after reorg candle trades = %d, want 0", got)
	}
	s.produce(
		smokeRecord(103, "0xb103", "0xb102", true),
		smokeRecord(104, "0xb104", "0xb103", false, memeSell()),
	)
	s.run()
	if got := s.memeTrades(); got != "104:0:0.03 101:2:0.03 100:1:0.03" {
		t.Fatalf("after fork trades = %s", got)
	}
	if got := s.memeCandleTrades(); got != 3 {
		t.Fatalf("after fork candle trades = %d, want 3", got)
	}
}

// TestSmokeOutliersThroughKafka runs market-engine over a kfake topic into
// real ClickHouse and PostgreSQL: a thin pool's swap at twice WETH's price
// is stored marked outlier and kept out of WETH's candles, then a reorg to a
// fork where WETH's price moved there unmarks it, across restarts.
func TestSmokeOutliersThroughKafka(t *testing.T) {
	s := newMarketSmoke(t)
	s.processor = func(t *testing.T) *Processor {
		t.Helper()
		processor, err := NewProcessor(Options{
			Catalog: DefaultCatalog(1), MinLiquidityUSD: decimal.NewFromInt(1000),
			CandleMinLiquidityUSD: decimal.NewFromInt(10000), MaxHops: 3,
			Intervals: []time.Duration{5 * time.Minute}, HistoryDepth: 16,
			Feeds:            []Feed{{Address: ethFeed, Token: NativeAddress, Decimals: 8}},
			OracleDeviation:  decimal.RequireFromString("0.02"),
			OutlierDeviation: decimal.RequireFromString("0.5"),
		})
		if err != nil {
			t.Fatalf("NewProcessor() error = %v", err)
		}
		return processor
	}
	first := smokeRecord(100, "0xa100", "0xa099", false, wethSwap(), thinSwap(6000))
	first.GetBlock().OracleUpdates = []*streamv1.OracleUpdate{oracleUpdate(ethFeed, "300000000000")}
	s.produce(first)
	s.run()
	if got := s.wethTrades(); got != "0xpool-weth-usdc-thin:6000:true 0xpool-weth-usdc:3000:false" {
		t.Fatalf("trades = %s", got)
	}
	if got := s.candleTrades(weth); got != 1 {
		t.Fatalf("WETH candle trades = %d, want 1", got)
	}

	moved := swap(wethUSDC,
		leg{weth, "-1000000000000000000", "1000000000000000000000", 18},
		leg{usdc, "6000000000", "6000000000000", 6},
	)
	s.produce(
		&streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Revert{Revert: &streamv1.BlockRevert{ChainId: 1, RollbackTo: 99}}},
		smokeRecord(100, "0xb100", "0xa099", false, moved, thinSwap(6000)),
	)
	s.run()
	if got := s.wethTrades(); got != "0xpool-weth-usdc-thin:6000:false 0xpool-weth-usdc:6000:false" {
		t.Fatalf("after reorg trades = %s", got)
	}
	if got := s.candleTrades(weth); got != 2 {
		t.Fatalf("after reorg WETH candle trades = %d, want 2", got)
	}
}

// wethTrades lists WETH trades newest first as pool:price:outlier.
func (s *marketSmoke) wethTrades() string {
	s.t.Helper()
	trades, err := s.output.Trades(context.Background(), storage.TradeQuery{ChainID: 1, BaseToken: weth, Limit: 100})
	if err != nil {
		s.t.Fatalf("Trades() error = %v", err)
	}
	keys := make([]string, len(trades))
	for index, trade := range trades {
		keys[index] = fmt.Sprintf("%s:%s:%t", trade.PoolID, trade.PriceUSD, trade.Outlier)
	}
	return strings.Join(keys, " ")
}

// TestSmokeBackfillThroughKafka runs ch-loader and market-engine over one
// kfake topic into real ClickHouse and PostgreSQL. market-engine holds
// nothing: when MEME's metadata lands, it reads MEME's earlier trades back
// from ch-loader's raw_trades and recomputes them with MEME's prices and
// candles. A reorg of that block removes them, and the fork recomputes
// them again.
func TestSmokeBackfillThroughKafka(t *testing.T) {
	s := newMarketSmoke(t)
	raw, err := chraw.Open(context.Background(), s.options)
	if err != nil {
		t.Fatalf("chraw.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := raw.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})
	s.raw = raw
	history := chraw.NewHistory(raw, 10*time.Second)
	s.processor = func(t *testing.T) *Processor { return newBackfillProcessor(t, history, 0, 0) }
	memePrices := func() string {
		t.Helper()
		prices, err := history.PriceHistory(context.Background(), 1, []string{meme}, 0, 1000)
		if err != nil {
			t.Fatalf("PriceHistory() error = %v", err)
		}
		heights := make([]string, len(prices[meme]))
		for index, point := range prices[meme] {
			heights[index] = fmt.Sprintf("%d:%s", point.Height, point.USD)
		}
		return strings.Join(heights, " ")
	}

	s.produce(
		smokeRecord(100, "0xa100", "0xa099", false, wethSwap(), undecided(memeSell(), meme, 1)),
		smokeRecord(101, "0xa101", "0xa100", false, undecided(memeSell(), meme, 2)),
		smokeRecord(102, "0xa102", "0xa101", true),
	)
	s.run()
	if got := s.memeTrades(); got != "101:2:0.03 100:1:0.03" {
		t.Fatalf("after metadata trades = %s", got)
	}
	if got := s.memeCandleTrades(); got != 2 {
		t.Fatalf("after metadata candle trades = %d, want 2", got)
	}
	if got := memePrices(); got != "100:0.03 101:0.03 102:0.03" {
		t.Fatalf("after metadata MEME prices = %s", got)
	}

	s.produce(
		&streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Revert{Revert: &streamv1.BlockRevert{ChainId: 1, RollbackTo: 101}}},
		smokeRecord(102, "0xb102", "0xa101", false),
	)
	s.run()
	if got, prices := s.memeTrades(), memePrices(); got != "" || prices != "" {
		t.Fatalf("after reorg trades = %q, prices = %q", got, prices)
	}
	s.produce(smokeRecord(103, "0xb103", "0xb102", true))
	s.run()
	if got := s.memeTrades(); got != "101:2:0.03 100:1:0.03" {
		t.Fatalf("after fork trades = %s", got)
	}
	if got := memePrices(); got != "100:0.03 101:0.03 103:0.03" {
		t.Fatalf("after fork MEME prices = %s", got)
	}
}
