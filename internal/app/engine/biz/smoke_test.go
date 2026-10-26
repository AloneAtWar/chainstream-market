package biz

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

const smokeTopic = "chainstream-eth"

// countingState reports how far the engine has committed, plus one.
type countingState struct {
	StateStore
	committed atomic.Int64
}

func (state *countingState) Commit(ctx context.Context, checkpoint chainsync.Checkpoint, delta StateDelta) error {
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
	// db is the ClickHouse database market-engine writes and, when raw is
	// set, ch-loader too.
	db     *gorm.DB
	output Output
	state  *countingState
	// raw, when set, makes run load the topic into db's raw tables
	// alongside market-engine, as ch-loader does.
	raw    bool
	offset int64
	// processor builds the engine's processor on every run.
	processor func(t *testing.T) *Processor
}

func newMarketSmoke(t *testing.T) *marketSmoke {
	t.Helper()
	ctx := context.Background()
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
	database := fmt.Sprintf("market_smoke_%d", time.Now().UnixNano())
	db, err := chdb.Connect(ctx, chdb.Options{
		Addr: strings.Split(addr, ","), Database: database,
		Username: os.Getenv("CLICKHOUSE_USER"), Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	})
	if err != nil {
		t.Fatalf("chdb.Connect() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := errors.Join(db.Exec("DROP DATABASE IF EXISTS "+database).Error, chdb.Close(db)); closeErr != nil {
			t.Errorf("drop database: %v", closeErr)
		}
	})
	if err = chdb.Migrate(ctx, db, chdb.MarketSchema); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	output, err := testStores.Output(ctx, db)
	if err != nil {
		t.Fatalf("output store error = %v", err)
	}
	state, err := pgdb.Open(ctx, pgdb.Options{DSN: dsn, Schema: fmt.Sprintf("market_smoke_%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatalf("pgdb.Open() error = %v", err)
	}
	t.Cleanup(func() { pgdb.Close(state) })
	return &marketSmoke{
		t: t, cluster: cluster, producer: producer, db: db, output: output,
		state: &countingState{StateStore: testStores.State(state)}, offset: -1,
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
	handler, err := NewHandler(HandlerOptions{ChainID: 1, Processor: processor, Output: s.output, State: s.state})
	if err != nil {
		s.t.Fatalf("NewHandler() error = %v", err)
	}
	chainEngine, err := chainsync.New(chainsync.Options{
		ChainID: 1, Chain: "eth", Handler: handler,
		Open: func(start int64) (source.Source, error) { return source.NewKafka(kafka, start) },
	})
	if err != nil {
		s.t.Fatalf("chainsync.New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(s.t.Context())
	if s.raw {
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

// load stands in for ch-loader over the topic until ctx ends, writing every
// block on its own commit.
func (s *marketSmoke) load(ctx context.Context, kafka source.KafkaOptions) <-chan error {
	s.t.Helper()
	loaderEngine, err := chainsync.New(chainsync.Options{
		ChainID: 1, Chain: "eth", Handler: &rawLoader{db: s.db},
		Open: func(start int64) (source.Source, error) { return source.NewKafka(kafka, start) },
	})
	if err != nil {
		s.t.Fatalf("chainsync.New() error = %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- loaderEngine.Run(ctx) }()
	return done
}

// rawLoader writes what ch-loader would: each block's raw facts, then its
// checkpoint. A revert deletes the facts above it.
type rawLoader struct {
	db      *gorm.DB
	pending []model.RawBlock
}

func (loader *rawLoader) Restore(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	return chdb.LoadRawCheckpoint(ctx, loader.db, chainID)
}

func (loader *rawLoader) Apply(_ context.Context, block *streamv1.BlockEvent) error {
	raw, err := model.NewRawBlock(block)
	if err != nil {
		return err
	}
	loader.pending = append(loader.pending, raw)
	return nil
}

func (loader *rawLoader) Revert(ctx context.Context, rollbackTo int64) error {
	return loader.deleteAbove(ctx, 1, rollbackTo)
}

func (loader *rawLoader) Commit(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	if len(loader.pending) > 0 {
		if err := chdb.WriteRaw(ctx, loader.db, loader.pending); err != nil {
			return err
		}
		loader.pending = loader.pending[:0]
	}
	return chdb.SaveRawCheckpoint(ctx, loader.db, checkpoint)
}

func (loader *rawLoader) deleteAbove(ctx context.Context, chainID uint64, height int64) error {
	for _, table := range chdb.RawTables {
		if err := loader.db.WithContext(ctx).Exec("DELETE FROM "+table+" WHERE chain_id = ? AND block_height > ?",
			chainID, height).Error; err != nil {
			return err
		}
	}
	return nil
}

// trades returns token's stored trades newest first.
func (s *marketSmoke) trades(token string) []chdb.TradeRow {
	s.t.Helper()
	var rows []chdb.TradeRow
	if err := s.db.Select("block_height, log_index, pool_id, price_usd, outlier").
		Where("chain_id = 1 AND base_token = ?", token).
		Order("block_height DESC, log_index DESC, operation_index DESC").Find(&rows).Error; err != nil {
		s.t.Fatalf("read trades: %v", err)
	}
	return rows
}

// memeTrades lists MEME trades newest first as height:log.
func (s *marketSmoke) memeTrades() string {
	s.t.Helper()
	trades := s.trades(meme)
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

// candleTrades sums the trades in the current version of token's 5m
// candles.
func (s *marketSmoke) candleTrades(token string) uint32 {
	s.t.Helper()
	var trades uint64
	if err := s.db.Raw(`SELECT sum(trades) FROM (
			SELECT argMax(trades, update_height) AS trades FROM candles
			WHERE chain_id = 1 AND subject = 'token' AND key = ? AND interval_seconds = 300
			GROUP BY open_time
		) SETTINGS prefer_column_name_to_alias = 1`, token).Row().Scan(&trades); err != nil {
		s.t.Fatalf("read candles: %v", err)
	}
	return uint32(trades)
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
			Catalog: asset.DefaultCatalog(1), MinLiquidityUSD: decimal.NewFromInt(1000),
			CandleMinLiquidityUSD: decimal.NewFromInt(10000), MaxHops: 3,
			Intervals: []time.Duration{5 * time.Minute}, HistoryDepth: 16,
			Feeds:            []Feed{{Address: ethFeed, Token: asset.NativeAddress, Decimals: 8}},
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
	trades := s.trades(weth)
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
	if err := chdb.Migrate(context.Background(), s.db, chdb.RawSchema); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	s.raw = true
	history := testStores.History(s.db, 10*time.Second)
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
