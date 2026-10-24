package chloader

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// fakeStore records every call in order.
type fakeStore struct {
	checkpoint *storage.Checkpoint
	calls      []string
	// failWrite fails the next WriteRaw after recording it.
	failWrite bool
}

func (fake *fakeStore) LoadRawCheckpoint(context.Context, uint64) (storage.Checkpoint, bool, error) {
	if fake.checkpoint == nil {
		return storage.Checkpoint{}, false, nil
	}
	return *fake.checkpoint, true, nil
}

func (fake *fakeStore) WriteRaw(_ context.Context, blocks []storage.RawBlock) error {
	heights := make([]string, len(blocks))
	for index, block := range blocks {
		heights[index] = fmt.Sprint(block.Height)
	}
	fake.calls = append(fake.calls, "write "+strings.Join(heights, ","))
	if fake.failWrite {
		fake.failWrite = false
		return errors.New("connection reset")
	}
	return nil
}

func (fake *fakeStore) DeleteRawAbove(_ context.Context, _ uint64, height int64) error {
	fake.calls = append(fake.calls, fmt.Sprintf("delete >%d", height))
	return nil
}

func (fake *fakeStore) SaveRawCheckpoint(_ context.Context, checkpoint storage.Checkpoint) error {
	fake.calls = append(fake.calls, fmt.Sprintf("checkpoint %d", checkpoint.Offset))
	return nil
}

func (fake *fakeStore) take() string {
	calls := strings.Join(fake.calls, "; ")
	fake.calls = nil
	return calls
}

var now = time.Unix(1_800_000_000, 0)

// testBlock is a block age old with one transfer.
func testBlock(height int64, age time.Duration) *streamv1.BlockEvent {
	return &streamv1.BlockEvent{
		ChainId: 1, BlockHeight: height, BlockTime: now.Add(-age).Unix(),
		Transfers: []*streamv1.TokenTransfer{{Token: "0xT", From: "0xA", To: "0xB", RawAmount: "1", LogIndex: 1}},
	}
}

func newLoader(t *testing.T, store *fakeStore, options Options) *Loader {
	t.Helper()
	options.ChainID, options.Store = 1, store
	options.Now = func() time.Time { return now }
	loader, err := New(options)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, _, err := loader.Restore(context.Background(), 1); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	return loader
}

func step(t *testing.T, loader *Loader, offset int64, block *streamv1.BlockEvent) {
	t.Helper()
	ctx := context.Background()
	if err := loader.Apply(ctx, block); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	checkpoint := storage.Checkpoint{ChainID: 1, Offset: offset, Height: block.GetBlockHeight()}
	if err := loader.Commit(ctx, checkpoint); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func TestLoaderWritesHeadBlocksOneByOne(t *testing.T) {
	store := &fakeStore{checkpoint: &storage.Checkpoint{ChainID: 1, Offset: 9, Height: 99}}
	loader := newLoader(t, store, Options{BatchLag: time.Minute, FlushRows: 100, FlushInterval: time.Hour})
	step(t, loader, 10, testBlock(100, time.Second))
	step(t, loader, 11, testBlock(101, time.Second))
	// The first block clears what an interrupted write may have left.
	if got := store.take(); got != "delete >99; write 100; checkpoint 10; write 101; checkpoint 11" {
		t.Fatalf("calls = %s", got)
	}
}

func TestLoaderBatchesOldBlocks(t *testing.T) {
	store := &fakeStore{}
	loader := newLoader(t, store, Options{BatchLag: time.Minute, FlushRows: 3, FlushInterval: time.Hour})
	for index := range int64(4) {
		step(t, loader, index, testBlock(100+index, time.Hour))
	}
	if got := store.take(); got != "delete >99; write 100,101,102; checkpoint 2" {
		t.Fatalf("calls = %s", got)
	}
	// Reaching the head flushes the rest at once.
	step(t, loader, 4, testBlock(104, time.Second))
	if got := store.take(); got != "write 103,104; checkpoint 4" {
		t.Fatalf("calls = %s", got)
	}
}

func TestLoaderFlushesBatchesByInterval(t *testing.T) {
	store := &fakeStore{}
	loader := newLoader(t, store, Options{BatchLag: time.Minute, FlushInterval: time.Second})
	clock := now
	loader.now = func() time.Time { return clock }
	step(t, loader, 0, testBlock(100, time.Hour))
	step(t, loader, 1, testBlock(101, time.Hour))
	clock = clock.Add(time.Second)
	step(t, loader, 2, testBlock(102, time.Hour))
	if got := store.take(); got != "delete >99; write 100,101,102; checkpoint 2" {
		t.Fatalf("calls = %s", got)
	}
}

func TestLoaderRevertsBufferedAndStoredBlocks(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	loader := newLoader(t, store, Options{BatchLag: time.Minute, FlushRows: 3, FlushInterval: time.Hour})
	for index := range int64(5) {
		step(t, loader, index, testBlock(100+index, time.Hour))
	}
	store.take() // 100..102 stored, 103 and 104 buffered

	// Above everything stored: only the buffer shrinks.
	if err := loader.Revert(ctx, 103); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	if got := store.take(); got != "" {
		t.Fatalf("revert above stored calls = %s", got)
	}
	if len(loader.pending) != 1 || loader.rows != 1 {
		t.Fatalf("pending = %d blocks, %d rows", len(loader.pending), loader.rows)
	}
	// Below stored facts: they are deleted, and the buffer is emptied.
	if err := loader.Revert(ctx, 101); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	if err := loader.Commit(ctx, storage.Checkpoint{ChainID: 1, Offset: 5, Height: 101}); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	step(t, loader, 6, testBlock(102, time.Second))
	if got := store.take(); got != "delete >101; write 102; checkpoint 6" {
		t.Fatalf("calls = %s", got)
	}
}

func TestLoaderRevertAfterRestartAlwaysDeletes(t *testing.T) {
	store := &fakeStore{checkpoint: &storage.Checkpoint{ChainID: 1, Offset: 9, Height: 99}}
	loader := newLoader(t, store, Options{})
	if err := loader.Revert(context.Background(), 120); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	step(t, loader, 10, testBlock(121, time.Hour))
	if got := store.take(); got != "delete >120; write 121; checkpoint 10" {
		t.Fatalf("calls = %s", got)
	}
}

func TestLoaderRetriesAFailedFlushWithoutDuplicates(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	loader := newLoader(t, store, Options{BatchLag: time.Minute, FlushRows: 2, FlushInterval: time.Hour})
	step(t, loader, 0, testBlock(100, time.Hour))
	if err := loader.Apply(ctx, testBlock(101, time.Hour)); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	store.failWrite = true
	if err := loader.Commit(ctx, storage.Checkpoint{ChainID: 1, Offset: 1, Height: 101}); err == nil {
		t.Fatal("Commit() error = nil")
	}
	// The write may have reached part of the tables, so the retry clears it.
	if err := loader.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if got := store.take(); got != "delete >99; write 100,101; delete >99; write 100,101; checkpoint 1" {
		t.Fatalf("calls = %s", got)
	}
}

func TestConvertKeepsPublishedFacts(t *testing.T) {
	bin := uint32(42)
	block := &streamv1.BlockEvent{
		ChainId: 56, BlockHeight: 7, BlockTime: 1_700_000_000,
		Transfers: []*streamv1.TokenTransfer{{Token: "0xT", From: "0xA", To: "0xB", RawAmount: "0", LogIndex: 3}},
		Trades: []*streamv1.Trade{{
			PoolId: "0xP", Action: streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_ADDED, Trader: "0xA",
			Amounts:   []*streamv1.Amount{{Asset: "0xT", Raw: "-10"}},
			PoolState: &streamv1.PoolState{ReservesAfter: []string{"1", "2"}, ActiveBinId: &bin},
		}},
		OracleUpdates: []*streamv1.OracleUpdate{{Feed: "0xF", Answer: "-3", RoundId: "9"}},
		Assets: []*streamv1.AssetFact{{
			Address: "0xT", Type: streamv1.AssetFactType_ASSET_FACT_TYPE_CREATED,
			Deployment: &streamv1.DeploymentRef{BlockHeight: 6, TransactionHash: "0xd"}, MetadataJson: []byte(`{"decimals":18}`),
		}},
		Pools: []*streamv1.PoolFact{{PoolId: "0xP", TokenAddresses: []string{"0xT", "0xW"}, Created: true}},
	}
	raw, err := convert(block)
	if err != nil {
		t.Fatalf("convert() error = %v", err)
	}
	// Zero-value transfers are kept: every transfer is stored.
	if raw.Rows() != 5 || raw.Transfers[0].Token != "0xt" || raw.Transfers[0].Amount.Sign() != 0 {
		t.Fatalf("raw = %+v", raw)
	}
	trade := raw.Trades[0]
	if trade.Action != "liquidity_added" || trade.PoolID != "0xp" || trade.Amounts[0].Raw != "-10" ||
		!slices.Equal(trade.ReservesAfter, []string{"1", "2"}) || *trade.ActiveBinID != 42 {
		t.Fatalf("trade = %+v", trade)
	}
	if raw.Oracles[0].Answer.Int64() != -3 || raw.Assets[0].Type != "created" || raw.Assets[0].DeploymentHeight != 6 ||
		raw.Assets[0].MetadataJSON != `{"decimals":18}` || raw.Pools[0].TokenAddresses[1] != "0xw" {
		t.Fatalf("facts = %+v %+v %+v", raw.Oracles, raw.Assets, raw.Pools)
	}

	block.Transfers[0].RawAmount = "-1"
	if _, err := convert(block); err == nil {
		t.Fatal("convert() with a negative transfer error = nil")
	}
}
