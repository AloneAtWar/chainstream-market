package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// stateStores returns the memory store and, when POSTGRES_DSN is set, a
// PostgreSQL store in a fresh schema.
func stateStores(t *testing.T) map[string]func(t *testing.T) StateStore {
	t.Helper()
	stores := map[string]func(t *testing.T) StateStore{
		"memory": func(*testing.T) StateStore { return NewMemory() },
	}
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return stores
	}
	stores["postgres"] = func(t *testing.T) StateStore {
		t.Helper()
		db, err := pgdb.Open(context.Background(), pgdb.Options{
			DSN: dsn, Schema: fmt.Sprintf("market_state_test_%d", time.Now().UnixNano()),
		})
		if err != nil {
			t.Fatalf("pgdb.Open() error = %v", err)
		}
		t.Cleanup(func() { pgdb.Close(db) })
		return testStores.State(db)
	}
	return stores
}

// chainBlock moves the WETH and MEME pools a little every block. variant
// selects a different fork.
func chainBlock(height, variant int64) *streamv1.BlockEvent {
	step := height - 100 + variant*1000
	wethTrade := swap(wethUSDC,
		leg{weth, "-1000000000000000000", "1000000000000000000000", 18},
		leg{usdc, "3000000000", fmt.Sprintf("%d000000", 3_000_000+step*1000), 6},
	)
	memeTrade := swap(memeWETH,
		leg{meme, "100000000000000000000", fmt.Sprintf("%d000000000000000000", 1_000_000-step*100), 18},
		leg{weth, "-1000000000000000", "10000000000000000000", 18},
	)
	clTrades := []*streamv1.Trade{
		clMint(clPool, -269460, -269340, fmt.Sprintf("%d000000000000000", step+1)),
		clSwap(clPool, clSqrtAt(-269400+int(step%30)), "", ""),
	}
	block := testBlock(height, (height-100)*12, append([]*streamv1.Trade{wethTrade, memeTrade}, clTrades...)...)
	// The pool is seeded at the first block and read again every fifth.
	if height == 100 || height%5 == 0 {
		block.TickSnapshots = []*streamv1.PoolTickSnapshot{clSnapshot(height-1, clSqrtAt(-269400), 3000, map[int32]string{
			-270000: "1000000000000000000", -268800: "-1000000000000000000",
			-269520: fmt.Sprintf("%d", 1_000_000+step), -269280: fmt.Sprintf("-%d", 1_000_000+step),
		})}
	}
	// A weighted pool prices MEME against WETH from its balances, and every
	// third block also carries a ladder for a crypto pool.
	block.PoolBalances = []*streamv1.PoolBalanceSnapshot{{
		PoolId: weightedPool, Protocol: "balancer_v2", SnapshotHeight: height, Tokens: []string{meme, weth},
		Balances: []string{fmt.Sprintf("%d000000000000000000", 4_000_000+step), "100000000000000000000"},
		Curve:    model.CurveWeighted, Fee: "3000000000000000", Weights: []string{"800000000000000000", "200000000000000000"},
	}, {
		PoolId: cryptoPool, Protocol: "curve_twocrypto_ng", SnapshotHeight: height, Tokens: []string{meme, weth},
		Balances: []string{"1000000000000000000000", fmt.Sprintf("%d0000000000000000", 100+step)}, Curve: model.CurveCrypto,
	}}
	if height%3 == 0 {
		block.PoolBalances[1].Ladders = []*streamv1.QuoteLadder{{
			TokenIn: 0, TokenOut: 1, Amounts: []string{"1000000000000000000", "2000000000000000000"},
			Quotes: []string{fmt.Sprintf("%d", 1_000_000+step), fmt.Sprintf("%d", 1_990_000+step)},
		}}
	}
	block.BlockHash = fmt.Sprintf("0x%d-%d", height, variant)
	block.FinalizedHeight = height - 4
	return block
}

// harness applies blocks and commits each block's delta like the engine.
type harness struct {
	t         *testing.T
	processor *Processor
	store     StateStore
	offset    int64
}

func (h *harness) apply(block *streamv1.BlockEvent) string {
	h.t.Helper()
	output, err := h.processor.Apply(context.Background(), block)
	if err != nil {
		h.t.Fatalf("Apply(%d) error = %v", block.GetBlockHeight(), err)
	}
	h.commit(block.GetBlockHeight(), block.GetBlockHash())
	raw, err := json.Marshal(output)
	if err != nil {
		h.t.Fatalf("Marshal() error = %v", err)
	}
	return string(raw)
}

func (h *harness) revert(rollbackTo int64) {
	h.t.Helper()
	if err := h.processor.Revert(context.Background(), rollbackTo); err != nil {
		h.t.Fatalf("Revert(%d) error = %v", rollbackTo, err)
	}
	h.commit(rollbackTo, "")
}

func (h *harness) commit(height int64, hash string) {
	h.t.Helper()
	if h.store == nil {
		return
	}
	delta, err := h.processor.TakeDelta()
	if err != nil {
		h.t.Fatalf("TakeDelta() error = %v", err)
	}
	checkpoint := chainsync.Checkpoint{ChainID: 1, Offset: h.offset, Height: height, Hash: hash}
	if err := h.store.Commit(context.Background(), checkpoint, delta); err != nil {
		h.t.Fatalf("Commit() error = %v", err)
	}
	h.offset++
}

// values returns every journal's current values, encoded, so two processors
// can be compared regardless of how they reached their state.
func (h *harness) values() map[string]string {
	h.t.Helper()
	out := make(map[string]string)
	add := func(kind, key string, value any) {
		raw, err := json.Marshal(value)
		if err != nil {
			h.t.Fatalf("Marshal() error = %v", err)
		}
		out[kind+"/"+key] = string(raw)
	}
	processor := h.processor
	for key, value := range processor.decimals.values {
		add(kindDecimals, key, value)
	}
	for key, value := range processor.pools.values {
		add(kindPools, key, value)
	}
	for key, value := range processor.prices.values {
		add(kindPrices, key, value)
	}
	for key, value := range processor.orientations.values {
		add(kindOrientations, key, value)
	}
	for key, value := range processor.candles.values {
		add(kindCandles, candleKeys.encode(key), value)
	}
	for key, value := range processor.oracle.values {
		add(kindOracle, key, value)
	}
	for key, value := range processor.late.values {
		add(kindLate, key, value)
	}
	for key, value := range processor.missed.values {
		add(kindMissed, key, value)
	}
	for key, value := range processor.ticks.values {
		add(model.StateKindTicks, tickKeys.encode(key), value)
	}
	for key, value := range processor.clamm.values {
		add(model.StateKindCLAMMPools, key, value)
	}
	for key, value := range processor.balances.values {
		add(model.StateKindBalancePools, key, value)
	}
	return out
}

func assertSameValues(t *testing.T, got, want *harness) {
	t.Helper()
	gotValues, wantValues := got.values(), want.values()
	if len(gotValues) != len(wantValues) {
		t.Fatalf("state has %d keys, want %d", len(gotValues), len(wantValues))
	}
	for key, value := range wantValues {
		if gotValues[key] != value {
			t.Fatalf("state %s = %s, want %s", key, gotValues[key], value)
		}
	}
}

// restart restores a fresh processor from the store.
func (h *harness) restart() {
	h.t.Helper()
	state, ok, err := h.store.LoadState(context.Background(), 1)
	if err != nil || !ok {
		h.t.Fatalf("LoadState() = %v, %v", ok, err)
	}
	processor, err := NewProcessor(h.processor.options)
	if err != nil {
		h.t.Fatalf("NewProcessor() error = %v", err)
	}
	h.processor = processor
	if err := h.processor.Restore(state); err != nil {
		h.t.Fatalf("Restore() error = %v", err)
	}
}

func TestRestoredProcessorMatchesUninterruptedRun(t *testing.T) {
	for name, open := range stateStores(t) {
		t.Run(name, func(t *testing.T) {
			reference := &harness{t: t, processor: newTestProcessor(t)}
			persisted := &harness{t: t, processor: newTestProcessor(t), store: open(t)}
			for height := int64(100); height < 120; height++ {
				reference.apply(chainBlock(height, 0))
				persisted.apply(chainBlock(height, 0))
			}
			persisted.restart()
			assertSameValues(t, persisted, reference)
			for height := int64(120); height < 140; height++ {
				want := reference.apply(chainBlock(height, 0))
				if got := persisted.apply(chainBlock(height, 0)); got != want {
					t.Fatalf("block %d after restart:\n got %s\nwant %s", height, got, want)
				}
			}
		})
	}
}

func TestRestoredProcessorReverts(t *testing.T) {
	for name, open := range stateStores(t) {
		t.Run(name, func(t *testing.T) {
			reference := &harness{t: t, processor: newTestProcessor(t)}
			persisted := &harness{t: t, processor: newTestProcessor(t), store: open(t)}
			for height := int64(100); height < 120; height++ {
				reference.apply(chainBlock(height, 0))
				persisted.apply(chainBlock(height, 0))
			}
			// The reorg reaches below the restart, into persisted undo history.
			persisted.restart()
			reference.revert(115)
			persisted.revert(115)
			assertSameValues(t, persisted, reference)
			for height := int64(116); height < 125; height++ {
				want := reference.apply(chainBlock(height, 1))
				if got := persisted.apply(chainBlock(height, 1)); got != want {
					t.Fatalf("fork block %d:\n got %s\nwant %s", height, got, want)
				}
			}
			// A second restart sees the fork, not the reverted blocks.
			persisted.restart()
			assertSameValues(t, persisted, reference)
			reference.revert(122)
			persisted.revert(122)
			assertSameValues(t, persisted, reference)
			for height := int64(123); height < 130; height++ {
				want := reference.apply(chainBlock(height, 2))
				if got := persisted.apply(chainBlock(height, 2)); got != want {
					t.Fatalf("second fork block %d:\n got %s\nwant %s", height, got, want)
				}
			}
			assertSameValues(t, persisted, reference)
		})
	}
}

func TestRestoreKeepsReorgLimit(t *testing.T) {
	persisted := &harness{t: t, processor: newTestProcessor(t), store: NewMemory()}
	for height := int64(100); height < 120; height++ {
		persisted.apply(chainBlock(height, 0))
	}
	persisted.restart()
	if err := persisted.processor.Revert(context.Background(), 110); err == nil {
		t.Fatal("Revert() below the finalized height after restore error = nil")
	}
}

func TestRestoreNeedsFreshProcessor(t *testing.T) {
	processor := newTestProcessor(t)
	if _, err := processor.Apply(context.Background(), chainBlock(100, 0)); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if err := processor.Restore(State{}); err == nil {
		t.Fatal("Restore() on a used processor error = nil")
	}
	if err := newTestProcessor(t).Restore(State{Entries: []StateEntry{{Kind: "nope", Key: "k", Value: []byte("1")}}}); err == nil {
		t.Fatal("Restore() with an unknown kind error = nil")
	}
}

func TestCandleKeyCodecRoundTrip(t *testing.T) {
	key := candleKey{Subject: model.CandleSubjectToken, Key: "0xab|cd", Interval: 4 * time.Hour}
	got, err := candleKeys.decode(candleKeys.encode(key))
	if err != nil || got != key {
		t.Fatalf("decode(encode()) = %+v, %v", got, err)
	}
	if _, err := candleKeys.decode("bad"); err == nil {
		t.Fatal("decode(bad) error = nil")
	}
}
