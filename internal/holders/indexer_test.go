package holders

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc/rpctest"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

const (
	token  = "0x1111111111111111111111111111111111111111"
	broken = "0x2222222222222222222222222222222222222222"
	alice  = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob    = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	carol  = "0xcccccccccccccccccccccccccccccccccccccccc"
)

func openStore(t *testing.T) *pgstore.Store {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	store, err := pgstore.Open(context.Background(), pgstore.Options{
		DSN: dsn, Schema: fmt.Sprintf("holders_test_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

type fixture struct {
	t       *testing.T
	store   *pgstore.Store
	node    *rpctest.Node
	indexer *Indexer
	offset  int64
}

func newFixture(t *testing.T, reconcileEvery int64) *fixture {
	t.Helper()
	node := rpctest.NewNode()
	t.Cleanup(node.Close)
	fixture := &fixture{t: t, store: openStore(t), node: node}
	fixture.restart(reconcileEvery)
	return fixture
}

func (f *fixture) restart(reconcileEvery int64) {
	f.t.Helper()
	client, err := rpc.New(rpc.Options{URL: f.node.URL})
	if err != nil {
		f.t.Fatalf("rpc.New() error = %v", err)
	}
	f.indexer, err = New(Options{
		ChainID: 1, Store: f.store, Balances: client, HistoryDepth: 8,
		ReconcileEvery: reconcileEvery, ReconcileBatch: 10,
	})
	if err != nil {
		f.t.Fatalf("New() error = %v", err)
	}
	if _, _, err := f.indexer.Restore(context.Background(), 1); err != nil {
		f.t.Fatalf("Restore() error = %v", err)
	}
}

func transfer(asset, from, to string, amount int64) *streamv1.TokenTransfer {
	return &streamv1.TokenTransfer{Token: asset, From: from, To: to, RawAmount: fmt.Sprint(amount)}
}

func hashOf(height int64, fork string) string { return fmt.Sprintf("0x%d%s", height, fork) }

func (f *fixture) apply(height int64, fork string, transfers ...*streamv1.TokenTransfer) error {
	f.t.Helper()
	block := &streamv1.BlockEvent{
		ChainId: 1, BlockHeight: height, BlockHash: hashOf(height, fork), Transfers: transfers,
		FinalizedHeight: height - 20,
	}
	if err := f.indexer.Apply(context.Background(), block); err != nil {
		return err
	}
	f.commit(height, block.GetBlockHash())
	return nil
}

func (f *fixture) mustApply(height int64, fork string, transfers ...*streamv1.TokenTransfer) {
	f.t.Helper()
	if err := f.apply(height, fork, transfers...); err != nil {
		f.t.Fatalf("Apply(%d) error = %v", height, err)
	}
}

func (f *fixture) revert(rollbackTo int64) {
	f.t.Helper()
	if err := f.indexer.Revert(context.Background(), rollbackTo); err != nil {
		f.t.Fatalf("Revert(%d) error = %v", rollbackTo, err)
	}
	f.commit(rollbackTo, "")
}

func (f *fixture) commit(height int64, hash string) {
	f.t.Helper()
	checkpoint := chainsync.Checkpoint{ChainID: 1, Offset: f.offset, Height: height, Hash: hash}
	if err := f.indexer.Commit(context.Background(), checkpoint); err != nil {
		f.t.Fatalf("Commit() error = %v", err)
	}
	f.offset++
}

// balances returns every stored holding of asset.
func (f *fixture) balances(asset string) map[string]int64 {
	f.t.Helper()
	holdings, err := f.store.Holdings(context.Background(), 1, []storage.HolderKey{
		{Token: asset, Holder: alice}, {Token: asset, Holder: bob}, {Token: asset, Holder: carol},
	})
	if err != nil {
		f.t.Fatalf("Holdings() error = %v", err)
	}
	out := make(map[string]int64, len(holdings))
	for _, holding := range holdings {
		out[holding.Holder] = holding.Balance.Int64()
	}
	return out
}

func assertBalances(t *testing.T, got map[string]int64, want map[string]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("balances = %v, want %v", got, want)
	}
	for holder, balance := range want {
		if value, ok := got[holder]; !ok || value != balance {
			t.Fatalf("balances = %v, want %v", got, want)
		}
	}
}

func TestIndexerSeedsAccumulatesAndReverts(t *testing.T) {
	f := newFixture(t, 0)
	// Alice already held 500 before the indexer started; balanceOf sees it.
	f.node.SetBalance(hashOf(100, ""), token, alice, 1500)
	f.mustApply(100, "", transfer(token, zeroAddress, alice, 1000))

	f.node.SetBalance(hashOf(101, ""), token, bob, 300)
	f.mustApply(101, "", transfer(token, alice, bob, 300))
	assertBalances(t, f.balances(token), map[string]int64{alice: 1200, bob: 300})

	f.node.SetBalance(hashOf(102, "a"), token, carol, 100)
	f.mustApply(102, "a", transfer(token, alice, carol, 100), transfer(token, bob, zeroAddress, 50))
	assertBalances(t, f.balances(token), map[string]int64{alice: 1100, bob: 250, carol: 100})

	f.revert(101)
	assertBalances(t, f.balances(token), map[string]int64{alice: 1200, bob: 300})

	f.node.SetBalance(hashOf(102, "b"), token, carol, 50)
	f.mustApply(102, "b", transfer(token, bob, carol, 50))
	assertBalances(t, f.balances(token), map[string]int64{alice: 1200, bob: 250, carol: 50})

	count, err := f.store.HolderCount(context.Background(), 1, token)
	if err != nil || count != 3 {
		t.Fatalf("HolderCount() = %d, %v", count, err)
	}
	top, err := f.store.TopHolders(context.Background(), 1, token, 2)
	if err != nil || len(top) != 2 || top[0].Holder != alice || top[1].Holder != bob {
		t.Fatalf("TopHolders() = %+v, %v", top, err)
	}
	// Seeding asked balanceOf once per new holding, never for known ones.
	if f.node.Calls() != 4 {
		t.Fatalf("balanceOf calls = %d, want 4", f.node.Calls())
	}
}

func TestIndexerFallsBackWhenBalanceOfFails(t *testing.T) {
	f := newFixture(t, 0)
	f.node.Revert(broken)
	f.mustApply(100, "", transfer(broken, zeroAddress, alice, 70))
	assertBalances(t, f.balances(broken), map[string]int64{alice: 70})

	// A node without the block's state stops the indexer instead of guessing.
	f.node.Missing(hashOf(101, ""))
	if err := f.apply(101, "", transfer(token, zeroAddress, bob, 1)); err == nil {
		t.Fatal("Apply() without block state error = nil")
	}
}

func TestIndexerReconcilesDriftAndRevertsCorrections(t *testing.T) {
	f := newFixture(t, 4)
	f.node.SetBalance(hashOf(101, ""), token, alice, 1000)
	f.mustApply(101, "", transfer(token, zeroAddress, alice, 1000))
	f.mustApply(102, "")
	f.mustApply(103, "")
	// A rebase grew Alice's balance without a Transfer.
	f.node.SetBalance(hashOf(104, ""), token, alice, 1010)
	f.mustApply(104, "")
	assertBalances(t, f.balances(token), map[string]int64{alice: 1010})

	f.revert(103)
	assertBalances(t, f.balances(token), map[string]int64{alice: 1000})
}

func TestIndexerResumesAndLimitsReorgs(t *testing.T) {
	f := newFixture(t, 0)
	f.node.SetBalance(hashOf(100, ""), token, alice, 10)
	f.mustApply(100, "", transfer(token, zeroAddress, alice, 10))
	for height := int64(101); height <= 120; height++ {
		f.mustApply(height, "", transfer(token, alice, alice, 1))
	}
	f.restart(0)
	checkpoint, _, err := f.indexer.Restore(context.Background(), 1)
	if err != nil || checkpoint.Height != 120 || checkpoint.Offset != 20 {
		t.Fatalf("Restore() = %+v, %v", checkpoint, err)
	}
	if err := f.indexer.Revert(context.Background(), 105); !errors.Is(err, ErrReorgTooDeep) {
		t.Fatalf("Revert() below history error = %v", err)
	}
}

func TestNetTransfers(t *testing.T) {
	deltas := netTransfers([]*streamv1.TokenTransfer{
		transfer(token, zeroAddress, "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 10),
		transfer(token, alice, bob, 4),
		transfer(token, bob, alice, 0),
		{Token: token, From: alice, To: bob, RawAmount: "bad"},
	})
	want := map[storage.HolderKey]int64{{Token: token, Holder: alice}: 6, {Token: token, Holder: bob}: 4}
	if len(deltas) != len(want) {
		t.Fatalf("deltas = %v", deltas)
	}
	for key, value := range want {
		if deltas[key] == nil || deltas[key].Cmp(big.NewInt(value)) != 0 {
			t.Fatalf("deltas = %v", deltas)
		}
	}
}

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("New() without dependencies error = nil")
	}
}
