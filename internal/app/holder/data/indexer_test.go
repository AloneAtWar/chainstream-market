package data

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/holder/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc/rpctest"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

const (
	token  = "0x1111111111111111111111111111111111111111"
	broken = "0x2222222222222222222222222222222222222222"
	alice  = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob    = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	carol  = "0xcccccccccccccccccccccccccccccccccccccccc"

	zeroAddress = "0x0000000000000000000000000000000000000000"
	// multicall answers getEthBalance; the fake node keys its native
	// balances by this address.
	multicall = "0xca11bde05977b3631167028862be2a173976ca11"
	native    = rpc.NativeToken
)

// openRepo opens the repository on a fresh schema.
func openRepo(t *testing.T) *holderRepo {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	db, err := pgdb.Open(context.Background(), pgdb.Options{
		DSN: dsn, Schema: fmt.Sprintf("holders_test_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { pgdb.Close(db) })
	return &holderRepo{db: db}
}

type fixture struct {
	t *testing.T
	// skipNative turns native balances off on the next restart.
	skipNative bool
	repo       *holderRepo
	node       *rpctest.Node
	indexer    *biz.Indexer
	offset     int64
}

func newFixture(t *testing.T, reconcileEvery int64) *fixture {
	t.Helper()
	node := rpctest.NewNode()
	t.Cleanup(node.Close)
	fixture := &fixture{t: t, repo: openRepo(t), node: node}
	fixture.restart(reconcileEvery)
	return fixture
}

func (f *fixture) restart(reconcileEvery int64) {
	f.t.Helper()
	client, err := rpc.New(rpc.Options{URL: f.node.URL, NativeBalanceContract: multicall})
	if err != nil {
		f.t.Fatalf("rpc.New() error = %v", err)
	}
	f.indexer, err = biz.New(biz.Options{
		ChainID: 1, Repo: f.repo, Balances: client, HistoryDepth: 8,
		ReconcileEvery: reconcileEvery, ReconcileBatch: 10, NativeBalances: !f.skipNative,
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

// hashOf is a 32-byte block hash for height on fork, a hex digit or "".
func hashOf(height int64, fork string) string {
	return fmt.Sprintf("0x%062d%02s", height, fork)
}

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

// applyNative applies a block that touched the native balances of holders.
func (f *fixture) applyNative(height int64, fork string, holders ...string) {
	f.t.Helper()
	block := &streamv1.BlockEvent{
		ChainId: 1, BlockHeight: height, BlockHash: hashOf(height, fork), NativeTouched: holders,
		FinalizedHeight: height - 20,
	}
	if err := f.indexer.Apply(context.Background(), block); err != nil {
		f.t.Fatalf("Apply(%d) error = %v", height, err)
	}
	f.commit(height, block.GetBlockHash())
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
	holdings, err := f.repo.Holdings(context.Background(), 1, []model.HolderKey{
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

	var count pgdb.HolderCountRow
	if err := f.repo.db.Where("chain_id = 1 AND token = ?", token).Take(&count).Error; err != nil || count.Holders != 3 {
		t.Fatalf("holder_counts = %+v, %v", count, err)
	}
	var top []string
	if err := f.repo.db.Table("holder_balances").Where("chain_id = 1 AND token = ? AND balance > 0", token).
		Order("balance DESC, holder").Limit(2).Pluck("holder", &top).Error; err != nil || len(top) != 2 || top[0] != alice || top[1] != bob {
		t.Fatalf("top holders = %v, %v", top, err)
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
	if err := f.indexer.Revert(context.Background(), 105); !errors.Is(err, biz.ErrReorgTooDeep) {
		t.Fatalf("Revert() below history error = %v", err)
	}
}

func TestIndexerTracksNativeBalancesOfTouchedAddresses(t *testing.T) {
	f := newFixture(t, 0)
	// Bob holds nothing, so he is not kept; the zero address never is.
	f.node.SetBalance(hashOf(100, ""), multicall, alice, 1000)
	f.applyNative(100, "", alice, bob, zeroAddress)
	assertBalances(t, f.balances(native), map[string]int64{alice: 1000})

	// Gas and a payment move balances; each is read again at the block.
	f.node.SetBalance(hashOf(101, ""), multicall, alice, 690)
	f.node.SetBalance(hashOf(101, ""), multicall, bob, 300)
	f.applyNative(101, "", alice, bob)
	assertBalances(t, f.balances(native), map[string]int64{alice: 690, bob: 300})

	// An untouched address keeps its balance.
	f.node.SetBalance(hashOf(102, "a"), multicall, alice, 0)
	f.node.SetBalance(hashOf(102, "a"), multicall, carol, 680)
	f.applyNative(102, "a", "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", carol)
	assertBalances(t, f.balances(native), map[string]int64{alice: 0, bob: 300, carol: 680})

	f.revert(101)
	assertBalances(t, f.balances(native), map[string]int64{alice: 690, bob: 300})

	f.node.SetBalance(hashOf(102, "b"), multicall, alice, 680)
	f.applyNative(102, "b", alice)
	assertBalances(t, f.balances(native), map[string]int64{alice: 680, bob: 300})

	var count pgdb.HolderCountRow
	if err := f.repo.db.Where("chain_id = 1 AND token = ?", native).Take(&count).Error; err != nil || count.Holders != 2 {
		t.Fatalf("holder_counts = %+v, %v", count, err)
	}
}

func TestIndexerReconcilesNativeBalances(t *testing.T) {
	f := newFixture(t, 4)
	f.node.SetBalance(hashOf(101, ""), multicall, alice, 1000)
	f.applyNative(101, "", alice)
	f.mustApply(102, "")
	f.mustApply(103, "")
	// A beacon-chain withdrawal credited Alice without touching her.
	f.node.SetBalance(hashOf(104, ""), multicall, alice, 1032)
	f.mustApply(104, "")
	assertBalances(t, f.balances(native), map[string]int64{alice: 1032})
}

func TestIndexerReconcilesTokensWhenNativeBalancesAreOff(t *testing.T) {
	f := newFixture(t, 0)
	// More native holdings than one reconciliation batch, all older than
	// the token holding.
	for index := range 12 {
		holder := fmt.Sprintf("0x%040x", index+1)
		f.node.SetBalance(hashOf(100+int64(index), ""), multicall, holder, 1)
		f.applyNative(100+int64(index), "", holder)
	}
	f.node.SetBalance(hashOf(112, ""), token, alice, 1000)
	f.mustApply(112, "", transfer(token, zeroAddress, alice, 1000))

	f.skipNative = true
	f.restart(4)
	// A rebase grew Alice's balance without a Transfer; the stale native
	// holdings must not crowd her out of the batch.
	f.node.SetBalance(hashOf(116, ""), token, alice, 1010)
	for height := int64(113); height <= 116; height++ {
		f.mustApply(height, "")
	}
	assertBalances(t, f.balances(token), map[string]int64{alice: 1010})
}
