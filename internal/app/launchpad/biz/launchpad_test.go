package biz

import (
	"context"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

type recordingRepo struct{ changes []model.LaunchpadChange }

func (*recordingRepo) LoadLaunchpadCheckpoint(context.Context, uint64) (chainsync.Checkpoint, int64, bool, error) {
	return chainsync.Checkpoint{}, 0, false, nil
}

func (repo *recordingRepo) CommitLaunchpad(_ context.Context, _ chainsync.Checkpoint, change model.LaunchpadChange) error {
	repo.changes = append(repo.changes, change)
	return nil
}

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(Options{Repo: &recordingRepo{}, HistoryDepth: 1}); err == nil {
		t.Fatal("New() accepted a zero chain id")
	}
	if _, err := New(Options{ChainID: 56, HistoryDepth: 1}); err == nil {
		t.Fatal("New() accepted a nil repo")
	}
	if _, err := New(Options{ChainID: 56, Repo: &recordingRepo{}}); err == nil {
		t.Fatal("New() accepted a zero history depth")
	}
}

func swap(protocol, poolID, token, tokenRaw string, curve *streamv1.CurveState) *streamv1.Trade {
	return &streamv1.Trade{
		Action: streamv1.TradeAction_TRADE_ACTION_SWAP, Protocol: protocol, PoolId: poolID,
		Amounts:   []*streamv1.Amount{{Asset: token, Raw: tokenRaw}, {Asset: "0xQuote", Raw: "1"}},
		PoolState: &streamv1.PoolState{Curve: curve},
	}
}

func TestApplyCollectsOneChangePerToken(t *testing.T) {
	repo := &recordingRepo{}
	launchpad, err := New(Options{ChainID: 56, Repo: repo, Protocols: []string{"FLAP"}, HistoryDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	block := &streamv1.BlockEvent{
		BlockHeight: 100, BlockTime: 1_000, FinalizedHeight: 90,
		LaunchpadEvents: []*streamv1.LaunchpadEvent{
			{Type: streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_CREATED, Protocol: "fourmeme", PoolId: "0xM:0xA",
				Manager: "0xM", Token: "0xA", Quote: "0xQuote", Name: "A\x00", Symbol: "A", Creator: "0xC"},
			{Type: streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_COMPLETED, Protocol: "flap", PoolId: "0xF:0xB",
				Manager: "0xF", Token: "0xB", Quote: "0xQuote"},
		},
		Trades: []*streamv1.Trade{
			swap("fourmeme", "0xM:0xA", "0xA", "-5", &streamv1.CurveState{Progress: "10", Price: "3"}),
			swap("fourmeme", "0xM:0xA", "0xA", "5", &streamv1.CurveState{Progress: "8", Price: "2"}),
			// A configured protocol counts without a curve state.
			swap("flap", "0xF:0xB", "0xB", "-1", nil),
			// Other DEX trades and unscoped pools are not curve trades.
			swap("uniswap_v2", "0xpair", "0xC", "-1", nil),
			swap("fourmeme", "0xpair", "0xD", "-1", &streamv1.CurveState{Progress: "1"}),
		},
	}
	if err := launchpad.Apply(context.Background(), block); err != nil {
		t.Fatal(err)
	}
	if err := launchpad.Commit(context.Background(), chainsync.Checkpoint{ChainID: 56}); err != nil {
		t.Fatal(err)
	}
	change := repo.changes[0]
	if !change.Applied || change.Height != 100 || change.Time != 1_000 || change.PrunedThrough != 90 || len(change.Tokens) != 2 {
		t.Fatalf("change = %+v", change)
	}
	a, b := change.Tokens[0], change.Tokens[1]
	if a.Token != "0xa" || !a.Launched || a.Name != "A" || a.Creator != "0xc" || a.Status != model.LaunchpadBonding ||
		a.Trades != 2 || a.Buys != 1 || a.Sells != 1 || a.Progress.String() != "8" || a.Price.String() != "2" ||
		a.LastTradeTime != 1_000 || a.PoolID != "0xm:0xa" || a.Manager != "0xm" {
		t.Fatalf("token A = %+v", a)
	}
	if b.Token != "0xb" || b.Status != model.LaunchpadCompleted || b.Trades != 1 || b.Progress.Cmp(progressOne) != 0 {
		t.Fatalf("token B = %+v", b)
	}
}

func TestRevertRefusesPrunedHistory(t *testing.T) {
	repo := &recordingRepo{}
	launchpad, err := New(Options{ChainID: 56, Repo: repo, HistoryDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := launchpad.Apply(context.Background(), &streamv1.BlockEvent{BlockHeight: 10}); err != nil {
		t.Fatal(err)
	}
	if err := launchpad.Revert(context.Background(), 5); err == nil {
		t.Fatal("Revert() below the pruned height succeeded")
	}
	if err := launchpad.Revert(context.Background(), 6); err != nil {
		t.Fatalf("Revert(6) error = %v", err)
	}
	if err := launchpad.Commit(context.Background(), chainsync.Checkpoint{ChainID: 56}); err != nil {
		t.Fatal(err)
	}
	if change := repo.changes[0]; !change.Reverted || change.RevertTo != 6 {
		t.Fatalf("change = %+v", change)
	}
}

func TestPruningKeepsEveryUnfinalizedBlock(t *testing.T) {
	repo := &recordingRepo{}
	launchpad, err := New(Options{ChainID: 56, Repo: repo, HistoryDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	// Finalized lags the depth: everything above it stays revertible.
	if err := launchpad.Apply(context.Background(), &streamv1.BlockEvent{BlockHeight: 100, FinalizedHeight: 90}); err != nil {
		t.Fatal(err)
	}
	if err := launchpad.Revert(context.Background(), 90); err != nil {
		t.Fatalf("Revert(90) error = %v", err)
	}
	if err := launchpad.Revert(context.Background(), 89); err == nil {
		t.Fatal("Revert(89) below the finalized height succeeded")
	}
	for _, test := range []struct{ height, finalized, want int64 }{
		{100, 90, 90}, // finalized lags: keep everything above it
		{100, 98, 96}, // finalized is recent: keep the depth
		{100, 0, 96},  // finalized unknown: keep the depth
	} {
		if got := pruneBound(test.height, test.finalized, 4); got != test.want {
			t.Errorf("pruneBound(%d, %d, 4) = %d, want %d", test.height, test.finalized, got, test.want)
		}
	}
}

func TestCommitWithoutABlockOnlyMovesTheCheckpoint(t *testing.T) {
	repo := &recordingRepo{}
	launchpad, err := New(Options{ChainID: 56, Repo: repo, HistoryDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := launchpad.Apply(context.Background(), &streamv1.BlockEvent{BlockHeight: 100, BlockTime: 1_700_000_100}); err != nil {
		t.Fatal(err)
	}
	// chainsync commits a block it skipped as already handled without
	// applying it.
	for range 2 {
		if err := launchpad.Commit(context.Background(), chainsync.Checkpoint{ChainID: 56}); err != nil {
			t.Fatal(err)
		}
	}
	if first := repo.changes[0]; !first.Applied || first.Height != 100 || first.Time != 1_700_000_100 {
		t.Fatalf("applied change = %+v", first)
	}
	if second := repo.changes[1]; second.Applied || second.Reverted || second.PrunedThrough != 96 {
		t.Fatalf("checkpoint-only change = %+v, want neither applied nor reverted", second)
	}
}
