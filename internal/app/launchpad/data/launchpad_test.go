package data

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/launchpad/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"gorm.io/gorm"
)

const (
	manager = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
	wbnb    = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
	tokenA  = "0xaaaa000000000000000000000000000000004444"
	tokenB  = "0xbbbb000000000000000000000000000000004444"
	pair    = "0x8ed59f0d00000000000000000000000000000ae2"
	// baseTime is block 0's time; each block is three seconds later.
	baseTime = 1_760_000_000
)

func openDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	db, err := pgdb.Open(context.Background(), pgdb.Options{
		DSN: dsn, Schema: fmt.Sprintf("launchpad_test_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { pgdb.Close(db) })
	return db
}

type fixture struct {
	t         *testing.T
	db        *gorm.DB
	launchpad *biz.Launchpad
	offset    int64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	fixture := &fixture{t: t, db: openDB(t)}
	fixture.restart()
	return fixture
}

func (f *fixture) restart() {
	f.t.Helper()
	var err error
	f.launchpad, err = biz.New(biz.Options{
		ChainID: 56, Repo: NewLaunchpadRepo(f.db), Protocols: []string{"fourmeme", "flap"}, HistoryDepth: 8,
	})
	if err != nil {
		f.t.Fatalf("New() error = %v", err)
	}
	if _, _, err := f.launchpad.Restore(context.Background(), 56); err != nil {
		f.t.Fatalf("Restore() error = %v", err)
	}
}

func poolID(token string) string { return manager + ":" + token }

func launch(token, symbol string) *streamv1.LaunchpadEvent {
	return &streamv1.LaunchpadEvent{
		Type: streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_CREATED, Protocol: "fourmeme", PoolId: poolID(token),
		Manager: manager, Token: token, Quote: wbnb, Creator: "0xC0FFEE", Name: symbol + " coin", Symbol: symbol,
		MetaUri: "ipfs://" + symbol, TransactionHash: "0xABC",
	}
}

func migrate(token string) *streamv1.LaunchpadEvent {
	return &streamv1.LaunchpadEvent{
		Type: streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_MIGRATED, Protocol: "fourmeme", PoolId: poolID(token),
		Manager: manager, Token: token, Quote: wbnb, TargetPoolId: pair,
	}
}

// trade is a curve trade: buys take tokens out of the curve.
func trade(token string, buy bool, progress string) *streamv1.Trade {
	tokenRaw, quoteRaw := "-1000", "5"
	if !buy {
		tokenRaw, quoteRaw = "1000", "-5"
	}
	return &streamv1.Trade{
		Action: streamv1.TradeAction_TRADE_ACTION_SWAP, Protocol: "fourmeme", PoolId: poolID(token),
		Amounts: []*streamv1.Amount{{Asset: token, Raw: tokenRaw}, {Asset: wbnb, Raw: quoteRaw}},
		PoolState: &streamv1.PoolState{Curve: &streamv1.CurveState{
			Progress: progress, BaseRemaining: "100", QuoteRaised: "7", Price: "9",
		}},
	}
}

func (f *fixture) apply(height int64, events []*streamv1.LaunchpadEvent, trades ...*streamv1.Trade) {
	f.t.Helper()
	block := &streamv1.BlockEvent{
		ChainId: 56, BlockHeight: height, BlockHash: fmt.Sprintf("0x%d", height), BlockTime: baseTime + 3*height,
		LaunchpadEvents: events, Trades: trades, FinalizedHeight: height - 20,
	}
	if err := f.launchpad.Apply(context.Background(), block); err != nil {
		f.t.Fatalf("Apply(%d) error = %v", height, err)
	}
	f.commit(height, block.GetBlockHash())
}

func (f *fixture) revert(rollbackTo int64) error {
	f.t.Helper()
	if err := f.launchpad.Revert(context.Background(), rollbackTo); err != nil {
		return err
	}
	f.commit(rollbackTo, "")
	return nil
}

func (f *fixture) commit(height int64, hash string) {
	f.t.Helper()
	checkpoint := chainsync.Checkpoint{ChainID: 56, Offset: f.offset, Height: height, Hash: hash}
	if err := f.launchpad.Commit(context.Background(), checkpoint); err != nil {
		f.t.Fatalf("Commit() error = %v", err)
	}
	f.offset++
}

func (f *fixture) token(address string) (model.LaunchpadListing, bool) {
	f.t.Helper()
	listing, ok, err := pgdb.LaunchpadToken(context.Background(), f.db, 56, address)
	if err != nil {
		f.t.Fatalf("LaunchpadToken() error = %v", err)
	}
	return listing, ok
}

func (f *fixture) list(column model.LaunchpadColumn) []string {
	f.t.Helper()
	listings, err := pgdb.LaunchpadList(context.Background(), f.db, model.LaunchpadQuery{
		ChainID: 56, Column: column, MaxAge: 3600, Stale: 60, MigratedAge: 3600, Limit: 10,
	})
	if err != nil {
		f.t.Fatalf("LaunchpadList(%s) error = %v", column, err)
	}
	tokens := make([]string, len(listings))
	for index, listing := range listings {
		tokens[index] = listing.Token
	}
	return tokens
}

func wad(percent int64) string {
	return new(big.Int).Mul(big.NewInt(percent), big.NewInt(10_000_000_000_000_000)).String()
}

func TestLaunchpadFollowsTheLifecycle(t *testing.T) {
	f := newFixture(t)
	f.apply(1, []*streamv1.LaunchpadEvent{launch(tokenA, "AAA")}, trade(tokenA, true, wad(10)))
	f.apply(2, []*streamv1.LaunchpadEvent{launch(tokenB, "BBB")},
		trade(tokenB, true, wad(30)), trade(tokenB, false, wad(25)), trade(tokenA, true, wad(20)))

	a, ok := f.token(tokenA)
	if !ok || a.Status != model.LaunchpadBonding || !a.LaunchSeen || a.LaunchHeight != 1 || a.LaunchTime != baseTime+3 ||
		*a.Symbol != "AAA" || *a.Creator != "0xc0ffee" || a.Trades != 2 || a.Buys != 2 || a.Progress.String() != wad(20) ||
		*a.LastTradeTime != baseTime+6 || a.HeadTime != baseTime+6 || a.PoolID != poolID(tokenA) {
		t.Fatalf("token A = %+v", a)
	}
	b, _ := f.token(tokenB)
	if b.Trades != 2 || b.Buys != 1 || b.Sells != 1 || b.Progress.String() != wad(25) {
		t.Fatalf("token B = %+v", b)
	}
	if got := f.list(model.LaunchpadColumnNew); fmt.Sprint(got) != fmt.Sprint([]string{tokenB, tokenA}) {
		t.Fatalf("new = %v", got)
	}
	if got := f.list(model.LaunchpadColumnBonding); fmt.Sprint(got) != fmt.Sprint([]string{tokenB, tokenA}) {
		t.Fatalf("bonding = %v", got)
	}

	// A migration closes the curve: progress fills, the token leaves the
	// new and bonding lists and joins the migrated one.
	f.apply(3, []*streamv1.LaunchpadEvent{migrate(tokenA)})
	a, _ = f.token(tokenA)
	if a.Status != model.LaunchpadMigrated || a.Progress.String() != wad(100) || *a.TargetPoolID != pair ||
		*a.MigratedTime != baseTime+9 || *a.CompletedTime != baseTime+9 || *a.Symbol != "AAA" {
		t.Fatalf("migrated token A = %+v", a)
	}
	if got := f.list(model.LaunchpadColumnMigrated); fmt.Sprint(got) != fmt.Sprint([]string{tokenA}) {
		t.Fatalf("migrated = %v", got)
	}
	if got := f.list(model.LaunchpadColumnNew); fmt.Sprint(got) != fmt.Sprint([]string{tokenB}) {
		t.Fatalf("new after migration = %v", got)
	}

	// A late curve trade never moves the status back.
	f.apply(4, nil, trade(tokenA, true, wad(50)))
	if a, _ = f.token(tokenA); a.Status != model.LaunchpadMigrated {
		t.Fatalf("status after a late trade = %s", a.Status)
	}

	// Tokens drop out of bonding once they stop trading for the stale
	// window, and out of new once they are older than max age.
	f.apply(40, nil)
	if got := f.list(model.LaunchpadColumnBonding); len(got) != 0 {
		t.Fatalf("bonding after a quiet minute = %v", got)
	}
	if got := f.list(model.LaunchpadColumnNew); fmt.Sprint(got) != fmt.Sprint([]string{tokenB}) {
		t.Fatalf("new after a quiet minute = %v", got)
	}
	f.apply(1300, nil)
	if got := f.list(model.LaunchpadColumnNew); len(got) != 0 {
		t.Fatalf("new after an hour = %v", got)
	}
}

func TestLaunchpadTradesBeforeTheLaunchAreNotNew(t *testing.T) {
	f := newFixture(t)
	f.apply(1, nil, trade(tokenA, true, wad(40)))
	a, ok := f.token(tokenA)
	if !ok || a.LaunchSeen || a.LaunchTime != baseTime+3 || a.Name != nil {
		t.Fatalf("token first seen trading = %+v", a)
	}
	if got := f.list(model.LaunchpadColumnNew); len(got) != 0 {
		t.Fatalf("new = %v", got)
	}
	if got := f.list(model.LaunchpadColumnBonding); fmt.Sprint(got) != fmt.Sprint([]string{tokenA}) {
		t.Fatalf("bonding = %v", got)
	}
}

func TestLaunchpadRevertsAndResumes(t *testing.T) {
	f := newFixture(t)
	f.apply(1, []*streamv1.LaunchpadEvent{launch(tokenA, "AAA")}, trade(tokenA, true, wad(10)))
	f.apply(2, nil, trade(tokenA, true, wad(20)))
	f.apply(3, []*streamv1.LaunchpadEvent{launch(tokenB, "BBB"), migrate(tokenA)}, trade(tokenB, true, wad(5)))

	if err := f.revert(1); err != nil {
		t.Fatalf("revert(1) error = %v", err)
	}
	a, _ := f.token(tokenA)
	if a.Status != model.LaunchpadBonding || a.Trades != 1 || a.Progress.String() != wad(10) || a.TargetPoolID != nil ||
		a.MigratedTime != nil {
		t.Fatalf("token A after revert = %+v", a)
	}
	if _, ok := f.token(tokenB); ok {
		t.Fatal("token B survived the revert of its launch")
	}

	// The replacement branch applies on top, and a restart resumes with
	// the undo history.
	f.apply(2, nil, trade(tokenA, false, wad(8)))
	f.restart()
	if err := f.revert(0); err != nil {
		t.Fatalf("revert(0) error = %v", err)
	}
	if _, ok := f.token(tokenA); ok {
		t.Fatal("token A survived a full revert")
	}

	// Undo history older than history_depth is pruned, and a reorg below
	// it is refused.
	for height := int64(1); height <= 20; height++ {
		f.apply(height, nil, trade(tokenA, true, wad(height)))
	}
	f.restart()
	if err := f.revert(11); !errors.Is(err, biz.ErrReorgTooDeep) {
		t.Fatalf("revert(11) error = %v, want ErrReorgTooDeep", err)
	}
	if err := f.revert(12); err != nil {
		t.Fatalf("revert(12) error = %v", err)
	}
	if a, _ = f.token(tokenA); a.Trades != 12 || a.Progress.String() != wad(12) {
		t.Fatalf("token A after revert(12) = %+v", a)
	}
}

// chainsync commits blocks it skips, a replayed block or a revert with
// nothing to undo, without applying them. Those commits move the checkpoint
// but keep the head the list windows count from.
func TestLaunchpadCheckpointOnlyCommitsKeepTheHead(t *testing.T) {
	f := newFixture(t)
	f.apply(100, []*streamv1.LaunchpadEvent{launch(tokenA, "AAA")}, trade(tokenA, true, wad(10)))
	f.commit(100, "0x100")
	f.commit(100, "0x100")
	var head struct{ BlockHeight, BlockTime int64 }
	if err := f.db.Raw(`SELECT block_height, block_time FROM launchpad_heads WHERE chain_id = 56`).Scan(&head).Error; err != nil {
		t.Fatalf("read head: %v", err)
	}
	if head.BlockHeight != 100 || head.BlockTime != baseTime+300 {
		t.Fatalf("head = %+v, want block 100 at %d", head, baseTime+300)
	}
	if a, ok := f.token(tokenA); !ok || a.Trades != 1 {
		t.Fatalf("token A = %+v (found %v)", a, ok)
	}
}
