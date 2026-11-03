package biz

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
)

// memoryRepo 是 WalletRepo 的内存实现，语义和 data 层一致：undo 每个（高度, 持仓）只留
// 第一次，回滚恢复范围内最早的 prev。
type memoryRepo struct {
	checkpoint *Checkpoint
	positions  map[model.WalletKey]model.Position
	undo       map[undoKey]*model.Position
	trades     []model.WalletTrade
	commits    int
}

func newMemoryRepo() *memoryRepo {
	return &memoryRepo{positions: make(map[model.WalletKey]model.Position), undo: make(map[undoKey]*model.Position)}
}

func (repo *memoryRepo) LoadWalletCheckpoint(context.Context, uint64) (Checkpoint, bool, error) {
	if repo.checkpoint == nil {
		return Checkpoint{}, false, nil
	}
	return *repo.checkpoint, true, nil
}

func (repo *memoryRepo) DiscardTradesAfter(_ context.Context, _ uint64, height int64, part int32) error {
	repo.trades = slices.DeleteFunc(repo.trades, func(trade model.WalletTrade) bool {
		return trade.ApplyHeight > height || (part != WholeBlock && trade.ApplyHeight == height && trade.Part > part)
	})
	return nil
}

func (repo *memoryRepo) Positions(_ context.Context, _ uint64, keys []model.WalletKey) ([]model.Position, error) {
	var found []model.Position
	for _, key := range keys {
		if position, ok := repo.positions[key]; ok {
			found = append(found, position)
		}
	}
	return found, nil
}

func (repo *memoryRepo) CommitWallet(_ context.Context, commit Commit) error {
	repo.commits++
	repo.trades = append(repo.trades, commit.Trades...)
	for _, position := range commit.Positions {
		repo.positions[position.Key()] = position
	}
	for _, entry := range commit.Undo {
		marker := undoKey{height: entry.Height, key: entry.Key}
		if _, ok := repo.undo[marker]; !ok {
			repo.undo[marker] = entry.Prev
		}
	}
	for marker := range repo.undo {
		if marker.height <= commit.Checkpoint.PrunedThrough {
			delete(repo.undo, marker)
		}
	}
	checkpoint := commit.Checkpoint
	repo.checkpoint = &checkpoint
	return nil
}

func (repo *memoryRepo) RevertWallet(_ context.Context, to int64, checkpoint Checkpoint) error {
	repo.trades = slices.DeleteFunc(repo.trades, func(trade model.WalletTrade) bool { return trade.ApplyHeight > to })
	earliest := make(map[model.WalletKey]undoKey)
	for marker := range repo.undo {
		if marker.height <= to {
			continue
		}
		if current, ok := earliest[marker.key]; !ok || marker.height < current.height {
			earliest[marker.key] = marker
		}
	}
	for key, marker := range earliest {
		delete(repo.positions, key)
		if prev := repo.undo[marker]; prev != nil {
			repo.positions[key] = *prev
		}
	}
	for marker := range repo.undo {
		if marker.height > to {
			delete(repo.undo, marker)
		}
	}
	repo.checkpoint = &checkpoint
	return nil
}

// memoryTrades 是 ClickHouse trades 表的替身。
type memoryTrades struct {
	trades []Trade
	first  int64
}

func (trades *memoryTrades) FirstHeightSince(context.Context, uint64, time.Time) (int64, bool, error) {
	return trades.first, trades.first > 0, nil
}

func (trades *memoryTrades) TradesBetween(_ context.Context, _ uint64, from, to int64) ([]Trade, error) {
	var found []Trade
	for _, trade := range trades.trades {
		if trade.RevisionHeight >= from && trade.RevisionHeight <= to {
			found = append(found, trade)
		}
	}
	return found, nil
}

type harness struct {
	t        *testing.T
	repo     *memoryRepo
	trades   *memoryTrades
	follower *Follower
	offset   int64
}

func newHarness(t *testing.T, repo *memoryRepo, trades *memoryTrades) *harness {
	t.Helper()
	follower, err := New(Options{
		ChainID: 1, Chain: "eth", Open: func(int64) (source.Source, error) { return nil, nil },
		Repo: repo, Trades: trades, HistoryDepth: 256, History: 24 * time.Hour, FinalAge: 1000 * time.Hour,
		BackfillHeights: 3, CloseRatio: 0.001, Now: func() time.Time { return t0.Add(time.Hour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	offset := int64(-1)
	if repo.checkpoint != nil {
		offset = repo.checkpoint.Offset
	}
	return &harness{t: t, repo: repo, trades: trades, follower: follower, offset: offset}
}

func liveTrade(trade Trade) live.Trade {
	return live.Trade{
		BlockHeight: trade.BlockHeight, BlockTime: trade.BlockTime.Unix(), RevisionHeight: trade.RevisionHeight,
		TransactionHash: trade.TransactionHash, LogIndex: trade.LogIndex, PoolID: trade.PoolID, Trader: trade.Trader,
		Side: trade.Side.String(), BaseToken: trade.BaseToken, QuoteToken: trade.QuoteToken,
		BaseAmount: trade.BaseAmount.String(), ValueUSD: trade.ValueUSD.String(), PriceUSD: "1",
	}
}

// send 把一条 live 记录作为下一个 offset 交给 Follower。
func (h *harness) send(update live.Update) error {
	h.t.Helper()
	update.ChainID = 1
	if update.Type == "" {
		update.Type = live.UpdateBlock
	}
	if update.Type == live.UpdateBlock && update.BlockTime == 0 {
		update.BlockTime = t0.Add(time.Duration(update.Height) * time.Second).Unix()
	}
	payload, err := json.Marshal(update)
	if err != nil {
		h.t.Fatal(err)
	}
	h.offset++
	return h.follower.Handle(context.Background(), source.Message{Offset: h.offset, Value: payload})
}

func (h *harness) block(height int64, part int, trades ...Trade) {
	h.t.Helper()
	update := live.Update{Height: height, Part: part}
	for _, trade := range trades {
		update.Trades = append(update.Trades, liveTrade(trade))
	}
	if err := h.send(update); err != nil {
		h.t.Fatalf("block %d part %d: %v", height, part, err)
	}
}

func (h *harness) position(token string) model.Position {
	return h.repo.positions[model.WalletKey{Wallet: walletA, Token: token}]
}

func (h *harness) tradeHeights() []int64 {
	heights := make([]int64, len(h.repo.trades))
	for index, trade := range h.repo.trades {
		heights[index] = trade.BlockHeight
	}
	return heights
}

func on(token string, base Trade) Trade {
	base.BaseToken = token
	return base
}

func TestFollowerBootstrapReplaysHistory(t *testing.T) {
	trades := &memoryTrades{first: 5, trades: []Trade{
		trade(5, model.SideBuy, "10", "10"),
		trade(7, model.SideBuy, "10", "30"),
		// 等待精度的成交：第 8 块的成交由第 9 块写出。
		func() Trade { late := trade(8, model.SideSell, "5", "15"); late.RevisionHeight = 9; return late }(),
	}}
	h := newHarness(t, newMemoryRepo(), trades)
	// 首次启动从末尾读到的第一条记录是 part 1：跳过，等第一个完整区块。
	h.block(9, 1)
	h.block(10, 0, trade(10, model.SideSell, "5", "20"))

	position := h.position(tokenX)
	// 买 20 个花 40（均价 2），卖 5 收 15（+5），再卖 5 收 20（+10）。
	if !position.Quantity.Equal(dec("10")) || !position.CostUSD.Equal(dec("20")) || !position.RealizedUSD.Equal(dec("15")) {
		t.Fatalf("position: quantity %s cost %s realized %s", position.Quantity, position.CostUSD, position.RealizedUSD)
	}
	if got := h.tradeHeights(); !slices.Equal(got, []int64{5, 7, 8, 10}) {
		t.Fatalf("trades %v", got)
	}
	if h.repo.checkpoint.Height != 10 || h.repo.checkpoint.Part != 0 || h.repo.checkpoint.Offset != 1 {
		t.Fatalf("checkpoint %+v", *h.repo.checkpoint)
	}
}

func TestFollowerFillsGapsAndSkipsDuplicates(t *testing.T) {
	trades := &memoryTrades{}
	h := newHarness(t, newMemoryRepo(), trades)
	h.block(10, 0, trade(10, model.SideBuy, "10", "10"))
	// 11-14 被 market-engine 的 max_lag 跳过，只在 ClickHouse 里。
	trades.trades = []Trade{trade(12, model.SideBuy, "10", "20"), trade(14, model.SideSell, "4", "8")}
	h.block(15, 0, trade(15, model.SideSell, "4", "12"))
	// market-engine 重启后重发。
	h.block(15, 0, trade(15, model.SideSell, "4", "12"))
	h.block(12, 0)

	if got := h.tradeHeights(); !slices.Equal(got, []int64{10, 12, 14, 15}) {
		t.Fatalf("trades %v", got)
	}
	position := h.position(tokenX)
	// 均价 1.5：卖 4 收 8（+2），卖 4 收 12（+6）。
	if !position.Quantity.Equal(dec("12")) || !position.RealizedUSD.Equal(dec("8")) {
		t.Fatalf("position: quantity %s realized %s", position.Quantity, position.RealizedUSD)
	}
}

func TestFollowerParts(t *testing.T) {
	h := newHarness(t, newMemoryRepo(), &memoryTrades{})
	h.block(10, 0)
	h.block(11, 0, trade(11, model.SideBuy, "1", "1"))
	h.block(11, 1, on(tokenY, trade(11, model.SideBuy, "1", "2")))
	if h.repo.checkpoint.Part != 1 || len(h.repo.trades) != 2 {
		t.Fatalf("checkpoint %+v trades %d", *h.repo.checkpoint, len(h.repo.trades))
	}
	// 缺了 part 2：不能带着缺失继续。
	if err := h.send(live.Update{Height: 11, Part: 3}); err == nil || !strings.Contains(err.Error(), "does not follow") {
		t.Fatalf("missing part: %v", err)
	}
}

func TestFollowerRevert(t *testing.T) {
	h := newHarness(t, newMemoryRepo(), &memoryTrades{})
	h.block(10, 0, trade(10, model.SideBuy, "10", "10"))
	h.block(11, 0, trade(11, model.SideBuy, "10", "30"), on(tokenY, trade(11, model.SideBuy, "1", "1")))
	h.block(12, 0, trade(12, model.SideSell, "20", "100"))
	before := h.position(tokenX)
	if !before.Quantity.IsZero() || !before.RealizedUSD.Equal(dec("60")) {
		t.Fatalf("before revert: %+v", before)
	}

	if err := h.send(live.Update{Type: live.UpdateRevert, Height: 10}); err != nil {
		t.Fatal(err)
	}
	after := h.position(tokenX)
	if !after.Quantity.Equal(dec("10")) || !after.CostUSD.Equal(dec("10")) || !after.RealizedUSD.IsZero() {
		t.Fatalf("after revert: %+v", after)
	}
	if _, ok := h.repo.positions[model.WalletKey{Wallet: walletA, Token: tokenY}]; ok {
		t.Fatal("a position opened above the revert height should be gone")
	}
	if got := h.tradeHeights(); !slices.Equal(got, []int64{10}) {
		t.Fatalf("trades %v", got)
	}
	// 新链上的 11。
	h.block(11, 0, trade(11, model.SideSell, "10", "5"))
	final := h.position(tokenX)
	if !final.Quantity.IsZero() || !final.RealizedUSD.Equal(dec("-5")) {
		t.Fatalf("new branch: %+v", final)
	}
	// 重复投递的回滚什么都不做。
	commits := h.repo.commits
	if err := h.send(live.Update{Type: live.UpdateRevert, Height: 11}); err != nil || h.repo.commits != commits {
		t.Fatalf("duplicate revert: %v, commits %d -> %d", err, commits, h.repo.commits)
	}
}

func TestFollowerRestartDiscardsUncommittedTrades(t *testing.T) {
	repo := newMemoryRepo()
	h := newHarness(t, repo, &memoryTrades{})
	h.block(10, 0, trade(10, model.SideBuy, "10", "10"))
	// 崩溃前写进 ClickHouse、但 PostgreSQL 事务没有提交的流水。
	repo.trades = append(repo.trades, model.WalletTrade{ApplyHeight: 11, BlockHeight: 11})

	restarted := newHarness(t, repo, &memoryTrades{})
	if got := restarted.tradeHeights(); !slices.Equal(got, []int64{10}) {
		t.Fatalf("trades after restart %v", got)
	}
	// 重启后从检查点的下一条继续，之前的 offset 被忽略。
	if err := restarted.follower.Handle(context.Background(), source.Message{Offset: 0, Value: []byte("not json")}); err != nil {
		t.Fatalf("an applied offset should be skipped: %v", err)
	}
	restarted.block(11, 0, trade(11, model.SideBuy, "10", "10"))
	if position := restarted.position(tokenX); !position.Quantity.Equal(dec("20")) {
		t.Fatalf("position %+v", position)
	}
}

func TestFollowerBatchesWhileCatchingUp(t *testing.T) {
	repo := newMemoryRepo()
	follower, err := New(Options{
		ChainID: 1, Open: func(int64) (source.Source, error) { return nil, nil }, Repo: repo, Trades: &memoryTrades{},
		HistoryDepth: 2, BackfillHeights: 10, BatchLag: time.Minute, FlushRows: 3, FlushInterval: time.Hour,
		Now: func() time.Time { return t0.Add(24 * time.Hour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, repo: repo, follower: follower, offset: -1}
	h.block(10, 0)
	h.block(11, 0, trade(11, model.SideBuy, "1", "1"))
	h.block(12, 0, trade(12, model.SideBuy, "1", "1"))
	if repo.commits != 0 {
		t.Fatalf("old blocks should be batched, got %d commits", repo.commits)
	}
	h.block(13, 0, trade(13, model.SideBuy, "1", "1"))
	if repo.commits != 1 || repo.checkpoint.Height != 13 || len(repo.trades) != 3 {
		t.Fatalf("commits %d checkpoint %+v trades %d", repo.commits, repo.checkpoint, len(repo.trades))
	}
	// finalized 为零时不裁剪 undo；之后的区块带上 finalized，undo 裁到 min(finalized, 高度 − depth)。
	if len(repo.undo) != 3 {
		t.Fatalf("undo %v", slices.Collect(maps.Keys(repo.undo)))
	}
	if err := h.send(live.Update{Height: 14, FinalizedHeight: 20, Trades: []live.Trade{liveTrade(trade(14, model.SideBuy, "1", "1"))},
		BlockTime: t0.Add(24 * time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	if repo.checkpoint.PrunedThrough != 12 || len(repo.undo) != 2 {
		t.Fatalf("pruned through %d undo %d", repo.checkpoint.PrunedThrough, len(repo.undo))
	}
}
