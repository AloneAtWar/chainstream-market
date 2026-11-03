package biz

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

type fakeStats struct {
	head         time.Time
	applyHeight  int64
	previous     *Refresh
	earliest     time.Time
	afterHeight  int64
	invalidDay   time.Time
	invalidAt    int64
	invalidSince uint64
	kept         []uint64
	rebuiltFrom  time.Time
	windows      map[int]time.Time
	saved        []Refresh
}

func (stats *fakeStats) TradeHead(context.Context, uint64) (time.Time, int64, bool, error) {
	return stats.head, stats.applyHeight, !stats.head.IsZero(), nil
}

func (stats *fakeStats) LoadRefresh(context.Context, uint64) (Refresh, bool, error) {
	if stats.previous == nil {
		return Refresh{}, false, nil
	}
	return *stats.previous, true, nil
}

func (stats *fakeStats) EarliestTradeFrom(_ context.Context, _ uint64, height int64) (time.Time, bool, error) {
	stats.afterHeight = height
	return stats.earliest, !stats.earliest.IsZero(), nil
}

func (stats *fakeStats) Invalidations(_ context.Context, _ uint64, since uint64) (time.Time, int64, bool, error) {
	stats.invalidSince = since
	return stats.invalidDay, stats.invalidAt, !stats.invalidDay.IsZero(), nil
}

func (stats *fakeStats) PruneStats(_ context.Context, _ uint64, keep []uint64) error {
	stats.kept = keep
	return nil
}

func (stats *fakeStats) RebuildDaily(_ context.Context, _ uint64, from time.Time, _ int64) error {
	stats.rebuiltFrom = from
	return nil
}

func (stats *fakeStats) WriteStats(_ context.Context, _ uint64, window int, from time.Time, _ uint64) error {
	stats.windows[window] = from
	return nil
}

func (stats *fakeStats) Bots(context.Context, uint64, BotRule, uint64) ([]string, error) {
	return []string{"0xbot"}, nil
}

func (stats *fakeStats) SmartMoney(context.Context, uint64, SmartMoneyRule, BotRule, uint64) ([]string, error) {
	return []string{"0xsmart"}, nil
}

func (stats *fakeStats) SaveRefresh(_ context.Context, refresh Refresh) error {
	stats.saved = append(stats.saved, refresh)
	return nil
}

type fakeLabels struct{ labels []model.WalletLabel }

func (labels *fakeLabels) ReplaceRuleLabels(_ context.Context, _ uint64, replaced []model.WalletLabel) error {
	labels.labels = replaced
	return nil
}

func newTestRefresher(t *testing.T, stats *fakeStats, labels *fakeLabels) *Refresher {
	t.Helper()
	refresher, err := NewRefresher(StatsOptions{
		ChainID: 1, Chain: "eth", Repo: stats, Labels: labels, Windows: []int{7, 30, 90},
		SmartMoney: SmartMoneyRule{WindowDays: 30}, Bot: BotRule{WindowDays: 7},
		Now: func() time.Time { return time.Date(2026, 11, 3, 8, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return refresher
}

func date(month time.Month, dayOfMonth int) time.Time {
	return time.Date(2026, month, dayOfMonth, 0, 0, 0, 0, time.UTC)
}

func TestRefreshFirstRunCoversLongestWindow(t *testing.T) {
	stats := &fakeStats{head: date(11, 2).Add(15 * time.Hour), applyHeight: 500, windows: make(map[int]time.Time)}
	labels := &fakeLabels{}
	if err := newTestRefresher(t, stats, labels).Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 窗口按最新成交的日子往前数，含当天。
	if !stats.rebuiltFrom.Equal(date(8, 5)) || !stats.windows[7].Equal(date(10, 27)) || !stats.windows[30].Equal(date(10, 4)) {
		t.Fatalf("rebuilt from %v windows %v", stats.rebuiltFrom, stats.windows)
	}
	if len(stats.saved) != 1 || stats.saved[0].ApplyHeight != 500 {
		t.Fatalf("saved %+v", stats.saved)
	}
	var got []string
	for _, label := range labels.labels {
		got = append(got, label.Wallet+"="+label.Label)
	}
	if !slices.Equal(got, []string{"0xsmart=smart_money", "0xbot=bot"}) {
		t.Fatalf("labels %v", got)
	}
}

func TestRefreshRebuildsOnlyChangedDays(t *testing.T) {
	stats := &fakeStats{
		head: date(11, 2).Add(time.Hour), applyHeight: 600, windows: make(map[int]time.Time),
		previous: &Refresh{Head: date(11, 1).Add(23 * time.Hour), ApplyHeight: 500},
	}
	refresher := newTestRefresher(t, stats, &fakeLabels{})
	if err := refresher.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 上次最新成交的前一天起。
	if !stats.rebuiltFrom.Equal(date(10, 31)) {
		t.Fatalf("rebuilt from %v", stats.rebuiltFrom)
	}
	// 补缺写进了更早的成交。
	stats.earliest = date(10, 20).Add(5 * time.Hour)
	if err := refresher.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !stats.rebuiltFrom.Equal(date(10, 20)) {
		t.Fatalf("rebuilt from %v", stats.rebuiltFrom)
	}
}

func TestRefreshRebuildsDaysOfRevertedTrades(t *testing.T) {
	stats := &fakeStats{
		head: date(11, 3).Add(time.Hour), applyHeight: 101, windows: make(map[int]time.Time),
		previous: &Refresh{Version: 7, Started: 5, Head: date(11, 3), ApplyHeight: 100},
		// 回滚 100 → 99 删掉了 11 月 1 日的延迟成交。
		invalidDay: date(11, 1), invalidAt: 99,
	}
	if err := newTestRefresher(t, stats, &fakeLabels{}).Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats.invalidSince != 5 || !stats.rebuiltFrom.Equal(date(11, 1)) || stats.afterHeight != 99 {
		t.Fatalf("since %d rebuilt from %v after height %d", stats.invalidSince, stats.rebuiltFrom, stats.afterHeight)
	}
	// 发布后只留这次和上次的版本。
	version := stats.saved[0].Version
	if len(stats.kept) != 2 || stats.kept[0] != version || stats.kept[1] != 7 || stats.saved[0].Started != version {
		t.Fatalf("kept %v saved %+v", stats.kept, stats.saved[0])
	}
}

func TestRefreshWithoutTradesDoesNothing(t *testing.T) {
	stats := &fakeStats{windows: make(map[int]time.Time)}
	if err := newTestRefresher(t, stats, &fakeLabels{}).Refresh(context.Background()); err != nil || len(stats.saved) != 0 {
		t.Fatalf("err %v saved %v", err, stats.saved)
	}
}

func TestNewRefresherRejectsRuleWindowOutsideStats(t *testing.T) {
	_, err := NewRefresher(StatsOptions{
		ChainID: 1, Repo: &fakeStats{}, Labels: &fakeLabels{}, Windows: []int{7, 30},
		SmartMoney: SmartMoneyRule{WindowDays: 90}, Bot: BotRule{WindowDays: 7},
	})
	if err == nil {
		t.Fatal("expected an error")
	}
}
