package biz

import (
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
)

var (
	t0        = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	ratio     = decimal.RequireFromString("0.001")
	walletA   = "0xaaaa"
	tokenX    = "0x1111"
	tokenY    = "0x2222"
	quoteWETH = "0xweth"
)

func dec(value string) decimal.Decimal { return decimal.RequireFromString(value) }

func trade(height int64, side model.Side, amount, value string) Trade {
	return Trade{
		RevisionHeight: height, BlockHeight: height, BlockTime: t0.Add(time.Duration(height) * time.Second),
		TransactionHash: "0xtx", PoolID: "0xpool", Trader: walletA, Side: side, BaseToken: tokenX,
		QuoteToken: quoteWETH, BaseAmount: dec(amount), ValueUSD: dec(value),
	}
}

func TestRecordMovingAverage(t *testing.T) {
	var position model.Position
	steps := []struct {
		trade    Trade
		event    model.WalletEvent
		realized string
		quantity string
		cost     string
	}{
		// 100 个，成本 100 美元，均价 1。
		{trade(1, model.SideBuy, "100", "100"), model.WalletEventOpen, "0", "100", "100"},
		// 再买 100 个花 300，均价 2。
		{trade(2, model.SideBuy, "100", "300"), model.WalletEventAdd, "0", "200", "400"},
		// 卖 50 个收 150：成本 100，盈利 50；均价不变。
		{trade(3, model.SideSell, "50", "150"), model.WalletEventReduce, "50", "150", "300"},
		// 卖剩下 150 个收 150：成本 300，亏 150，清仓。
		{trade(10, model.SideSell, "150", "150"), model.WalletEventClose, "-150", "0", "0"},
	}
	for index, step := range steps {
		entry := record(&position, step.trade, ratio)
		if entry.Event != step.event || !entry.RealizedUSD.Equal(dec(step.realized)) ||
			!position.Quantity.Equal(dec(step.quantity)) || !position.CostUSD.Equal(dec(step.cost)) {
			t.Fatalf("step %d: event %s realized %s quantity %s cost %s", index, entry.Event, entry.RealizedUSD,
				position.Quantity, position.CostUSD)
		}
	}
	if !position.RealizedUSD.Equal(dec("-100")) || position.Wins != 1 || position.Losses != 1 {
		t.Fatalf("totals: realized %s wins %d losses %d", position.RealizedUSD, position.Wins, position.Losses)
	}
	if position.Buys != 2 || position.Sells != 2 || !position.BoughtUSD.Equal(dec("400")) || !position.SoldUSD.Equal(dec("300")) {
		t.Fatalf("volume: %+v", position)
	}
	if !position.OpenedAt.IsZero() || position.FirstBuyHeight != 1 {
		t.Fatalf("opened %v first buy %d", position.OpenedAt, position.FirstBuyHeight)
	}
}

func TestRecordHoldSecondsOnClose(t *testing.T) {
	var position model.Position
	record(&position, trade(100, model.SideBuy, "10", "10"), ratio)
	entry := record(&position, trade(160, model.SideSell, "10", "20"), ratio)
	if entry.Event != model.WalletEventClose || entry.HoldSeconds != 60 {
		t.Fatalf("event %s hold %d", entry.Event, entry.HoldSeconds)
	}
	// 清仓后再买是新的一轮。
	entry = record(&position, trade(200, model.SideBuy, "1", "1"), ratio)
	if entry.Event != model.WalletEventOpen || !position.OpenedAt.Equal(trade(200, model.SideBuy, "1", "1").BlockTime) {
		t.Fatalf("reopen: event %s opened %v", entry.Event, position.OpenedAt)
	}
}

func TestRecordUntrackedSell(t *testing.T) {
	var position model.Position
	// 没买过就卖：转入或空投来的，只计成交额。
	entry := record(&position, trade(1, model.SideSell, "10", "50"), ratio)
	if entry.Event != model.WalletEventUntracked || entry.Counted || !entry.RealizedUSD.IsZero() {
		t.Fatalf("untracked: %+v", entry)
	}
	if position.Sells != 1 || !position.SoldUSD.Equal(dec("50")) || position.Wins+position.Losses != 0 {
		t.Fatalf("position: %+v", position)
	}
}

func TestRecordSellBeyondCostedQuantity(t *testing.T) {
	var position model.Position
	record(&position, trade(1, model.SideBuy, "10", "10"), ratio)
	// 卖 40 个收 200：只有 10 个有成本，按比例算收入 50，盈利 40；其余 30 个不计盈亏。
	entry := record(&position, trade(2, model.SideSell, "40", "200"), ratio)
	if entry.Event != model.WalletEventClose || !entry.CostSoldUSD.Equal(dec("10")) || !entry.RealizedUSD.Equal(dec("40")) {
		t.Fatalf("entry: event %s cost %s realized %s", entry.Event, entry.CostSoldUSD, entry.RealizedUSD)
	}
	if !position.SoldQty.Equal(dec("40")) || !position.SoldUSD.Equal(dec("200")) {
		t.Fatalf("volume: sold %s %s", position.SoldQty, position.SoldUSD)
	}
}

func TestRecordDustCloses(t *testing.T) {
	var position model.Position
	record(&position, trade(1, model.SideBuy, "1000", "1000"), ratio)
	// 剩 0.5 个，不到 0.1%：清仓，剩余成本全部算进这笔卖出。
	entry := record(&position, trade(2, model.SideSell, "999.5", "1999"), ratio)
	if entry.Event != model.WalletEventClose || !entry.CostSoldUSD.Equal(dec("1000")) || !position.Quantity.IsZero() {
		t.Fatalf("entry: event %s cost %s quantity %s", entry.Event, entry.CostSoldUSD, position.Quantity)
	}
	if !entry.RealizedUSD.Equal(dec("999")) {
		t.Fatalf("realized %s", entry.RealizedUSD)
	}
}

func TestTracked(t *testing.T) {
	base := trade(1, model.SideBuy, "1", "1")
	cases := map[string]func(*Trade){
		"no usd value": func(trade *Trade) { trade.ValueUSD = decimal.Zero },
		"outlier":      func(trade *Trade) { trade.Outlier = true },
		"no trader":    func(trade *Trade) { trade.Trader = "" },
		"liquidity":    func(trade *Trade) { trade.Side = model.SideUnknown },
		"zero amount":  func(trade *Trade) { trade.BaseAmount = decimal.Zero },
	}
	if !tracked(base) {
		t.Fatal("a priced buy should be tracked")
	}
	for name, change := range cases {
		candidate := base
		change(&candidate)
		if tracked(candidate) {
			t.Errorf("%s: tracked", name)
		}
	}
}
