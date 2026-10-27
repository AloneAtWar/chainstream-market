package biz

import (
	"context"
	"math/big"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/AloneAtWar/oh-my-blockchain/chain/evm/feature/dex/mechanism/clamm"
	"github.com/shopspring/decimal"
)

// clPool trades MEME (token0, 18 decimals) against USDC (token1, 6 decimals)
// near tick -269400, about $2 a MEME.
const clPool = "0xpool-meme-usdc-cl"

func clSqrtAt(tick int) string {
	sqrt, err := clamm.SqrtRatioAtTick(tick)
	if err != nil {
		panic(err)
	}
	return sqrt.String()
}

func clLegs(trade *streamv1.Trade, memeDelta, usdcDelta string) *streamv1.Trade {
	trade.Amounts = []*streamv1.Amount{
		{Asset: meme, Raw: memeDelta, Decimals: 18, DecimalsKnown: true},
		{Asset: usdc, Raw: usdcDelta, Decimals: 6, DecimalsKnown: true},
	}
	return trade
}

func clSwap(pool, sqrtPrice, liquidity, tick string) *streamv1.Trade {
	return clLegs(&streamv1.Trade{
		TransactionHash: "0xtx-swap-" + pool, Protocol: "uniswap_v3", PoolId: pool, Trader: "0xtrader",
		Action:    streamv1.TradeAction_TRADE_ACTION_SWAP,
		PoolState: &streamv1.PoolState{SqrtPriceX96: sqrtPrice, Liquidity: liquidity, Tick: tick},
	}, "1000000000000000000", "-2000000")
}

func clMint(pool string, lower, upper int32, delta string) *streamv1.Trade {
	action := streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_ADDED
	if delta[0] == '-' {
		action = streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_REMOVED
	}
	return clLegs(&streamv1.Trade{
		TransactionHash: "0xtx-mint-" + pool, Protocol: "uniswap_v3", PoolId: pool, Trader: "0xlp", Action: action,
		LiquidityRange: &streamv1.LiquidityRange{TickLower: lower, TickUpper: upper, LiquidityDelta: delta},
	}, "1", "1")
}

func clSnapshot(height int64, sqrtPrice string, fee uint32, ticks map[int32]string) *streamv1.PoolTickSnapshot {
	snapshot := &streamv1.PoolTickSnapshot{
		PoolId: clPool, Protocol: "uniswap_v3", SnapshotHeight: height, SqrtPriceX96: sqrtPrice, Fee: fee,
	}
	for tick, net := range ticks {
		snapshot.Ticks = append(snapshot.Ticks, &streamv1.TickNet{Tick: tick, LiquidityNet: net})
	}
	return snapshot
}

func bigTable(t *testing.T, values map[int]string) map[int]*big.Int {
	t.Helper()
	table := make(map[int]*big.Int, len(values))
	for tick, raw := range values {
		value, ok := new(big.Int).SetString(raw, 10)
		if !ok {
			t.Fatalf("bad liquidity %q", raw)
		}
		table[tick] = value
	}
	return table
}

func TestTickBookMatchesSegmentAmounts(t *testing.T) {
	table := bigTable(t, map[int]string{
		-887220: "7", -600: "3000000000000", -60: "5000000000000000", 0: "-1",
		60: "-3000000000000", 900: "-5000000000000000", 887220: "-6",
	})
	book := newTickBook(table)
	if book.err != nil {
		t.Fatalf("newTickBook() error = %v", book.err)
	}
	for _, tick := range []int{-887272, -887220, -700, -600, -59, -1, 0, 1, 30, 60, 899, 900, 5000, 887219} {
		sqrt, _ := clamm.SqrtRatioAtTick(tick)
		for _, price := range []*big.Int{sqrt, new(big.Int).Add(sqrt, big.NewInt(12345))} {
			want, err := clamm.SegmentAmounts(table, price)
			if err != nil {
				t.Fatalf("SegmentAmounts() error = %v", err)
			}
			got, ok := book.amounts(price)
			if !ok || got[0].Cmp(want.Amount0) != 0 || got[1].Cmp(want.Amount1) != 0 {
				t.Fatalf("tick %d: amounts %v, want %s / %s", tick, got, want.Amount0, want.Amount1)
			}
		}
	}
	for tick, want := range map[int32]string{-887221: "0", -887220: "7", -61: "3000000000007", 0: "5003000000000006", 899: "5000000000000006", 900: "6", 887220: "0"} {
		if got := book.active(tick); got.String() != want {
			t.Fatalf("active(%d) = %s, want %s", tick, got, want)
		}
	}
	if newTickBook(bigTable(t, map[int]string{-60: "5", 60: "-4"})).err == nil {
		t.Fatal("newTickBook() accepted a table that does not net to zero")
	}
}

func TestProcessorPricesThroughTickTable(t *testing.T) {
	processor := newTestProcessor(t)
	ctx := context.Background()
	const liquidity = "1000000000000000000"
	price := clSqrtAt(-269400)

	// Without a table the pool has a price but no amounts, so it prices
	// nothing.
	if _, err := processor.Apply(ctx, testBlock(99, 0, clSwap(clPool, price, liquidity, "-269400"))); err != nil {
		t.Fatalf("Apply(99) error = %v", err)
	}
	if state, _ := processor.pools.get(clPool); !state.HasPrice || state.HasReserves {
		t.Fatalf("unseeded pool = %+v", state)
	}
	if _, ok := processor.priceOf(meme); ok {
		t.Fatal("MEME priced through a pool without amounts")
	}

	block := testBlock(100, 12, clSwap(clPool, price, liquidity, "-269400"))
	block.TickSnapshots = []*streamv1.PoolTickSnapshot{clSnapshot(99, price, 3000, map[int32]string{-270000: liquidity, -268800: "-" + liquidity})}
	if _, err := processor.Apply(ctx, block); err != nil {
		t.Fatalf("Apply(100) error = %v", err)
	}
	table := bigTable(t, map[int]string{-270000: liquidity, -268800: "-" + liquidity})
	sqrt, _ := new(big.Int).SetString(price, 10)
	want, _ := clamm.SegmentAmounts(table, sqrt)
	state, _ := processor.pools.get(clPool)
	assertDecimal(t, "MEME amount", state.Reserves[0], decimal.NewFromBigInt(want.Amount0, -18).String())
	assertDecimal(t, "USDC amount", state.Reserves[1], decimal.NewFromBigInt(want.Amount1, -6).String())
	depth0, depth1, _ := clamm.DepthWithin(want, sqrt, 200)
	if !state.HasDepth || !state.Depth[0].Equal(decimal.NewFromBigInt(depth0, -18)) || !state.Depth[1].Equal(decimal.NewFromBigInt(depth1, -6)) {
		t.Fatalf("depth = %v %t, want %s / %s within ±2%%", state.Depth, state.HasDepth, depth0, depth1)
	}
	quote, ok := processor.priceOf(meme)
	if !ok || quote.Pool != clPool || !quote.LiquidityUSD.Equal(state.Reserves[1]) {
		t.Fatalf("MEME price = %+v, %t", quote, ok)
	}
	if usd := quote.USD.InexactFloat64(); usd < 1.99 || usd > 2.0 {
		t.Fatalf("MEME price = %s, want about 2", quote.USD)
	}

	// A mint adds its range; a later swap reporting the summed liquidity
	// keeps the table.
	mint := clMint(clPool, -269460, -269340, "500000000000000000")
	if _, err := processor.Apply(ctx, testBlock(101, 24, mint, clSwap(clPool, price, "1500000000000000000", "-269400"))); err != nil {
		t.Fatalf("Apply(101) error = %v", err)
	}
	record, _ := processor.clamm.get(clPool)
	if record.Stale || len(processor.poolTicks[clPool]) != 4 {
		t.Fatalf("after mint: record %+v, ticks %v", record, processor.poolTicks[clPool])
	}
	minted, _ := processor.pools.get(clPool)
	if !minted.Reserves[1].GreaterThan(state.Reserves[1]) {
		t.Fatalf("mint did not add USDC: %s, before %s", minted.Reserves[1], state.Reserves[1])
	}

	// A swap reporting other liquidity marks the table stale until the next
	// snapshot, and the pool loses its amounts.
	if _, err := processor.Apply(ctx, testBlock(102, 36, clSwap(clPool, price, "7", "-269400"))); err != nil {
		t.Fatalf("Apply(102) error = %v", err)
	}
	if record, _ := processor.clamm.get(clPool); !record.Stale {
		t.Fatal("mismatched swap left the table usable")
	}
	if stale, _ := processor.pools.get(clPool); stale.HasReserves || stale.HasDepth {
		t.Fatalf("stale pool kept amounts: %+v", stale)
	}

	// A reorg restores the table as of block 101.
	if err := processor.Revert(ctx, 101); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}
	if record, _ := processor.clamm.get(clPool); record.Stale || len(processor.poolTicks[clPool]) != 4 {
		t.Fatalf("after revert: record %+v, ticks %v", record, processor.poolTicks[clPool])
	}
	if reverted, _ := processor.pools.get(clPool); !reverted.Reserves[1].Equal(minted.Reserves[1]) {
		t.Fatalf("after revert USDC = %s, want %s", reverted.Reserves[1], minted.Reserves[1])
	}

	// A new snapshot replaces the table: ticks it lacks are removed.
	block = testBlock(102, 36)
	block.TickSnapshots = []*streamv1.PoolTickSnapshot{clSnapshot(101, price, 3000, map[int32]string{-269000: liquidity, -268000: "-" + liquidity})}
	if _, err := processor.Apply(ctx, block); err != nil {
		t.Fatalf("Apply(102 fork) error = %v", err)
	}
	if ticks := processor.poolTicks[clPool]; len(ticks) != 2 {
		t.Fatalf("reseeded ticks = %v", ticks)
	}
	// The price sits below the new range, so the pool holds only MEME.
	if reseeded, _ := processor.pools.get(clPool); !reseeded.HasReserves || !reseeded.Reserves[1].IsZero() || !reseeded.Reserves[0].IsPositive() {
		t.Fatalf("reseeded pool = %+v", reseeded)
	}
}

func TestSwapDeltaHookPoolsHaveNoAmounts(t *testing.T) {
	processor := newTestProcessor(t)
	price := clSqrtAt(-269400)
	block := testBlock(100, 0, clSwap(clPool, price, "1000000000000000000", "-269400"))
	snapshot := clSnapshot(99, price, model.DynamicFeeFlag, map[int32]string{-270000: "1000000000000000000", -268800: "-1000000000000000000"})
	snapshot.SwapDeltaHooks = true
	block.TickSnapshots = []*streamv1.PoolTickSnapshot{snapshot}
	if _, err := processor.Apply(context.Background(), block); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if state, _ := processor.pools.get(clPool); state.HasReserves || !state.HasPrice {
		t.Fatalf("hooked pool = %+v", state)
	}
}

func TestDepthBPSByFeeTier(t *testing.T) {
	processor := newTestProcessor(t)
	for fee, want := range map[uint32]int{100: 50, 500: 100, 2500: 200, 3000: 200, 10000: 500, 2_000_000: 500, model.DynamicFeeFlag: 200} {
		if got := processor.depthBPS(fee); got != want {
			t.Fatalf("depthBPS(%d) = %d, want %d", fee, got, want)
		}
	}
}
