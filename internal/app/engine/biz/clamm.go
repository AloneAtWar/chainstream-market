package biz

import (
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"sort"
	"strconv"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/AloneAtWar/oh-my-blockchain/chain/evm/feature/dex/mechanism/clamm"
	"github.com/shopspring/decimal"
)

// A concentrated-liquidity pool's token amounts come from its tick table, not
// from virtual reserves at the active liquidity: chainstream-evm snapshots a
// pool's table at the parent of the block it first trades in, and each later
// mint and burn carries its tick range. Between ranges, the price is all that
// moves, so the amounts follow from the table at the latest swap's price.

// tickKey names one initialized tick of a pool.
type tickKey struct {
	Pool string
	Tick int32
}

var tickKeys = keyCodec[tickKey]{
	kind:   model.StateKindTicks,
	encode: func(key tickKey) string { return model.TickStateKey(key.Pool, key.Tick) },
	decode: func(raw string) (tickKey, error) {
		pool, tick, err := model.ParseTickStateKey(raw)
		return tickKey{Pool: pool, Tick: tick}, err
	},
}

// depthBPS returns the band for fee.
func (processor *Processor) depthBPS(fee uint32) int {
	return model.BandBPS(processor.options.DepthBands, processor.options.DynamicFeeDepthBPS, fee)
}

func validateDepthBands(options *Options) error {
	if len(options.DepthBands) == 0 {
		options.DepthBands = model.DefaultDepthBands()
	}
	if options.DynamicFeeDepthBPS == 0 {
		options.DynamicFeeDepthBPS = model.DefaultDynamicFeeDepthBPS
	}
	valid := func(bps int) bool { return bps > 0 && bps < 10_000 }
	if !valid(options.DynamicFeeDepthBPS) {
		return fmt.Errorf("market: dynamic fee depth band %d bps outside (0, 10000)", options.DynamicFeeDepthBPS)
	}
	for index, band := range options.DepthBands {
		if !valid(band.BPS) {
			return fmt.Errorf("market: depth band %d bps outside (0, 10000)", band.BPS)
		}
		if index > 0 && band.MaxFee <= options.DepthBands[index-1].MaxFee {
			return errors.New("market: depth bands must ascend by fee")
		}
	}
	return nil
}

// applySnapshots replaces the tick tables of the pools the block seeds. A
// snapshot was read at the parent block, so the block's own ranges apply on
// top of it.
//
// applySnapshots 用区块携带的 tick 快照替换池子的 tick 表（快照在父区块读取，本块的区间变化叠加在其上）。
func (processor *Processor) applySnapshots(height int64, block *streamv1.BlockEvent, changed map[string]struct{}) {
	for _, snapshot := range block.GetTickSnapshots() {
		id := normalize(snapshot.GetPoolId())
		if id == "" {
			continue
		}
		record := model.CLAMMPool{
			Protocol: snapshot.GetProtocol(), Fee: snapshot.GetFee(), SnapshotHeight: snapshot.GetSnapshotHeight(),
			SqrtPriceX96: snapshot.GetSqrtPriceX96(), SwapDeltaHooks: snapshot.GetSwapDeltaHooks(),
		}
		next := make(map[int32]string, len(snapshot.GetTicks()))
		for _, tick := range snapshot.GetTicks() {
			net, ok := new(big.Int).SetString(tick.GetLiquidityNet(), 10)
			if !ok {
				record.Stale = true
				continue
			}
			if net.Sign() != 0 {
				next[tick.GetTick()] = net.String()
			}
		}
		for tick := range processor.poolTicks[id] {
			if _, kept := next[tick]; !kept {
				processor.setTick(height, id, tick, "")
			}
		}
		for _, tick := range slices.Sorted(maps.Keys(next)) {
			if current, _ := processor.ticks.get(tickKey{id, tick}); current != next[tick] {
				processor.setTick(height, id, tick, next[tick])
			}
		}
		processor.clamm.set(height, id, record)
		delete(processor.books, id)
		if _, err := processor.book(id); err != nil {
			record.Stale = true
			processor.clamm.set(height, id, record)
			processor.options.Logger.Warn("rejecting tick snapshot", "pool", id, "height", height, "error", err)
		}
		changed[id] = struct{}{}
	}
}

// applyRange adds a mint or burn to its seeded pool's tick table.
//
// applyRange 把 mint / burn 的区间变化加到 tick 表：下界 tick 加 delta，上界 tick 减 delta。
func (processor *Processor) applyRange(height int64, trade *streamv1.Trade, changed map[string]struct{}) {
	span := trade.GetLiquidityRange()
	if span == nil {
		return
	}
	id := normalize(trade.GetPoolId())
	record, ok := processor.clamm.get(id)
	if !ok || record.Stale {
		return
	}
	changed[id] = struct{}{}
	delta, ok := new(big.Int).SetString(span.GetLiquidityDelta(), 10)
	if !ok || span.GetTickLower() >= span.GetTickUpper() {
		processor.markStale(height, id, record, "invalid liquidity range")
		return
	}
	if delta.Sign() == 0 {
		return
	}
	processor.addNet(height, id, span.GetTickLower(), delta)
	processor.addNet(height, id, span.GetTickUpper(), new(big.Int).Neg(delta))
	delete(processor.books, id)
}

// checkSwap moves a seeded pool to its post-swap price and checks the swap's
// active liquidity against the table, marking the table stale on a mismatch.
//
// checkSwap 用 swap 后的价格移动池子，并校验事件给出的活跃流动性是否等于 tick 表中
// 当前 tick 以下 liquidityNet 之和；不一致说明表已失真，标记为 stale，直到下次快照。
func (processor *Processor) checkSwap(height int64, trade *streamv1.Trade, changed map[string]struct{}) {
	state := trade.GetPoolState()
	if trade.GetAction() != streamv1.TradeAction_TRADE_ACTION_SWAP || state.GetSqrtPriceX96() == "" {
		return
	}
	id := normalize(trade.GetPoolId())
	record, ok := processor.clamm.get(id)
	if !ok || record.Stale {
		return
	}
	changed[id] = struct{}{}
	record.SqrtPriceX96 = state.GetSqrtPriceX96()
	if state.GetLiquidity() == "" || state.GetTick() == "" {
		processor.clamm.set(height, id, record)
		return
	}
	reported, okLiquidity := new(big.Int).SetString(state.GetLiquidity(), 10)
	tick, err := strconv.ParseInt(state.GetTick(), 10, 32)
	book, bookErr := processor.book(id)
	switch {
	case !okLiquidity || err != nil:
		processor.markStale(height, id, record, "unreadable swap state")
	case bookErr != nil:
		processor.markStale(height, id, record, bookErr.Error())
	case book.active(int32(tick)).Cmp(reported) != 0:
		processor.markStale(height, id, record, fmt.Sprintf(
			"swap reports active liquidity %s at tick %d, table has %s", reported, tick, book.active(int32(tick))))
	default:
		processor.clamm.set(height, id, record)
	}
}

func (processor *Processor) markStale(height int64, id string, record model.CLAMMPool, reason string) {
	record.Stale = true
	processor.clamm.set(height, id, record)
	processor.options.Logger.Warn("tick table stale until the next snapshot", "pool", id, "height", height, "reason", reason)
}

func (processor *Processor) addNet(height int64, id string, tick int32, delta *big.Int) {
	net := new(big.Int)
	if current, ok := processor.ticks.get(tickKey{id, tick}); ok {
		net.SetString(current, 10)
	}
	net.Add(net, delta)
	value := ""
	if net.Sign() != 0 {
		value = net.String()
	}
	processor.setTick(height, id, tick, value)
}

// setTick writes a tick's liquidityNet, or removes the tick for "".
func (processor *Processor) setTick(height int64, id string, tick int32, net string) {
	key := tickKey{id, tick}
	if net == "" {
		processor.ticks.remove(height, key)
		delete(processor.poolTicks[id], tick)
		return
	}
	processor.ticks.set(height, key, net)
	if processor.poolTicks[id] == nil {
		processor.poolTicks[id] = make(map[int32]struct{})
	}
	processor.poolTicks[id][tick] = struct{}{}
}

// rebuildTicks indexes the tick journal by pool after a restore or revert.
func (processor *Processor) rebuildTicks() {
	processor.poolTicks = make(map[string]map[int32]struct{})
	for key := range processor.ticks.values {
		if processor.poolTicks[key.Pool] == nil {
			processor.poolTicks[key.Pool] = make(map[int32]struct{})
		}
		processor.poolTicks[key.Pool][key.Tick] = struct{}{}
	}
	processor.books = make(map[string]*tickBook)
}

// tableAmounts returns the raw token amounts a seeded pool's table holds at
// sqrtPriceX96. ok is false when the pool has no usable table.
func (processor *Processor) tableAmounts(id string, sqrtPriceX96 *big.Int) ([2]*big.Int, bool) {
	record, ok := processor.clamm.get(id)
	if !ok || !record.Usable() {
		return [2]*big.Int{}, false
	}
	book, err := processor.book(id)
	if err != nil {
		return [2]*big.Int{}, false
	}
	return book.amounts(sqrtPriceX96)
}

// settleTables sets the token amounts and depth of every seeded pool whose
// table or price the block changed, at the pool's latest price.
//
// settleTables 对本块中 tick 表或价格变化的池子，按最新价格重新计算储备和价格带深度。
func (processor *Processor) settleTables(height int64, changed map[string]struct{}, touched map[string]struct{}) {
	for _, id := range slices.Sorted(maps.Keys(changed)) {
		state, exists := processor.pools.get(id)
		if !exists {
			continue
		}
		next := state
		next.HasReserves, next.Reserves = false, [2]decimal.Decimal{}
		next.HasDepth, next.Depth = false, [2]decimal.Decimal{}
		if record, ok := processor.clamm.get(id); ok && record.Usable() {
			processor.fillFromTable(id, record, &next)
		}
		processor.setPool(height, id, next, true, touched)
	}
}

func (processor *Processor) fillFromTable(id string, record model.CLAMMPool, next *pool) {
	d0, ok0 := processor.decimals.get(next.Tokens[0])
	d1, ok1 := processor.decimals.get(next.Tokens[1])
	price, okPrice := new(big.Int).SetString(record.SqrtPriceX96, 10)
	book, err := processor.book(id)
	if !ok0 || !ok1 || !okPrice || err != nil {
		return
	}
	amounts, ok := book.amounts(price)
	if !ok {
		return
	}
	next.Reserves = [2]decimal.Decimal{decimal.NewFromBigInt(amounts[0], -int32(d0)), decimal.NewFromBigInt(amounts[1], -int32(d1))}
	next.HasReserves = true
	depth0, depth1, err := clamm.DepthWithin(book.distribution, price, processor.depthBPS(record.Fee))
	if err != nil {
		return
	}
	next.Depth = [2]decimal.Decimal{decimal.NewFromBigInt(depth0, -int32(d0)), decimal.NewFromBigInt(depth1, -int32(d1))}
	next.HasDepth = true
}

// book returns the pool's prepared table, building it when its ticks
// changed.
func (processor *Processor) book(id string) (*tickBook, error) {
	if book, ok := processor.books[id]; ok {
		return book, book.err
	}
	table := make(map[int]*big.Int, len(processor.poolTicks[id]))
	for tick := range processor.poolTicks[id] {
		raw, _ := processor.ticks.get(tickKey{id, tick})
		net, ok := new(big.Int).SetString(raw, 10)
		if !ok {
			net = nil
		}
		table[int(tick)] = net
	}
	book := newTickBook(table)
	processor.books[id] = book
	return book, book.err
}

// tickBook is a tick table prepared for amount lookups at any price: each
// range between initialized ticks holds only token0 above the price and only
// token1 below it, so the amounts are a suffix sum, a prefix sum and the one
// range the price sits in. Amounts round down per range, as SegmentAmounts
// does.
//
// tickBook 是为任意价格下快速查询币量而预处理的 tick 表：价格之上的区间只有 token0，
// 之下只有 token1，因此币量 = 后缀和 + 前缀和 + 价格所在区间的拆分；每段向下取整，
// 与 OMB 的 SegmentAmounts 一致。
type tickBook struct {
	distribution clamm.Distribution
	// ticks are the initialized ticks in order, and liquidity[i] the active
	// liquidity from ticks[i] to ticks[i+1].
	ticks     []int32
	liquidity []*big.Int
	// sqrtLower and sqrtUpper bound each range in distribution.Segments;
	// above0[i] sums token0 of ranges i and up, below1[i] token1 of ranges
	// before i.
	sqrtLower, sqrtUpper []*big.Int
	above0, below1       []*big.Int
	err                  error
}

func newTickBook(table map[int]*big.Int) *tickBook {
	book := &tickBook{}
	for tick, net := range table {
		if net == nil {
			book.err = fmt.Errorf("tick %d has no liquidity", tick)
			return book
		}
	}
	// SegmentAmounts validates the table; its amounts at the lowest price
	// are each range's full token0.
	low, err := clamm.SegmentAmounts(table, minSqrtRatio)
	if err != nil {
		book.err = err
		return book
	}
	book.distribution = low
	ticks := slices.Sorted(maps.Keys(table))
	cumulative := new(big.Int)
	for _, tick := range ticks {
		cumulative = new(big.Int).Add(cumulative, table[tick])
		book.ticks = append(book.ticks, int32(tick))
		book.liquidity = append(book.liquidity, cumulative)
	}
	count := len(low.Segments)
	book.sqrtLower, book.sqrtUpper = make([]*big.Int, count), make([]*big.Int, count)
	book.above0, book.below1 = make([]*big.Int, count+1), make([]*big.Int, count+1)
	book.above0[count], book.below1[0] = new(big.Int), new(big.Int)
	full1 := make([]*big.Int, count)
	for index, segment := range low.Segments {
		book.sqrtLower[index], _ = clamm.SqrtRatioAtTick(segment.TickLower)
		book.sqrtUpper[index], _ = clamm.SqrtRatioAtTick(segment.TickUpper)
		full1[index] = amount1Down(book.sqrtLower[index], book.sqrtUpper[index], segment.Liquidity)
	}
	for index := count - 1; index >= 0; index-- {
		book.above0[index] = new(big.Int).Add(book.above0[index+1], low.Segments[index].Amount0)
	}
	for index := range count {
		book.below1[index+1] = new(big.Int).Add(book.below1[index], full1[index])
	}
	return book
}

// amounts returns the token0 and token1 the table holds at sqrtPriceX96.
func (book *tickBook) amounts(sqrtPriceX96 *big.Int) ([2]*big.Int, bool) {
	if book.err != nil || clamm.ValidateSqrtPriceX96(sqrtPriceX96) != nil {
		return [2]*big.Int{}, false
	}
	count := len(book.distribution.Segments)
	// index is the first range whose upper bound is above the price.
	index := sort.Search(count, func(i int) bool { return book.sqrtUpper[i].Cmp(sqrtPriceX96) > 0 })
	amount0 := new(big.Int).Set(book.above0[index])
	amount1 := new(big.Int).Set(book.below1[index])
	if index < count && sqrtPriceX96.Cmp(book.sqrtLower[index]) > 0 {
		liquidity := book.distribution.Segments[index].Liquidity
		amount0.Sub(amount0, book.distribution.Segments[index].Amount0)
		amount0.Add(amount0, amount0Down(sqrtPriceX96, book.sqrtUpper[index], liquidity))
		amount1.Add(amount1, amount1Down(book.sqrtLower[index], sqrtPriceX96, liquidity))
	}
	return [2]*big.Int{amount0, amount1}, true
}

// active returns the liquidity in range at tick, the sum of liquidityNet at
// every initialized tick at or below it.
func (book *tickBook) active(tick int32) *big.Int {
	index := sort.Search(len(book.ticks), func(i int) bool { return book.ticks[i] > tick })
	if index == 0 {
		return new(big.Int)
	}
	return book.liquidity[index-1]
}

var (
	q96Int          = new(big.Int).Lsh(big.NewInt(1), 96)
	minSqrtRatio, _ = clamm.SqrtRatioAtTick(clamm.MinTick)
)

// amount0Down and amount1Down are Uniswap's SqrtPriceMath amount deltas
// rounded down, for sqrtA below sqrtB.
func amount0Down(sqrtA, sqrtB, liquidity *big.Int) *big.Int {
	numerator := new(big.Int).Lsh(liquidity, 96)
	numerator.Mul(numerator, new(big.Int).Sub(sqrtB, sqrtA))
	numerator.Quo(numerator, sqrtB)
	return numerator.Quo(numerator, sqrtA)
}

func amount1Down(sqrtA, sqrtB, liquidity *big.Int) *big.Int {
	product := new(big.Int).Mul(liquidity, new(big.Int).Sub(sqrtB, sqrtA))
	return product.Quo(product, q96Int)
}
