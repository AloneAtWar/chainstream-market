package model

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// market-engine keeps each seeded concentrated-liquidity pool's tick table in
// its state, which market-api reads to show how many tokens each price range
// holds.
const (
	// StateKindTicks holds one initialized tick per entry, keyed by
	// TickStateKey, with the tick's liquidityNet as a JSON string.
	StateKindTicks = "ticks"
	// StateKindCLAMMPools holds a CLAMMPool per pool ID.
	StateKindCLAMMPools = "clamm_pools"
)

// DynamicFeeFlag marks a Uniswap V4 or PancakeSwap Infinity pool whose hook
// sets the fee per swap.
const DynamicFeeFlag = 0x800000

// DepthBand is the price band, in basis points either side of the price,
// that a concentrated pool's depth is measured over when its fee is at most
// MaxFee (hundredths of a basis point).
type DepthBand struct {
	MaxFee uint32
	BPS    int
}

// DefaultDepthBands size a pool's band by its fee tier: the tighter a tier,
// the closer to the price its liquidity sits.
func DefaultDepthBands() []DepthBand {
	return []DepthBand{{MaxFee: 100, BPS: 50}, {MaxFee: 500, BPS: 100}, {MaxFee: 3000, BPS: 200}, {MaxFee: 1_000_000, BPS: 500}}
}

// DefaultDynamicFeeDepthBPS is the band of a pool whose hook sets its fee.
const DefaultDynamicFeeDepthBPS = 200

// BandBPS returns the band for fee: dynamicBPS for a dynamic fee, else the
// first band whose MaxFee covers fee, else the last band.
func BandBPS(bands []DepthBand, dynamicBPS int, fee uint32) int {
	if fee&DynamicFeeFlag != 0 {
		return dynamicBPS
	}
	for _, band := range bands {
		if fee <= band.MaxFee {
			return band.BPS
		}
	}
	return bands[len(bands)-1].BPS
}

// CLAMMPool is a concentrated-liquidity pool whose tick table market-engine
// maintains: loaded from a chainstream-evm snapshot and kept current by the
// liquidity ranges of later mints and burns.
type CLAMMPool struct {
	Protocol string `json:"protocol"`
	// Fee is in hundredths of a basis point, or carries DynamicFeeFlag.
	Fee uint32 `json:"fee"`
	// SnapshotHeight is the height the table was last read at.
	SnapshotHeight int64 `json:"snapshot_height"`
	// SqrtPriceX96 is the pool price after its latest swap.
	SqrtPriceX96 string `json:"sqrt_price_x96"`
	// SwapDeltaHooks is set when the pool's hook may settle swaps outside
	// the tick table, so the table does not bound what the pool trades.
	SwapDeltaHooks bool `json:"swap_delta_hooks,omitempty"`
	// Stale is set when the table stopped matching the chain, such as a swap
	// reporting a different active liquidity. The pool's amounts are unknown
	// until the next snapshot.
	Stale bool `json:"stale,omitempty"`
}

// Usable reports whether the pool's table gives its token amounts.
func (pool CLAMMPool) Usable() bool { return !pool.Stale && !pool.SwapDeltaHooks }

// PoolTickTable is a pool's tick table as market-engine last committed it:
// each initialized tick's liquidityNet.
type PoolTickTable struct {
	Pool  CLAMMPool
	Ticks map[int]*big.Int
}

// TickStateKey is the state key of one pool tick: "pool|tick".
func TickStateKey(pool string, tick int32) string {
	return pool + "|" + strconv.FormatInt(int64(tick), 10)
}

// ParseTickStateKey splits a TickStateKey.
func ParseTickStateKey(key string) (string, int32, error) {
	index := strings.LastIndexByte(key, '|')
	if index <= 0 {
		return "", 0, fmt.Errorf("tick state key %q: want pool|tick", key)
	}
	tick, err := strconv.ParseInt(key[index+1:], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("tick state key %q: %w", key, err)
	}
	return key[:index], int32(tick), nil
}
