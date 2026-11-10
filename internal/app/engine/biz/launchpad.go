package biz

import (
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"github.com/shopspring/decimal"
)

// minLiquidity is the counterpart reserve value state needs before a price
// propagates through it.
//
// minLiquidity：曲线池用 CurveMinLiquidityUSD，其它池子用 MinLiquidityUSD。
func (processor *Processor) minLiquidity(state pool) decimal.Decimal {
	if state.Curve {
		return processor.options.CurveMinLiquidityUSD
	}
	return processor.options.MinLiquidityUSD
}

// applyLaunchpad marks the curves that migrated in this block and clears
// their reserves. Their liquidity now sits in a DEX pool, so the curve's last
// state must not keep pricing the token; the price stays for the curve's own
// candles. A curve with no state yet, because its token's decimals are still
// unknown, gets a placeholder that keeps the mark.
//
// It returns each cleared curve's state from before the migration: this
// block's own curve trades happened then, so their candles are judged against
// that liquidity.
//
// applyLaunchpad：标记本块迁移到 DEX 的曲线池并清空储备，此后不再参与定价，代币改由 DEX 池定价。
// 尚无状态的曲线（代币小数位未知）写入占位状态保留迁移标记；返回清空前的状态，供本块曲线交易的 K 线门槛使用。
func (processor *Processor) applyLaunchpad(height int64, block *streamv1.BlockEvent, touched map[string]struct{}) map[string]pool {
	var before map[string]pool
	for _, event := range block.GetLaunchpadEvents() {
		if event.GetType() != streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_MIGRATED {
			continue
		}
		id := normalize(event.GetPoolId())
		state, ok := processor.pools.get(id)
		if !ok {
			state = pool{Curve: true, Tokens: [2]string{normalize(event.GetToken()), normalize(event.GetQuote())}}
		}
		if !state.Curve || state.Migrated {
			continue
		}
		if state.HasReserves {
			if before == nil {
				before = make(map[string]pool)
			}
			before[id] = state
		}
		state.Migrated = true
		state.Reserves, state.HasReserves = [2]decimal.Decimal{}, false
		processor.setPool(height, id, state, ok, touched)
	}
	return before
}
