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

// applyLaunchpad clears the reserves of curves that migrated in this block.
// Their liquidity now sits in a DEX pool, so the curve's last state must not
// keep pricing the token; the price stays for the curve's own candles.
//
// applyLaunchpad：本块迁移到 DEX 的曲线池清空储备，此后不再参与定价，代币改由 DEX 池定价。
func (processor *Processor) applyLaunchpad(height int64, block *streamv1.BlockEvent, touched map[string]struct{}) {
	for _, event := range block.GetLaunchpadEvents() {
		if event.GetType() != streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_MIGRATED {
			continue
		}
		id := normalize(event.GetPoolId())
		state, ok := processor.pools.get(id)
		if !ok || !state.Curve || !state.HasReserves {
			continue
		}
		state.Reserves, state.HasReserves = [2]decimal.Decimal{}, false
		processor.setPool(height, id, state, true, touched)
	}
}
