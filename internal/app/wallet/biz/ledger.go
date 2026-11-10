package biz

import (
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
)

// usdPlaces 是账本里美元金额保留的小数位。
const usdPlaces = 18

// Trade 是记入账本的一笔已定价成交：market-engine 的 live 记录或 ClickHouse trades 表里的一行。
type Trade struct {
	// RevisionHeight 是写出这笔成交的区块：通常等于 BlockHeight，等待代币精度的成交会由
	// 之后的区块写出。
	RevisionHeight  int64
	BlockHeight     int64
	BlockTime       time.Time
	TransactionHash string
	LogIndex        uint32
	OperationIndex  uint32
	PoolID          string
	Trader          string
	Side            model.Side
	BaseToken       string
	QuoteToken      string
	BaseAmount      decimal.Decimal
	PriceUSD        decimal.Decimal
	ValueUSD        decimal.Decimal
	Outlier         bool
}

// tracked reports whether a trade enters the ledger. Liquidity changes are
// not trades; a trade without a USD value or flagged as an outlier would
// put a wrong cost or a fake profit on the books.
//
// tracked：只有带 USD 金额、不是异常价的买卖进账本。流动性变化不是交易；没有 USD 金额
// 或异常价的成交会记出错误的成本或虚假的利润。
func tracked(trade Trade) bool {
	return (trade.Side == model.SideBuy || trade.Side == model.SideSell) && trade.Trader != "" &&
		trade.BaseToken != "" && trade.BaseAmount.IsPositive() && trade.ValueUSD.IsPositive() && !trade.Outlier
}

// record applies trade to position with the moving weighted average cost
// method and returns the trade as the ledger saw it. position is updated in
// place; a zero position is a wallet that never held the token.
//
// A buy adds its amount and USD value to the quantity and cost. A sell takes
// the costed part of its amount, min(amount, quantity), at the average cost:
// realized PnL is that part's share of the proceeds less its cost, and the
// cost left shrinks in proportion, so the average does not move. Whatever
// the sell takes beyond the costed quantity came in by transfer or airdrop
// and counts as volume but not as PnL. A sell that leaves less than
// closeRatio of the position closes it.
//
// record 按移动加权平均成本记账：买入把数量和 USD 金额加进持仓；卖出按均价取走有成本的
// 部分 min(数量, 持仓)，已实现盈亏 = 这部分对应的收入 − 成本，剩余成本按比例扣减所以
// 均价不变。超出有成本数量的部分来自转入或空投，只计成交额不计盈亏。卖出后剩余不到
// closeRatio 的视为清仓，避免舍入留下的零头让持仓一直不关。
func record(position *model.Position, trade Trade, closeRatio decimal.Decimal) model.WalletTrade {
	entry := model.WalletTrade{
		BlockHeight: trade.BlockHeight, BlockTime: trade.BlockTime, TransactionHash: trade.TransactionHash,
		LogIndex: trade.LogIndex, OperationIndex: trade.OperationIndex, Wallet: trade.Trader, Token: trade.BaseToken,
		QuoteToken: trade.QuoteToken, PoolID: trade.PoolID, Side: trade.Side, Amount: trade.BaseAmount,
		ValueUSD: trade.ValueUSD, PriceUSD: trade.PriceUSD,
	}
	position.Wallet, position.Token = trade.Trader, trade.BaseToken
	if trade.BlockTime.After(position.LastTradeTime) {
		position.LastTradeTime = trade.BlockTime
	}
	switch trade.Side {
	case model.SideBuy:
		entry.Event = model.WalletEventAdd
		if !position.Quantity.IsPositive() {
			entry.Event = model.WalletEventOpen
			position.OpenedAt = trade.BlockTime
		}
		if position.FirstBuyHeight == 0 {
			position.FirstBuyHeight, position.FirstBuyTime = trade.BlockHeight, trade.BlockTime
		}
		position.Quantity = position.Quantity.Add(trade.BaseAmount)
		position.CostUSD = position.CostUSD.Add(trade.ValueUSD)
		position.BoughtQty = position.BoughtQty.Add(trade.BaseAmount)
		position.BoughtUSD = position.BoughtUSD.Add(trade.ValueUSD)
		position.Buys++
	case model.SideSell:
		position.SoldQty = position.SoldQty.Add(trade.BaseAmount)
		position.SoldUSD = position.SoldUSD.Add(trade.ValueUSD)
		position.Sells++
		if !position.Quantity.IsPositive() {
			entry.Event = model.WalletEventUntracked
			break
		}
		costed := decimal.Min(trade.BaseAmount, position.Quantity)
		closing := position.Quantity.Sub(costed).LessThanOrEqual(position.Quantity.Mul(closeRatio))
		if closing {
			costed = position.Quantity
		}
		costSold := position.CostUSD
		if !closing {
			costSold = position.CostUSD.Mul(costed).DivRound(position.Quantity, usdPlaces)
		}
		proceeds := trade.ValueUSD
		if costed.LessThan(trade.BaseAmount) {
			proceeds = trade.ValueUSD.Mul(costed).DivRound(trade.BaseAmount, usdPlaces)
		}
		entry.Counted = true
		entry.CostSoldUSD = costSold
		entry.RealizedUSD = proceeds.Sub(costSold)
		position.RealizedUSD = position.RealizedUSD.Add(entry.RealizedUSD)
		if entry.RealizedUSD.IsPositive() {
			position.Wins++
		} else {
			position.Losses++
		}
		entry.Event = model.WalletEventReduce
		if closing {
			entry.Event = model.WalletEventClose
			if !position.OpenedAt.IsZero() {
				entry.HoldSeconds = max(int64(trade.BlockTime.Sub(position.OpenedAt)/time.Second), 0)
			}
			position.Quantity, position.CostUSD, position.OpenedAt = decimal.Zero, decimal.Zero, time.Time{}
		} else {
			position.Quantity = position.Quantity.Sub(costed)
			position.CostUSD = position.CostUSD.Sub(costSold)
		}
	case model.SideUnknown, model.SideLiquidityAdded, model.SideLiquidityRemoved:
		// tracked 已经把它们挡在账本外。
	}
	entry.QuantityAfter, entry.CostAfter = position.Quantity, position.CostUSD
	return entry
}
