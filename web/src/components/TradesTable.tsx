import { useEffect, useRef } from "react";
import type { Trade } from "../lib/api";
import type { Chain } from "../lib/chains";
import { formatAge, formatCompact, formatPrice, formatUsd, shortAddress } from "../lib/format";
import { useNow, useTokenSymbol } from "../lib/hooks";
import { tradeKey } from "../lib/series";
import { Address, Status } from "./common";

const SIDE_TEXT: Record<Trade["side"], string> = {
  buy: "买入",
  sell: "卖出",
  liquidity_added: "加池",
  liquidity_removed: "撤池",
  unknown: "未知",
};

// TradesTable lists trades newest first and flashes the ones that arrive
// after the first render.
export function TradesTable({
  chain,
  trades,
  loading,
  error,
  showPool,
}: {
  chain: Chain;
  trades: readonly Trade[];
  loading: boolean;
  error: Error | null;
  showPool?: boolean;
}) {
  const now = useNow();
  const seen = useRef<Set<string> | null>(null);
  const fresh = new Set<string>();
  if (seen.current) {
    for (const trade of trades) if (!seen.current.has(tradeKey(trade))) fresh.add(tradeKey(trade));
  }
  useEffect(() => {
    if (!loading) seen.current = new Set(trades.map(tradeKey));
  }, [trades, loading]);

  return (
    <div className="table-scroll">
      <table className="table trades">
        <thead>
          <tr>
            <th>时间</th>
            <th>类型</th>
            <th className="num">价格</th>
            <th className="num">数量</th>
            <th className="num">价值</th>
            <th className="num">报价数量</th>
            {showPool && <th>池子</th>}
            <th>交易者</th>
            <th>交易</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((trade) => {
            const key = tradeKey(trade);
            return (
              <tr key={key} className={`${trade.side} ${fresh.has(key) ? "fresh" : ""}`}>
                <td className="muted" title={`#${trade.block_height}`}>
                  {formatAge(trade.block_time, now)}
                </td>
                <td className={`side ${trade.side}`}>
                  {SIDE_TEXT[trade.side]}
                  {trade.outlier && (
                    <span className="badge warn" title="价格偏离参考价过多，未计入代币 K 线">
                      异常
                    </span>
                  )}
                </td>
                <td className="num mono">${formatPrice(trade.price_usd)}</td>
                <td className="num">{formatCompact(trade.base_amount)}</td>
                <td className={`num side ${trade.side}`}>{formatUsd(trade.value_usd)}</td>
                <td className="num">
                  <QuoteAmount chain={chain} trade={trade} />
                </td>
                {showPool && (
                  <td>
                    <Address chain={chain} value={trade.pool_id} kind="pool" label={`${trade.protocol} ${shortAddress(trade.pool_id, 4, 4)}`} />
                  </td>
                )}
                <td>
                  <Address chain={chain} value={trade.trader} kind="holder" label={shortAddress(trade.trader, 4, 4)} />
                </td>
                <td>
                  <Address chain={chain} value={trade.transaction_hash} kind="tx" label={`${trade.transaction_hash.slice(0, 10)}…`} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <Status loading={loading && trades.length === 0} error={error} empty={!loading && !error && trades.length === 0} />
    </div>
  );
}

function QuoteAmount({ chain, trade }: { chain: Chain; trade: Trade }) {
  const symbol = useTokenSymbol(chain.id, trade.quote_token);
  return (
    <>
      {formatCompact(trade.quote_amount)} <span className="muted">{symbol}</span>
    </>
  );
}
