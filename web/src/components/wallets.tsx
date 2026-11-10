import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type Position, type RoiDistribution, type Trader, type WalletEvent, type WalletStats, type WalletTradeCursor } from "../lib/api";
import type { Chain } from "../lib/chains";
import {
  formatAge,
  formatCompact,
  formatDuration,
  formatInteger,
  formatPercent,
  formatPnl,
  formatPrice,
  formatUsd,
  pnlClass,
  shortAddress,
} from "../lib/format";
import { useNow } from "../lib/hooks";
import { Address, Stat, Status } from "./common";
import { TokenSymbol } from "./tables";

// LABELS are how market-wallet's rule labels read; other labels show as is.
const LABELS: Record<string, { text: string; icon: string; className: string }> = {
  smart_money: { text: "聪明钱", icon: "🧠", className: "smart" },
  bot: { text: "机器人", icon: "🤖", className: "bot" },
};

export function labelText(label: string): string {
  return LABELS[label]?.text ?? label;
}

export function WalletLabels({ labels, compact }: { labels: readonly string[] | undefined; compact?: boolean }) {
  if (!labels || labels.length === 0) return null;
  return (
    <span className="wallet-labels">
      {labels.map((label) => {
        const known = LABELS[label];
        return (
          <span key={label} className={`label ${known?.className ?? ""}`} title={known?.text ?? label}>
            {known ? (compact ? known.icon : `${known.icon} ${known.text}`) : label}
          </span>
        );
      })}
    </span>
  );
}

// Wallet links a wallet to its profile, with its labels after it.
export function Wallet({ chain, wallet, labels }: { chain: Chain; wallet: string; labels?: readonly string[] }) {
  return (
    <span className="wallet-cell">
      <Address chain={chain} value={wallet} kind="holder" label={shortAddress(wallet, 6, 4)} />
      <WalletLabels labels={labels} compact />
    </span>
  );
}

const ROI_BUCKETS: { key: keyof RoiDistribution; label: string; className: string }[] = [
  { key: "over_5x", label: "> 5x", className: "roi-5x" },
  { key: "from_2x_to_5x", label: "2x – 5x", className: "roi-2x" },
  { key: "from_0_to_2x", label: "0 – 2x", className: "roi-0x" },
  { key: "loss_0_to_50", label: "亏损 0 – 50%", className: "roi-loss" },
  { key: "loss_over_50", label: "亏损 > 50%", className: "roi-loss50" },
];

// RoiBar stacks how many tokens ended in each ROI bucket.
export function RoiBar({ distribution, legend }: { distribution: RoiDistribution; legend?: boolean }) {
  const total = ROI_BUCKETS.reduce((sum, bucket) => sum + distribution[bucket.key], 0);
  const title = ROI_BUCKETS.map((bucket) => `${bucket.label}: ${distribution[bucket.key]}`).join("\n");
  return (
    <div className="roi">
      <div className="roi-bar" title={title}>
        {total === 0 ? (
          <span className="roi-empty" />
        ) : (
          ROI_BUCKETS.map((bucket) =>
            distribution[bucket.key] > 0 ? (
              <span key={bucket.key} className={bucket.className} style={{ flexGrow: distribution[bucket.key] }} />
            ) : null,
          )
        )}
      </div>
      {legend && (
        <div className="roi-legend">
          {ROI_BUCKETS.map((bucket) => (
            <span key={bucket.key}>
              <i className={bucket.className} />
              {bucket.label} <b>{distribution[bucket.key]}</b>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

// WalletStatsGrid shows one window of a wallet's stats.
export function WalletStatsGrid({ stats }: { stats: WalletStats }) {
  return (
    <>
      <dl className="stats wallet-stats">
        <Stat label="已实现盈亏" value={<span className={pnlClass(stats.realized_usd)}>{formatPnl(stats.realized_usd)}</span>} />
        <Stat label="ROI" value={<span className={pnlClass(stats.roi)}>{formatPercent(stats.roi)}</span>} />
        <Stat
          label="胜率"
          value={
            <span title={`计入盈亏的卖出 ${stats.counted_sells} 笔，盈利 ${stats.wins} 笔`}>
              {stats.counted_sells > 0 ? formatPercent(stats.win_rate) : "—"}
              <span className="muted stat-sub">
                {" "}
                {stats.wins}/{stats.counted_sells}
              </span>
            </span>
          }
        />
        <Stat label="交易代币" value={formatInteger(stats.tokens)} />
        <Stat label="买入" value={`${formatUsd(stats.bought_usd)} · ${formatInteger(stats.buys)} 笔`} />
        <Stat label="卖出" value={`${formatUsd(stats.sold_usd)} · ${formatInteger(stats.sells)} 笔`} />
        <Stat label="平均持仓" value={stats.closes > 0 ? formatDuration(stats.average_hold_seconds) : "—"} />
        <Stat
          label="快进快出"
          value={stats.closes > 0 ? `${formatInteger(stats.fast_closes)} / ${formatInteger(stats.closes)} 次清仓` : "—"}
        />
      </dl>
      <div className="roi-block">
        <div className="muted roi-title">代币收益分布</div>
        <RoiBar distribution={stats.roi_distribution} legend />
      </div>
    </>
  );
}

const EVENT_TEXT: Record<WalletEvent, string> = {
  open: "建仓",
  add: "加仓",
  reduce: "减仓",
  close: "清仓",
  untracked: "卖出无成本",
  unknown: "未知",
};

export function PositionsTable({ chain, wallet, status }: { chain: Chain; wallet: string; status?: "open" | "closed" }) {
  const now = useNow(10_000);
  const positions = useQuery({
    queryKey: ["wallet-positions", chain.id, wallet, status],
    queryFn: () => api.walletPositions(chain.id, wallet, status, 100),
    refetchInterval: 30_000,
  });
  const rows = positions.data ?? [];
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>代币</th>
            <th className="num">持仓</th>
            <th className="num">均价</th>
            <th className="num">现价</th>
            <th className="num">未实现</th>
            <th className="num">已实现</th>
            <th className="num">买入 / 卖出</th>
            <th className="num">胜 / 负</th>
            <th className="num">建仓</th>
            <th className="num">最近交易</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((position) => (
            <PositionRow key={position.token} chain={chain} position={position} now={now} />
          ))}
        </tbody>
      </table>
      <Status loading={positions.isLoading} error={positions.error} empty={!positions.isLoading && rows.length === 0}>
        {status === "open" ? "当前没有持仓" : "没有持仓记录"}
      </Status>
    </div>
  );
}

function PositionRow({ chain, position, now }: { chain: Chain; position: Position; now: number }) {
  return (
    <tr>
      <td>
        <Link to={`/${chain.id}/token/${position.token}`}>{position.symbol ?? <TokenSymbol chain={chain} token={position.token} />}</Link>
        {position.status === "closed" && <span className="badge">已清仓</span>}
      </td>
      <td className="num">{position.status === "open" ? formatCompact(position.quantity) : "—"}</td>
      <td className="num mono">{position.status === "open" ? `$${formatPrice(position.average_cost_usd)}` : "—"}</td>
      <td className="num mono">{position.price_usd === null ? "—" : `$${formatPrice(position.price_usd)}`}</td>
      <td className={`num ${pnlClass(position.unrealized_usd)}`}>{position.status === "open" ? formatPnl(position.unrealized_usd) : "—"}</td>
      <td className={`num ${pnlClass(position.realized_usd)}`}>{formatPnl(position.realized_usd)}</td>
      <td className="num">
        {formatUsd(position.bought_usd)} / {formatUsd(position.sold_usd)}
      </td>
      <td className="num">
        <span className="up">{position.wins}</span> / <span className="down">{position.losses}</span>
      </td>
      <td className="num muted">{position.opened_at ? formatAge(position.opened_at, now) : "—"}</td>
      <td className="num muted">{formatAge(position.last_trade_time, now)}</td>
    </tr>
  );
}

// WalletTradesTable pages through a wallet's ledger trades, optionally of
// one token.
export function WalletTradesTable({ chain, wallet, token }: { chain: Chain; wallet: string; token?: string }) {
  const now = useNow();
  const trades = useInfiniteQuery({
    queryKey: ["wallet-trades", chain.id, wallet, token],
    queryFn: ({ pageParam }) => api.walletTrades(chain.id, wallet, { limit: 50, token, cursor: pageParam }),
    initialPageParam: undefined as WalletTradeCursor | undefined,
    getNextPageParam: (page) => page.next,
    refetchInterval: 30_000,
  });
  const rows = trades.data?.pages.flatMap((page) => page.trades) ?? [];
  return (
    <div className="table-scroll">
      <table className="table trades">
        <thead>
          <tr>
            <th>时间</th>
            <th>代币</th>
            <th>操作</th>
            <th className="num">数量</th>
            <th className="num">价格</th>
            <th className="num">价值</th>
            <th className="num">已实现</th>
            <th className="num">持仓时长</th>
            <th>交易</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((trade) => (
            <tr key={`${trade.block_height}:${trade.log_index}:${trade.operation_index}:${trade.transaction_hash}`}>
              <td className="muted" title={`#${trade.block_height}`}>
                {formatAge(trade.block_time, now)}
              </td>
              <td>
                <Link to={`/${chain.id}/token/${trade.token}`}>
                  <TokenSymbol chain={chain} token={trade.token} />
                </Link>
              </td>
              <td className={`side ${trade.side}`}>{EVENT_TEXT[trade.event] ?? trade.event}</td>
              <td className="num">{formatCompact(trade.amount)}</td>
              <td className="num mono">${formatPrice(trade.price_usd)}</td>
              <td className={`num side ${trade.side}`}>{formatUsd(trade.value_usd)}</td>
              <td className={`num ${trade.counted ? pnlClass(trade.realized_usd) : "muted"}`}>
                {trade.counted ? formatPnl(trade.realized_usd) : "—"}
              </td>
              <td className="num muted">{trade.event === "close" ? formatDuration(trade.hold_seconds) : "—"}</td>
              <td>
                <Address chain={chain} value={trade.transaction_hash} kind="tx" label={`${trade.transaction_hash.slice(0, 10)}…`} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <Status loading={trades.isLoading} error={trades.error} empty={!trades.isLoading && rows.length === 0}>
        还没有成交记录
      </Status>
      {trades.hasNextPage && (
        <div className="pager">
          <button disabled={trades.isFetchingNextPage} onClick={() => void trades.fetchNextPage()}>
            {trades.isFetchingNextPage ? "加载中…" : "加载更多"}
          </button>
        </div>
      )}
    </div>
  );
}

// TradersTable is a token's profit leaderboard: its traders by realized PnL.
export function TradersTable({ chain, token }: { chain: Chain; token: string }) {
  const now = useNow(10_000);
  const traders = useQuery({
    queryKey: ["token-traders", chain.id, token],
    queryFn: () => api.tokenTraders(chain.id, token, 100),
    refetchInterval: 30_000,
  });
  const rows = traders.data ?? [];
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th className="rank">#</th>
            <th>交易者</th>
            <th className="num">已实现</th>
            <th className="num">未实现</th>
            <th className="num">买入</th>
            <th className="num">卖出</th>
            <th className="num">持仓</th>
            <th className="num">最近交易</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((trader, index) => (
            <TraderRow key={trader.wallet} chain={chain} trader={trader} rank={index + 1} now={now} />
          ))}
        </tbody>
      </table>
      <Status loading={traders.isLoading} error={traders.error} empty={!traders.isLoading && rows.length === 0}>
        还没有交易者数据，market-wallet 记账后会出现在这里
      </Status>
    </div>
  );
}

function TraderRow({ chain, trader, rank, now }: { chain: Chain; trader: Trader; rank: number; now: number }) {
  return (
    <tr>
      <td className="rank muted">{rank}</td>
      <td>
        <Wallet chain={chain} wallet={trader.wallet} labels={trader.labels} />
      </td>
      <td className={`num ${pnlClass(trader.realized_usd)}`}>{formatPnl(trader.realized_usd)}</td>
      <td className={`num ${pnlClass(trader.unrealized_usd)}`}>{trader.status === "open" ? formatPnl(trader.unrealized_usd) : "—"}</td>
      <td className="num">
        {formatUsd(trader.bought_usd)} <span className="muted">· {trader.buys}</span>
      </td>
      <td className="num">
        {formatUsd(trader.sold_usd)} <span className="muted">· {trader.sells}</span>
      </td>
      <td className="num">{trader.status === "open" ? formatCompact(trader.quantity) : <span className="muted">已清仓</span>}</td>
      <td className="num muted">{formatAge(trader.last_trade_time, now)}</td>
    </tr>
  );
}
