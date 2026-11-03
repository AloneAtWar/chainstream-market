import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { Status, Tabs } from "../components/common";
import { TokenSymbol } from "../components/tables";
import { RoiBar, Wallet } from "../components/wallets";
import { api, type RankedWallet, type Trade, type WalletSort } from "../lib/api";
import type { Chain } from "../lib/chains";
import { channels } from "../lib/live";
import {
  formatAge,
  formatDuration,
  formatInteger,
  formatPercent,
  formatPnl,
  formatUsd,
  pnlClass,
  shortAddress,
} from "../lib/format";
import { useLiveFeed, useNow } from "../lib/hooks";

type Scope = "smart_money" | "all";

const SCOPES: { value: Scope; label: string }[] = [
  { value: "smart_money", label: "🧠 聪明钱" },
  { value: "all", label: "全部钱包" },
];

const WINDOWS = [
  { value: "7", label: "7 天" },
  { value: "30", label: "30 天" },
  { value: "90", label: "90 天" },
] as const;
type Window = (typeof WINDOWS)[number]["value"];

const SORTS: { value: WalletSort; label: string }[] = [
  { value: "realized", label: "已实现盈亏" },
  { value: "roi", label: "ROI" },
  { value: "winrate", label: "胜率" },
  { value: "volume", label: "成交额" },
];

const LIMIT = 200;
const FEED_LIMIT = 60;

// SmartMoneyPage ranks wallets by their market-wallet stats over a window
// and streams what the smart money wallets trade right now.
export function SmartMoneyPage({ chain }: { chain: Chain }) {
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const now = useNow(5000);
  const scope: Scope = params.get("scope") === "all" ? "all" : "smart_money";
  const window: Window = (WINDOWS.find((item) => item.value === params.get("window"))?.value ?? "30") as Window;
  const sort: WalletSort = SORTS.find((item) => item.value === params.get("sort"))?.value ?? "realized";

  const set = (changes: Record<string, string | null>) => {
    const next = new URLSearchParams(params);
    for (const [key, value] of Object.entries(changes)) {
      if (value === null) next.delete(key);
      else next.set(key, value);
    }
    setParams(next, { replace: true });
  };

  const list = useQuery({
    queryKey: ["wallets", chain.id, scope, window, sort],
    queryFn: () =>
      api.walletList(chain.id, {
        window: Number(window),
        sort,
        label: scope === "all" ? undefined : scope,
        limit: LIMIT,
      }),
    placeholderData: keepPreviousData,
    refetchInterval: 60_000,
  });
  const wallets = list.data?.wallets ?? [];
  const refreshed = list.data?.refreshed_at;

  return (
    <div className="page smart-page">
      <div className="toolbar">
        <Tabs value={scope} options={SCOPES} onChange={(value) => set({ scope: value === "smart_money" ? null : value })} />
        <div className="toolbar-right">
          <Tabs small value={window} options={WINDOWS} onChange={(value) => set({ window: value === "30" ? null : value })} />
          <Tabs small value={sort} options={SORTS} onChange={(value) => set({ sort: value === "realized" ? null : value })} />
        </div>
      </div>
      <div className="smart-grid">
        <div className="card table-card">
          <div className="card-head">
            <h3>{scope === "smart_money" ? "聪明钱榜" : "钱包排行"}</h3>
            <span className="muted">
              {window} 天窗口
              {refreshed ? ` · 统计更新于 ${formatAge(refreshed, now)} 前` : ""}
            </span>
          </div>
          <div className="table-scroll">
            <table className="table wallets">
              <thead>
                <tr>
                  <th className="rank">#</th>
                  <th>钱包</th>
                  <th className="num">已实现盈亏</th>
                  <th className="num">ROI</th>
                  <th className="num">胜率</th>
                  <th className="num">代币</th>
                  <th className="roi-col">收益分布</th>
                  <th className="num">成交额</th>
                  <th className="num">笔数</th>
                  <th className="num">平均持仓</th>
                </tr>
              </thead>
              <tbody>
                {wallets.map((wallet, index) => (
                  <WalletRow
                    key={wallet.wallet}
                    chain={chain}
                    wallet={wallet}
                    rank={index + 1}
                    onOpen={() => navigate(`/${chain.id}/wallet/${wallet.wallet}`)}
                  />
                ))}
              </tbody>
            </table>
            <Status loading={list.isLoading} error={list.error} empty={!list.isLoading && !list.error && wallets.length === 0}>
              {scope === "smart_money" ? "还没有符合规则的聪明钱，market-wallet 汇总后会出现在这里" : "还没有钱包统计"}
            </Status>
          </div>
        </div>
        <SmartMoneyFeed chain={chain} now={now} />
      </div>
    </div>
  );
}

function WalletRow({ chain, wallet, rank, onOpen }: { chain: Chain; wallet: RankedWallet; rank: number; onOpen: () => void }) {
  return (
    <tr className="clickable" onClick={onOpen}>
      <td className="rank muted">{rank}</td>
      <td onClick={(event) => event.stopPropagation()}>
        <Wallet chain={chain} wallet={wallet.wallet} labels={wallet.labels} />
      </td>
      <td className={`num ${pnlClass(wallet.realized_usd)}`}>{formatPnl(wallet.realized_usd)}</td>
      <td className={`num ${pnlClass(wallet.roi)}`}>{formatPercent(wallet.roi)}</td>
      <td className="num" title={`${wallet.wins} / ${wallet.counted_sells} 笔计入盈亏的卖出`}>
        {wallet.counted_sells > 0 ? formatPercent(wallet.win_rate) : "—"}
      </td>
      <td className="num">{formatInteger(wallet.tokens)}</td>
      <td className="roi-col">
        <RoiBar distribution={wallet.roi_distribution} />
      </td>
      <td className="num">{formatUsd(Number(wallet.bought_usd) + Number(wallet.sold_usd))}</td>
      <td className="num muted">
        <span className="up">{formatInteger(wallet.buys)}</span> / <span className="down">{formatInteger(wallet.sells)}</span>
      </td>
      <td className="num muted">{wallet.closes > 0 ? formatDuration(wallet.average_hold_seconds) : "—"}</td>
    </tr>
  );
}

// SmartMoneyFeed streams the trades of every smart money wallet as they
// happen; it starts empty, since the API keeps no history of the channel.
function SmartMoneyFeed({ chain, now }: { chain: Chain; now: number }) {
  const trades = useLiveFeed(channels.labelTrades(chain.id, "smart_money"), FEED_LIMIT);
  return (
    <aside className="card feed">
      <div className="card-head">
        <h3>聪明钱实时动向</h3>
        <span className="muted">实时推送</span>
      </div>
      <ul className="feed-list">
        {trades.map((trade) => (
          <FeedItem key={`${trade.block_height}:${trade.transaction_hash}:${trade.log_index}:${trade.operation_index}`} chain={chain} trade={trade} now={now} />
        ))}
      </ul>
      {trades.length === 0 && <div className="status">等待聪明钱的下一笔成交…</div>}
    </aside>
  );
}

function FeedItem({ chain, trade, now }: { chain: Chain; trade: Trade; now: number }) {
  const buy = trade.side === "buy";
  return (
    <li className="feed-item">
      <div className="feed-line">
        <Link to={`/${chain.id}/wallet/${trade.trader}`} className="mono">
          {shortAddress(trade.trader, 6, 4)}
        </Link>
        <span className={`side ${trade.side}`}>{buy ? "买入" : trade.side === "sell" ? "卖出" : trade.side}</span>
        <Link to={`/${chain.id}/token/${trade.base_token}`} className="feed-token">
          <TokenSymbol chain={chain} token={trade.base_token} />
        </Link>
        <span className={`num side ${trade.side}`}>{formatUsd(trade.value_usd)}</span>
      </div>
      <div className="muted feed-meta">
        {formatAge(trade.block_time, now)} 前 · #{formatInteger(trade.block_height)}
      </div>
    </li>
  );
}
