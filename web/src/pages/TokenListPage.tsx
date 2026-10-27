import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { CopyButton, Status, Tabs, TokenAvatar } from "../components/common";
import { api, type RankedToken, type RankingSort } from "../lib/api";
import type { Chain } from "../lib/chains";
import {
  changeClass,
  formatAge,
  formatChange,
  formatCompact,
  formatInteger,
  formatPrice,
  formatUsd,
  shortAddress,
} from "../lib/format";
import { useNow } from "../lib/hooks";

const SORTS: { value: RankingSort; label: string }[] = [
  { value: "volume_24h", label: "🔥 成交额" },
  { value: "price_change_24h", label: "📈 涨幅" },
  { value: "liquidity", label: "💧 流动性" },
  { value: "holders", label: "👥 持有人" },
  { value: "created", label: "🆕 新币" },
];

const LIQUIDITY_FILTERS = [
  { value: "", label: "全部" },
  { value: "1000", label: "≥ $1K" },
  { value: "10000", label: "≥ $10K" },
  { value: "100000", label: "≥ $100K" },
] as const;

const PAGE = 100;

// Column keys the API can sort by; the others are display only.
const SORTABLE: Partial<Record<string, RankingSort>> = {
  change: "price_change_24h",
  volume: "volume_24h",
  liquidity: "liquidity",
  holders: "holders",
  age: "created",
};

export function TokenListPage({ chain }: { chain: Chain }) {
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const now = useNow(5000);
  const sort = (params.get("sort") as RankingSort | null) ?? "volume_24h";
  const order = params.get("order") === "asc" ? "asc" : "desc";
  const q = params.get("q") ?? "";
  const minLiquidity = params.get("min_liquidity") ?? "";
  const page = Math.max(0, Number(params.get("page") ?? 0) || 0);

  const set = (changes: Record<string, string | null>) => {
    const next = new URLSearchParams(params);
    for (const [key, value] of Object.entries(changes)) {
      if (value === null || value === "") next.delete(key);
      else next.set(key, value);
    }
    if (!("page" in changes)) next.delete("page");
    setParams(next, { replace: true });
  };

  const list = useQuery({
    queryKey: ["tokens", chain.id, sort, order, q, minLiquidity, page],
    queryFn: () =>
      api.tokenList(chain.id, { sort, order, q, minLiquidityUsd: minLiquidity, limit: PAGE, offset: page * PAGE }),
    placeholderData: keepPreviousData,
    refetchInterval: 15_000,
  });

  const toggleSort = (column: string) => {
    const target = SORTABLE[column];
    if (!target) return;
    if (target === sort) set({ order: order === "desc" ? "asc" : null });
    else set({ sort: target === "volume_24h" ? null : target, order: null });
  };
  const arrow = (column: string) => (SORTABLE[column] === sort ? (order === "desc" ? " ↓" : " ↑") : "");
  const header = (column: string, label: string) => (
    <th className={SORTABLE[column] ? "sortable num" : "num"} onClick={() => toggleSort(column)}>
      {label}
      {arrow(column)}
    </th>
  );

  const tokens = list.data?.tokens ?? [];
  const refreshed = list.data?.refreshed_at;

  return (
    <div className="page">
      <div className="toolbar">
        <Tabs value={sort} options={SORTS} onChange={(value) => set({ sort: value === "volume_24h" ? null : value, order: null })} />
        <div className="toolbar-right">
          <label className="filter">
            流动性
            <select value={minLiquidity} onChange={(event) => set({ min_liquidity: event.target.value })}>
              {LIQUIDITY_FILTERS.map((filter) => (
                <option key={filter.value} value={filter.value}>
                  {filter.label}
                </option>
              ))}
            </select>
          </label>
          {refreshed && <span className="muted">排行更新于 {formatAge(refreshed, now)} 前</span>}
        </div>
      </div>
      {q && (
        <div className="search-banner">
          搜索 “<b>{q}</b>” 的结果
          <button className="link-button" onClick={() => set({ q: null })}>
            清除
          </button>
        </div>
      )}
      <div className="card table-card">
        <table className="table tokens">
          <thead>
            <tr>
              <th className="rank">#</th>
              <th>代币</th>
              <th className="num">价格</th>
              {header("change", "24h 涨跌")}
              {header("volume", "24h 成交额")}
              <th className="num">24h 笔数</th>
              {header("liquidity", "流动性")}
              {header("holders", "持有人")}
              {header("age", "创建")}
            </tr>
          </thead>
          <tbody>
            {tokens.map((token, index) => (
              <TokenRow
                key={token.token}
                chain={chain}
                token={token}
                rank={page * PAGE + index + 1}
                now={now}
                onOpen={() => navigate(`/${chain.id}/token/${token.token}`)}
              />
            ))}
          </tbody>
        </table>
        <Status
          loading={list.isLoading}
          error={list.error}
          empty={!list.isLoading && !list.error && tokens.length === 0}
        >
          {q ? "没有匹配的代币" : "还没有排行数据，market-rankings 刷新后会出现在这里"}
        </Status>
      </div>
      {(page > 0 || tokens.length === PAGE) && (
        <div className="pager">
          <button disabled={page === 0} onClick={() => set({ page: page > 1 ? String(page - 1) : null })}>
            上一页
          </button>
          <span className="muted">第 {page + 1} 页</span>
          <button disabled={tokens.length < PAGE} onClick={() => set({ page: String(page + 1) })}>
            下一页
          </button>
        </div>
      )}
    </div>
  );
}

function TokenRow({
  chain,
  token,
  rank,
  now,
  onOpen,
}: {
  chain: Chain;
  token: RankedToken;
  rank: number;
  now: number;
  onOpen: () => void;
}) {
  return (
    <tr className="clickable" onClick={onOpen}>
      <td className="rank muted">{rank}</td>
      <td>
        <div className="token-cell">
          <TokenAvatar symbol={token.symbol} address={token.token} />
          <div>
            <div className="token-name">
              <Link to={`/${chain.id}/token/${token.token}`} onClick={(event) => event.stopPropagation()}>
                {token.symbol ?? shortAddress(token.token)}
              </Link>
              {!token.ranked && <span className="badge">无行情</span>}
            </div>
            <div className="token-sub muted" onClick={(event) => event.stopPropagation()}>
              {token.name && <span className="ellipsis">{token.name}</span>}
              <span className="mono">{shortAddress(token.token, 4, 4)}</span>
              <CopyButton value={token.token} />
            </div>
          </div>
        </div>
      </td>
      <td className="num mono">{token.price_usd === null ? "—" : `$${formatPrice(token.price_usd)}`}</td>
      <td className={`num ${changeClass(token.price_change_24h)}`}>{token.ranked ? formatChange(token.price_change_24h) : "—"}</td>
      <td className="num">{formatUsd(token.volume_24h_usd)}</td>
      <td className="num">{formatCompact(token.trades_24h, 1)}</td>
      <td className="num">{formatUsd(token.liquidity_usd)}</td>
      <td className="num">{formatInteger(token.holders)}</td>
      <td className="num muted">{formatAge(token.created_time, now)}</td>
    </tr>
  );
}
