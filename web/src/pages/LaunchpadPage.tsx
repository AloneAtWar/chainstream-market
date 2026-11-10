import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { CopyButton, ProgressBar, Status, Tabs, TokenAvatar } from "../components/common";
import { api, type LaunchpadColumn, type LaunchpadToken } from "../lib/api";
import type { Chain } from "../lib/chains";
import { formatAge, formatInteger, formatPrice, formatProgress, formatUsd, shortAddress } from "../lib/format";
import { useNow } from "../lib/hooks";
import { formatRaised, progressBasis, protocolLabel, unlistedProtocol } from "../lib/launchpad";

const COLUMNS: { value: LaunchpadColumn; title: string; hint: string }[] = [
  { value: "new", title: "🆕 新币", hint: "按发射时间倒序" },
  { value: "bonding", title: "🚀 发射中", hint: "按进度倒序，1 小时无成交即退出" },
  { value: "migrated", title: "✅ 已迁移", hint: "按迁移时间倒序" },
];

const PAGE = 50;
const POLL = 5_000;
// PROTOCOL_POLL refreshes a non-empty platform list; an empty one polls with
// the columns, and a column holding a token of an unlisted platform refreshes
// it at once.
const PROTOCOL_POLL = 60_000;

// LaunchpadPage shows launchpad tokens in three columns: new launches,
// curves filling up and curves that moved to a DEX. market-api has no live
// channel for them yet, so each column polls.
export function LaunchpadPage({ chain }: { chain: Chain }) {
  const [params, setParams] = useSearchParams();
  const listed = useQuery({
    queryKey: ["launchpad-protocols", chain.id],
    queryFn: () => api.launchpadProtocols(chain.id),
    refetchInterval: (query) => (query.state.data?.length ? PROTOCOL_POLL : POLL),
  });
  const protocols = listed.data ?? [];
  const requested = params.get("protocol") ?? "";
  const protocol = protocols.includes(requested) ? requested : "";
  const now = useNow(1000);

  const options = [{ value: "", label: "全部" }, ...protocols.map((value) => ({ value, label: protocolLabel(value) }))];
  return (
    <div className="page">
      <div className="toolbar">
        <Tabs
          value={protocol}
          options={options}
          onChange={(value) => {
            const next = new URLSearchParams(params);
            if (value) next.set("protocol", value);
            else next.delete("protocol");
            setParams(next, { replace: true });
          }}
        />
        <span className="muted toolbar-right">
          {listed.isSuccess && protocols.length === 0 ? `${chain.name} 上还没有发射平台代币` : "每 5 秒刷新"}
        </span>
      </div>
      <div className="launchpad-columns">
        {COLUMNS.map((column) => (
          <LaunchpadColumnCard
            key={column.value}
            chain={chain}
            column={column}
            protocol={protocol}
            protocols={listed.isSuccess ? protocols : null}
            now={now}
          />
        ))}
      </div>
    </div>
  );
}

function LaunchpadColumnCard({
  chain,
  column,
  protocol,
  protocols,
  now,
}: {
  chain: Chain;
  column: (typeof COLUMNS)[number];
  protocol: string;
  protocols: string[] | null;
  now: number;
}) {
  const navigate = useNavigate();
  const client = useQueryClient();
  const list = useQuery({
    queryKey: ["launchpad", chain.id, column.value, protocol],
    queryFn: () => api.launchpadList(chain.id, column.value, { protocol, limit: PAGE }),
    placeholderData: keepPreviousData,
    refetchInterval: POLL,
  });
  const tokens = list.data?.tokens ?? [];
  const stale = protocols !== null && unlistedProtocol(tokens, protocols);
  useEffect(() => {
    if (stale) void client.invalidateQueries({ queryKey: ["launchpad-protocols", chain.id] });
  }, [stale, client, chain.id]);
  return (
    <section className="card launchpad-column">
      <div className="card-head">
        <h3>{column.title}</h3>
        <span className="muted launchpad-hint">{column.hint}</span>
      </div>
      <div className="launchpad-list">
        {tokens.map((token) => (
          <LaunchpadCard
            key={token.token}
            chain={chain}
            token={token}
            column={column.value}
            now={now}
            onOpen={() => navigate(`/${chain.id}/token/${token.token}`)}
          />
        ))}
        <Status loading={list.isLoading} error={list.error} empty={!list.isLoading && !list.error && tokens.length === 0}>
          暂无代币
        </Status>
      </div>
    </section>
  );
}

function LaunchpadCard({
  chain,
  token,
  column,
  now,
  onOpen,
}: {
  chain: Chain;
  token: LaunchpadToken;
  column: LaunchpadColumn;
  now: number;
  onOpen: () => void;
}) {
  // A token seen trading before its launch block has only an approximate
  // launch time, so its age shows only once the launch was seen.
  const age = column === "migrated" ? token.migrated_time : token.launch_seen ? token.launch_time : null;
  return (
    <article className="launchpad-card clickable" onClick={onOpen}>
      <div className="launchpad-top">
        <TokenAvatar symbol={token.symbol} address={token.token} size={36} />
        <div className="launchpad-title">
          <div className="token-name">
            <Link to={`/${chain.id}/token/${token.token}`} onClick={(event) => event.stopPropagation()}>
              {token.symbol ?? shortAddress(token.token)}
            </Link>
            <span className="badge">{protocolLabel(token.protocol)}</span>
            {token.status === "completed" && <span className="badge warn">待迁移</span>}
            {token.direct && <span className="badge">直发</span>}
          </div>
          <div className="token-sub muted" onClick={(event) => event.stopPropagation()}>
            {token.name && <span className="ellipsis">{token.name}</span>}
            <span className="mono">{shortAddress(token.token, 4, 4)}</span>
            <CopyButton value={token.token} />
          </div>
        </div>
        <span className="muted launchpad-age" title={column === "migrated" ? "迁移于" : "发射于"}>
          {formatAge(age, now)}
        </span>
      </div>
      <div className="launchpad-progress" title={progressBasis(token)}>
        <ProgressBar fraction={token.progress} done={token.status !== "bonding"} />
        <span className="mono">{formatProgress(token.progress)}</span>
      </div>
      <div className="launchpad-stats muted">
        <span title="曲线已募集">💰 {formatRaised(chain, token)}</span>
        <span title="价格">{token.price_usd === null ? "—" : `$${formatPrice(token.price_usd)}`}</span>
        <span title="24h 成交额">Vol {formatUsd(token.volume_24h_usd)}</span>
        <span title={token.direct ? "成交 买/卖" : "曲线成交 买/卖"}>
          <span className="up">{formatInteger(token.buys)}</span>/<span className="down">{formatInteger(token.sells)}</span>
        </span>
        {token.holders !== null && <span title="持有人">👥 {formatInteger(token.holders)}</span>}
      </div>
    </article>
  );
}
