import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { CandleChart } from "../components/CandleChart";
import { Address, CopyButton, Info, Stat, Tabs, TokenAvatar } from "../components/common";
import { HoldersTable, PoolsTable, TransfersTable } from "../components/tables";
import { TradesTable } from "../components/TradesTable";
import { api } from "../lib/api";
import { canonicalToken, INTERVALS, type Chain, type Interval } from "../lib/chains";
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
import { useLiveCandles, useLiveTrades, useNow, useTokenMeta } from "../lib/hooks";

type Panel = "trades" | "holders" | "pools" | "transfers";

const PANELS: { value: Panel; label: string }[] = [
  { value: "trades", label: "实时成交" },
  { value: "holders", label: "持有人" },
  { value: "pools", label: "池子" },
  { value: "transfers", label: "转账" },
];

export function TokenPage({ chain }: { chain: Chain }) {
  const address = useParams().address!.toLowerCase();
  // Market data lives under the wrapped token; holders and the registry
  // keep the address as given.
  const market = canonicalToken(chain, address);
  const [params, setParams] = useSearchParams();
  const interval = (INTERVALS as readonly string[]).includes(params.get("interval") ?? "")
    ? (params.get("interval") as Interval)
    : "1m";
  const [panel, setPanel] = useState<Panel>("trades");
  const now = useNow(5000);

  const meta = useTokenMeta(chain.id, address);
  const stats = useQuery({
    queryKey: ["token-stats", chain.id, address],
    queryFn: () => api.tokenList(chain.id, { q: address, limit: 1 }).then((list) => list.tokens[0] ?? null),
    refetchInterval: 15_000,
  });
  const price = useQuery({
    queryKey: ["price", chain.id, market],
    queryFn: () => api.price(chain.id, market),
    refetchInterval: 15_000,
  });
  const candles = useLiveCandles(chain.id, "token", market, interval);
  const trades = useLiveTrades(chain.id, "token", market);

  // The newest priced swap beats the polled price when it is more recent.
  const lastSwap = trades.data.find((trade) => (trade.side === "buy" || trade.side === "sell") && !trade.outlier);
  const currentPrice =
    lastSwap && (!price.data || lastSwap.block_height >= price.data.block_height) ? lastSwap.price_usd : price.data?.price_usd;

  const symbol = meta.data?.symbol ?? stats.data?.symbol ?? null;
  const name = meta.data?.name ?? stats.data?.name ?? null;
  const ranking = stats.data;

  return (
    <div className="page token-page">
      <section className="token-header card">
        <div className="token-title">
          <TokenAvatar symbol={symbol} address={address} size={44} />
          <div>
            <h1>
              {symbol ?? shortAddress(address)}
              {name && <span className="muted token-fullname">{name}</span>}
            </h1>
            <div className="muted token-address">
              <span className="mono">{address}</span>
              <CopyButton value={address} />
              <a href={`${chain.explorer}/token/${address}`} target="_blank" rel="noreferrer" className="icon-link">
                ↗
              </a>
            </div>
          </div>
        </div>
        <div className="token-price">
          <div className="price-big mono">{currentPrice ? `$${formatPrice(currentPrice)}` : "—"}</div>
          {ranking && <div className={`price-change ${changeClass(ranking.price_change_24h)}`}>{formatChange(ranking.price_change_24h)} 24h</div>}
        </div>
        <dl className="stats">
          <Stat label="24h 成交额" value={formatUsd(ranking?.volume_24h_usd)} />
          <Stat label="24h 笔数" value={formatCompact(ranking?.trades_24h ?? null, 1)} />
          <Stat label="流动性" value={formatUsd(price.data?.liquidity_usd ?? ranking?.liquidity_usd)} />
          <Stat label="持有人" value={formatInteger(ranking?.holders)} />
          <Stat label="创建" value={ranking?.created_time ? `${formatAge(ranking.created_time, now)} 前` : "—"} />
        </dl>
      </section>

      <div className="token-grid">
        <div className="token-main">
          <section className="card chart-card">
            <div className="card-head">
              <Tabs
                small
                value={interval}
                options={INTERVALS.map((value) => ({ value, label: value }))}
                onChange={(value) => {
                  const next = new URLSearchParams(params);
                  if (value === "1m") next.delete("interval");
                  else next.set("interval", value);
                  setParams(next, { replace: true });
                }}
              />
              <span className="muted">USD 价格 · 聚合所有流动性达标的池子</span>
            </div>
            {candles.error ? (
              <div className="status error">K 线加载失败：{candles.error.message}</div>
            ) : (
              <CandleChart candles={candles.data} interval={interval} onNeedOlder={() => void candles.loadOlder()} />
            )}
          </section>

          <section className="card">
            <div className="card-head">
              <Tabs value={panel} options={PANELS} onChange={setPanel} />
            </div>
            {panel === "trades" && <TradesTable chain={chain} trades={trades.data} loading={trades.loading} error={trades.error} showPool />}
            {panel === "holders" && <HoldersTable chain={chain} token={address} />}
            {panel === "pools" && <PoolsTable chain={chain} token={address} />}
            {panel === "transfers" && <TransfersTable chain={chain} subject="token" value={address} />}
          </section>
        </div>

        <aside className="token-side">
          <section className="card info">
            <h3>代币信息</h3>
            {meta.data === null && <p className="muted">market-registry 还没有收录这个代币。</p>}
            {meta.data && (
              <dl>
                <Info label="标准" value={meta.data.standard || "—"} />
                <Info label="识别" value={meta.data.recognition || "—"} />
                <Info label="精度" value={meta.data.decimals ?? "未知"} />
                <Info label="元数据完整" value={meta.data.metadata_complete ? "是" : "否"} />
                <Info label="首次出现" value={`#${formatInteger(meta.data.first_height)}`} />
                {meta.data.deployment && (
                  <>
                    <Info label="部署区块" value={`#${formatInteger(meta.data.deployment.block_height)}`} />
                    <Info
                      label="部署交易"
                      value={<Address chain={chain} value={meta.data.deployment.transaction_hash} kind="tx" label={shortAddress(meta.data.deployment.transaction_hash)} />}
                    />
                  </>
                )}
              </dl>
            )}
          </section>
          <section className="card info">
            <h3>定价</h3>
            {price.data === null && <p className="muted">还没有价格。</p>}
            {price.data && (
              <dl>
                <Info label="来源" value={price.data.source === "anchor" ? "锚定（稳定币/预言机）" : price.data.source === "graph" ? "价格图" : price.data.source} />
                <Info label="跳数" value={price.data.hops} />
                <Info label="定价池流动性" value={formatUsd(price.data.liquidity_usd)} />
                <Info label="价格区块" value={`#${formatInteger(price.data.block_height)}`} />
                <Info label="价格时间" value={`${formatAge(price.data.block_time, now)} 前`} />
              </dl>
            )}
          </section>
        </aside>
      </div>
    </div>
  );
}
