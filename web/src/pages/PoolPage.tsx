import { useQuery } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { CandleChart } from "../components/CandleChart";
import { CopyButton, Info, Status, Tabs, TokenAvatar } from "../components/common";
import { PairName, TokenSymbol } from "../components/tables";
import { TradesTable } from "../components/TradesTable";
import { api } from "../lib/api";
import { INTERVALS, type Chain, type Interval } from "../lib/chains";
import { formatAge, formatInteger, formatPrice } from "../lib/format";
import { useLiveCandles, useLiveTrades, useNow } from "../lib/hooks";

export function PoolPage({ chain }: { chain: Chain }) {
  const poolId = useParams().pool!.toLowerCase();
  const [params, setParams] = useSearchParams();
  const interval = (INTERVALS as readonly string[]).includes(params.get("interval") ?? "")
    ? (params.get("interval") as Interval)
    : "1m";
  const now = useNow(5000);
  const pool = useQuery({ queryKey: ["pool", chain.id, poolId], queryFn: () => api.pool(chain.id, poolId) });
  const candles = useLiveCandles(chain.id, "pool", poolId, interval);
  const trades = useLiveTrades(chain.id, "pool", poolId);
  const last = candles.data[candles.data.length - 1];

  return (
    <div className="page token-page">
      <section className="token-header card">
        <div className="token-title">
          <TokenAvatar symbol={pool.data?.protocol ?? "P"} address={poolId} size={44} />
          <div>
            <h1>
              {pool.data ? <PairName chain={chain} pool={pool.data} /> : "池子"}
              {pool.data && <span className="badge">{pool.data.protocol}</span>}
            </h1>
            <div className="muted token-address">
              <span className="mono">{poolId}</span>
              <CopyButton value={poolId} />
            </div>
          </div>
        </div>
        <div className="token-price">
          <div className="price-big mono">{last ? `$${formatPrice(last.close)}` : "—"}</div>
          <div className="muted">基础代币 USD 价格</div>
        </div>
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
              <span className="muted">这个池子的成交价（含异常成交）</span>
            </div>
            {candles.error ? (
              <div className="status error">K 线加载失败：{candles.error.message}</div>
            ) : (
              <CandleChart candles={candles.data} interval={interval} onNeedOlder={() => void candles.loadOlder()} />
            )}
          </section>
          <section className="card">
            <div className="card-head">
              <h3>实时成交</h3>
            </div>
            <TradesTable chain={chain} trades={trades.data} loading={trades.loading} error={trades.error} />
          </section>
        </div>
        <aside className="token-side">
          <section className="card info">
            <h3>池子信息</h3>
            <Status loading={pool.isLoading} error={pool.error} />
            {pool.data === null && <p className="muted">market-registry 还没有收录这个池子。</p>}
            {pool.data && (
              <dl>
                <Info label="协议" value={pool.data.protocol} />
                <Info label="DEX" value={pool.data.dex_address ? <span className="mono">{pool.data.dex_address.slice(0, 10)}…</span> : "—"} />
                {pool.data.tokens.map((token, index) => (
                  <Info
                    key={token}
                    label={`代币 ${index + 1}`}
                    value={
                      <Link to={`/${chain.id}/token/${token}`}>
                        <TokenSymbol chain={chain} token={token} />
                      </Link>
                    }
                  />
                ))}
                <Info label="新建池" value={pool.data.created ? "是" : "否"} />
                <Info label="首次出现" value={`#${formatInteger(pool.data.first_height)}`} />
                <Info label="元数据更新" value={pool.data.source_time ? `${formatAge(pool.data.source_time, now)} 前` : `#${pool.data.source_height}`} />
              </dl>
            )}
            {pool.data?.metadata !== undefined && (
              <details className="raw">
                <summary>原始元数据</summary>
                <pre>{JSON.stringify(pool.data.metadata, null, 2)}</pre>
              </details>
            )}
          </section>
        </aside>
      </div>
    </div>
  );
}
