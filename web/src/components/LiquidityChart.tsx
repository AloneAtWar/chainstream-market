import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { api, type LiquiditySegment, type PoolLiquidity, type TokenAmount } from "../lib/api";
import { canonicalToken, NATIVE_ADDRESS, type Chain } from "../lib/chains";
import { formatCompact, formatInteger, formatPrice } from "../lib/format";
import { useTokenSymbol } from "../lib/hooks";
import {
  axisTicks,
  barAt,
  currentTick,
  layout,
  makeAxis,
  pickZoom,
  priceAt,
  segmentRange,
  toU,
  windowFor,
  ZOOMS,
  type Zoom,
} from "../lib/liquidity";
import { Tabs } from "./common";

const HEIGHT = 220;
const AXIS = 22;
const TOP = 12;
// Ranges above the price hold only the priced (base) token and ranges below
// only the quote token, whichever way round the pool orders them. The pair
// is validated against the panel surface (CVD ΔE 15.9).
const BASE_COLOR = "#3987e5";
const QUOTE_COLOR = "#d55181";

function amountText(amount: TokenAmount | undefined): string {
  if (!amount) return "—";
  return amount.value !== null ? formatCompact(amount.value) : `${formatCompact(amount.raw)} (原始)`;
}

// liquidityText writes a raw L, which runs to 1e30 and beyond for tokens
// with 18 decimals.
function liquidityText(raw: string): string {
  const value = Number(raw);
  return value >= 1e15 ? value.toExponential(2) : formatCompact(value);
}

// useWidth tracks an element's width.
function useWidth() {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    setWidth(element.clientWidth);
    const observer = new ResizeObserver(() => setWidth(element.clientWidth));
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  return [ref, width] as const;
}

// LiquidityCard shows a concentrated (V3/V4-style) pool's liquidity across
// prices, the way Uniswap draws it. It renders nothing for other pools.
// baseToken, when known, is the token the pool's trades price, which the
// chart prices too.
export function LiquidityCard({ chain, poolId, baseToken }: { chain: Chain; poolId: string; baseToken?: string }) {
  const liquidity = useQuery({
    queryKey: ["liquidity", chain.id, poolId],
    queryFn: () => api.poolLiquidity(chain.id, poolId),
    refetchInterval: 15_000,
  });
  if (liquidity.data === null) return null;
  return (
    <section className="card liquidity-card">
      {liquidity.error ? (
        <>
          <div className="card-head">
            <h3>流动性分布</h3>
          </div>
          <div className="status error">流动性分布加载失败：{liquidity.error.message}</div>
        </>
      ) : !liquidity.data ? (
        <>
          <div className="card-head">
            <h3>流动性分布</h3>
          </div>
          <div className="status">加载中…</div>
        </>
      ) : (
        <LiquidityBody chain={chain} data={liquidity.data} baseToken={baseToken} />
      )}
    </section>
  );
}

function LiquidityBody({ chain, data, baseToken }: { chain: Chain; data: PoolLiquidity; baseToken?: string }) {
  const tokens = data.tokens ?? [];
  const symbol0 = useTokenSymbol(chain.id, tokens[0] ?? NATIVE_ADDRESS);
  const symbol1 = useTokenSymbol(chain.id, tokens[1] ?? NATIVE_ADDRESS);
  const symbols: [string, string] = data.tokens ? [symbol0, symbol1] : ["token0", "token1"];
  const tick = currentTick(data.sqrt_price_x96);

  // By default the chart prices the token the trades price; failing that,
  // the token paired with the wrapped native coin.
  const [userFlipped, setUserFlipped] = useState<boolean | null>(null);
  const autoFlipped = useMemo(() => {
    const canonical = tokens.map((token) => canonicalToken(chain, token));
    if (baseToken) {
      const base = canonicalToken(chain, baseToken);
      if (canonical[1] === base) return true;
      if (canonical[0] === base) return false;
    }
    return canonical[0] === chain.wrappedNative;
  }, [chain, tokens, baseToken]);
  const flipped = userFlipped ?? autoFlipped;
  const axis = useMemo(() => makeAxis(flipped, data.decimals), [flipped, data.decimals]);
  const [base, quote] = flipped ? [symbols[1], symbols[0]] : [symbols[0], symbols[1]];

  const [userZoom, setUserZoom] = useState<Zoom | null>(null);
  const autoZoom = useMemo(() => (tick === null ? "all" : pickZoom(data.segments, tick)), [data.segments, tick]);
  const zoom = userZoom ?? autoZoom;

  const decimalsKnown = data.decimals?.[0] != null && data.decimals?.[1] != null;
  const warnings: string[] = [];
  if (data.stale) warnings.push("tick 表与链上不一致，等待下一次快照修正，分布仅供参考");
  else if (data.swap_delta_hooks) warnings.push("池子的 hook 可以在 tick 表之外结算，实际可成交量可能不同");
  if (!decimalsKnown) warnings.push("代币精度未知，价格按原始单位比值显示");

  return (
    <>
      <div className="card-head">
        <h3>
          流动性分布 <span className="badge">{data.protocol}</span>
          {data.fee > 0 && data.fee < 0x800000 && <span className="badge">{(data.fee / 10_000).toFixed(2)}%</span>}
          {data.fee >= 0x800000 && <span className="badge">动态费率</span>}
        </h3>
        <div className="liquidity-controls">
          <Tabs small value={zoom} options={ZOOMS.map((option) => ({ value: option.value, label: option.label }))} onChange={setUserZoom} />
          <button className="tab small-button" onClick={() => setUserFlipped(!flipped)} title="切换计价方向">
            {base}/{quote} ⇄
          </button>
        </div>
      </div>
      <dl className="stats liquidity-stats">
        <div className="stat">
          <dt>当前价</dt>
          <dd className="mono">
            {tick === null ? "—" : `1 ${base} = ${formatPrice(priceAt(axis, toU(axis, tick)))} ${quote}`}
          </dd>
        </div>
        <div className="stat">
          <dt>池内 {symbols[0]}</dt>
          <dd className="mono">{amountText(data.amounts[0])}</dd>
        </div>
        <div className="stat">
          <dt>池内 {symbols[1]}</dt>
          <dd className="mono">{amountText(data.amounts[1])}</dd>
        </div>
        <div className="stat">
          <dt>±{data.depth.band_bps / 100}% 深度</dt>
          <dd className="mono">
            {amountText(data.depth.amounts[0])} {symbols[0]} / {amountText(data.depth.amounts[1])} {symbols[1]}
          </dd>
        </div>
      </dl>
      {warnings.map((warning) => (
        <div key={warning} className="liquidity-warning">
          ⚠ {warning}
        </div>
      ))}
      {tick === null || data.segments.length === 0 ? (
        <div className="status">池子里没有流动性</div>
      ) : (
        <Plot data={data} axis={axis} tick={tick} zoom={zoom} symbols={symbols} quote={quote} />
      )}
      <div className="liquidity-foot muted">
        <span className="legend-item">
          <span className="swatch" style={{ background: BASE_COLOR }} />
          高于当前价：只含 {base}
        </span>
        <span className="legend-item">
          <span className="swatch" style={{ background: QUOTE_COLOR }} />
          低于当前价：只含 {quote}
        </span>
        <span>柱高为该价格区间的活跃流动性 L · tick 快照 #{formatInteger(data.snapshot_height)}，之后按加池/撤池事件更新</span>
      </div>
      <SegmentTable data={data} axis={axis} tick={tick} symbols={symbols} quote={quote} base={base} />
    </>
  );
}

function Plot({
  data,
  axis,
  tick,
  zoom,
  symbols,
  quote,
}: {
  data: PoolLiquidity;
  axis: ReturnType<typeof makeAxis>;
  tick: number;
  zoom: Zoom;
  symbols: [string, string];
  quote: string;
}) {
  const [ref, width] = useWidth();
  const [hoverU, setHoverU] = useState<number | null>(null);
  const view = useMemo(() => layout(axis, data.segments, tick, windowFor(axis, data.segments, tick, zoom)), [axis, data.segments, tick, zoom]);
  const plotHeight = HEIGHT - AXIS - TOP;
  const x = (u: number) => ((u - view.from) / (view.to - view.from)) * width;
  const y = (liquidity: number) => TOP + plotHeight - (view.maxLiquidity > 0 ? (liquidity / view.maxLiquidity) * plotHeight : 0);
  const hovered = hoverU === null ? undefined : barAt(view.bars, hoverU);
  const segment: LiquiditySegment | undefined = hovered ? data.segments[hovered.segment] : undefined;
  const range = segment ? segmentRange(axis, segment) : null;

  return (
    <div className="liquidity-plot" ref={ref}>
      {width > 0 && (
        <svg
          width={width}
          height={HEIGHT}
          role="img"
          aria-label="按价格区间的流动性分布"
          onMouseMove={(event) => {
            const box = event.currentTarget.getBoundingClientRect();
            setHoverU(view.from + ((event.clientX - box.left) / width) * (view.to - view.from));
          }}
          onMouseLeave={() => setHoverU(null)}
        >
          <line x1={0} x2={width} y1={TOP + plotHeight} y2={TOP + plotHeight} className="liquidity-baseline" />
          {view.bars.map((bar, index) => {
            const left = x(bar.lo);
            const barWidth = x(bar.hi) - left;
            // Leave a 1px surface gap on each side of bars wide enough to spare it.
            const inset = barWidth >= 6 ? 1 : 0;
            const top = y(bar.liquidity);
            return (
              <rect
                key={index}
                x={left + inset}
                y={top}
                width={Math.max(barWidth - inset * 2, 0.5)}
                height={TOP + plotHeight - top}
                fill={bar.lo >= view.cur ? BASE_COLOR : QUOTE_COLOR}
                opacity={hovered && hovered.segment !== bar.segment ? 0.45 : 0.9}
              />
            );
          })}
          <line x1={x(view.cur)} x2={x(view.cur)} y1={TOP - 6} y2={TOP + plotHeight} className="liquidity-current" />
          {axisTicks(view.from, view.to, Math.max(2, Math.min(6, Math.floor(width / 120)))).map((u) => (
            <text key={u} x={x(u)} y={HEIGHT - 6} textAnchor="middle" className="liquidity-axis">
              {formatPrice(priceAt(axis, u))}
            </text>
          ))}
        </svg>
      )}
      {segment && range && hovered && (
        <div
          className="liquidity-tip"
          style={x(hovered.lo) > width / 2 ? { right: width - x(hovered.lo) + 8 } : { left: x(hovered.hi) + 8 }}
        >
          <div className="mono">
            {formatPrice(priceAt(axis, range[0]))} – {formatPrice(priceAt(axis, range[1]))} {quote}
          </div>
          <div>
            {symbols[0]} <span className="mono">{amountText(segment.amounts[0])}</span>
          </div>
          <div>
            {symbols[1]} <span className="mono">{amountText(segment.amounts[1])}</span>
          </div>
          <div className="muted">
            L <span className="mono">{liquidityText(segment.liquidity)}</span> · tick {segment.tick_lower} ~ {segment.tick_upper}
          </div>
        </div>
      )}
    </div>
  );
}

// SegmentTable lists the ranges nearest the current price, as a text view
// of the chart.
function SegmentTable({
  data,
  axis,
  tick,
  symbols,
  quote,
  base,
}: {
  data: PoolLiquidity;
  axis: ReturnType<typeof makeAxis>;
  tick: number | null;
  symbols: [string, string];
  quote: string;
  base: string;
}) {
  const rows = useMemo(() => {
    const ranked = data.segments.map((segment) => {
      const distance =
        tick === null ? 0 : tick < segment.tick_lower ? segment.tick_lower - tick : tick > segment.tick_upper ? tick - segment.tick_upper : 0;
      return { segment, distance, range: segmentRange(axis, segment) };
    });
    return ranked
      .sort((a, b) => a.distance - b.distance)
      .slice(0, 60)
      .sort((a, b) => a.range[0] - b.range[0]);
  }, [data.segments, tick, axis]);
  if (data.segments.length === 0) return null;
  return (
    <details className="raw liquidity-table">
      <summary>区间明细（离当前价最近的 {rows.length} 段，共 {data.segments.length} 段）</summary>
      <div className="table-scroll">
        <table className="table">
          <thead>
            <tr>
              <th>价格区间（{quote}/{base}）</th>
              <th className="num">{symbols[0]}</th>
              <th className="num">{symbols[1]}</th>
              <th className="num">L</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(({ segment, range }) => (
              <tr key={segment.tick_lower}>
                <td className="mono">
                  {formatPrice(priceAt(axis, range[0]))} – {formatPrice(priceAt(axis, range[1]))}
                </td>
                <td className="num mono">{amountText(segment.amounts[0])}</td>
                <td className="num mono">{amountText(segment.amounts[1])}</td>
                <td className="num mono">{liquidityText(segment.liquidity)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </details>
  );
}
