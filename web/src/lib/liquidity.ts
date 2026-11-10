// Layout math for a concentrated pool's liquidity distribution chart. Ticks
// are log-prices (price = 1.0001^tick), so the chart's x axis works in ticks
// and only the labels are prices.
//
// The chart can price either token in the other. When it prices token1 in
// token0 the axis is mirrored: position u = -tick, so prices still rise to
// the right. A range above the current tick holds only token0, one below
// only token1, and the range the price sits in holds both.

import type { LiquiditySegment } from "./api";

const LOG_TICK = Math.log(1.0001);
const Q96 = 2 ** 96;
// FULL_RANGE_EDGE marks ticks this far out as a full-range position's edges:
// the usable extremes of every tick spacing in use lie beyond it.
const FULL_RANGE_EDGE = 880000;

// ZOOMS are the chart's windows around the current price: ×k shows prices
// from current/k to current×k. null shows every range.
export const ZOOMS = [
  { value: "1.002", factor: 1.002, label: "±0.2%" },
  { value: "1.01", factor: 1.01, label: "±1%" },
  { value: "1.05", factor: 1.05, label: "±5%" },
  { value: "1.2", factor: 1.2, label: "±20%" },
  { value: "1.5", factor: 1.5, label: "×1.5" },
  { value: "2", factor: 2, label: "×2" },
  { value: "5", factor: 5, label: "×5" },
  { value: "20", factor: 20, label: "×20" },
  { value: "all", factor: null, label: "全部" },
] as const;

export type Zoom = (typeof ZOOMS)[number]["value"];

// currentTick is the fractional tick of a sqrtPriceX96, or null when it does
// not parse.
export function currentTick(sqrtPriceX96: string): number | null {
  const sqrt = Number(sqrtPriceX96);
  if (!Number.isFinite(sqrt) || sqrt <= 0) return null;
  return (2 * Math.log(sqrt / Q96)) / LOG_TICK;
}

export interface Axis {
  // flipped prices token1 in token0; otherwise token0 is priced in token1.
  flipped: boolean;
  // scale turns 1.0001^u into a price in whole tokens; it is 1 when either
  // token's decimals are unknown, and prices are then raw-unit ratios.
  scale: number;
}

export function makeAxis(flipped: boolean, decimals: readonly (number | null)[] | null): Axis {
  const d0 = decimals?.[0];
  const d1 = decimals?.[1];
  if (d0 === null || d0 === undefined || d1 === null || d1 === undefined) return { flipped, scale: 1 };
  return { flipped, scale: 10 ** (flipped ? d1 - d0 : d0 - d1) };
}

// toU maps a tick onto the chart's axis.
export function toU(axis: Axis, tick: number): number {
  return axis.flipped ? -tick : tick;
}

// priceAt is the displayed price at axis position u.
export function priceAt(axis: Axis, u: number): number {
  return Math.exp(u * LOG_TICK) * axis.scale;
}

// Bar is one segment's slice of the window, in axis units. side is the token
// it holds: a segment the price sits in yields two bars.
export interface Bar {
  segment: number;
  lo: number;
  hi: number;
  liquidity: number;
  side: 0 | 1;
}

export interface Layout {
  // [from, to] is the window on the axis, cur the current price on it.
  from: number;
  to: number;
  cur: number;
  bars: Bar[];
  // maxLiquidity is the tallest visible bar, for the y scale.
  maxLiquidity: number;
}

// segmentRange is a segment's span on the axis, low end first.
export function segmentRange(axis: Axis, segment: LiquiditySegment): [number, number] {
  const a = toU(axis, segment.tick_lower);
  const b = toU(axis, segment.tick_upper);
  return a < b ? [a, b] : [b, a];
}

// windowFor returns the axis window a zoom shows around the current tick.
export function windowFor(axis: Axis, segments: readonly LiquiditySegment[], tick: number, zoom: Zoom): [number, number] {
  const cur = toU(axis, tick);
  const factor = ZOOMS.find((option) => option.value === zoom)?.factor ?? null;
  if (factor !== null) {
    const half = Math.log(factor) / LOG_TICK;
    return [cur - half, cur + half];
  }
  // A full-range position's edges sit at the protocol's limits and would
  // flatten everything else; it still shows as a floor across the window.
  let from = cur;
  let to = cur;
  for (const segment of segments) {
    for (const edge of [segment.tick_lower, segment.tick_upper]) {
      if (Math.abs(edge) >= FULL_RANGE_EDGE) continue;
      from = Math.min(from, toU(axis, edge));
      to = Math.max(to, toU(axis, edge));
    }
  }
  const pad = Math.max((to - from) * 0.05, 10);
  return [from - pad, to + pad];
}

// layout clips the segments to a window and splits the one the price sits
// in, so each bar holds a single token.
export function layout(axis: Axis, segments: readonly LiquiditySegment[], tick: number, window: [number, number]): Layout {
  const [from, to] = window;
  const cur = toU(axis, tick);
  const bars: Bar[] = [];
  let maxLiquidity = 0;
  segments.forEach((segment, index) => {
    const liquidity = Number(segment.liquidity);
    if (!(liquidity > 0)) return;
    const [lo, hi] = segmentRange(axis, segment);
    const clippedLo = Math.max(lo, from);
    const clippedHi = Math.min(hi, to);
    if (clippedLo >= clippedHi) return;
    maxLiquidity = Math.max(maxLiquidity, liquidity);
    // On the axis, positions right of the current price hold token0 unless
    // the axis is mirrored.
    const right: 0 | 1 = axis.flipped ? 1 : 0;
    const left: 0 | 1 = axis.flipped ? 0 : 1;
    if (clippedHi <= cur) bars.push({ segment: index, lo: clippedLo, hi: clippedHi, liquidity, side: left });
    else if (clippedLo >= cur) bars.push({ segment: index, lo: clippedLo, hi: clippedHi, liquidity, side: right });
    else {
      bars.push({ segment: index, lo: clippedLo, hi: cur, liquidity, side: left });
      bars.push({ segment: index, lo: cur, hi: clippedHi, liquidity, side: right });
    }
  });
  return { from, to, cur, bars, maxLiquidity };
}

// pickZoom chooses the narrowest window that reaches every range holding at
// least a quarter of the peak liquidity and shows at least three range edges,
// so a stable pair opens on its tight band and a volatile pool on its
// spread.
export function pickZoom(segments: readonly LiquiditySegment[], tick: number): Zoom {
  let peak = 0;
  for (const segment of segments) peak = Math.max(peak, Number(segment.liquidity) || 0);
  if (peak === 0) return "all";
  let reach = 0;
  for (const segment of segments) {
    if ((Number(segment.liquidity) || 0) < peak / 4) continue;
    const distance =
      tick < segment.tick_lower ? segment.tick_lower - tick : tick > segment.tick_upper ? tick - segment.tick_upper : 0;
    reach = Math.max(reach, distance);
  }
  const edges = new Set<number>();
  for (const segment of segments) {
    edges.add(segment.tick_lower);
    edges.add(segment.tick_upper);
  }
  for (const option of ZOOMS) {
    if (option.factor === null) break;
    const half = Math.log(option.factor) / LOG_TICK;
    if (half < reach * 1.25) continue;
    let visible = 0;
    for (const edge of edges) if (Math.abs(edge - tick) <= half) visible++;
    if (visible >= 3) return option.value;
  }
  return "all";
}

// axisTicks returns about count evenly spaced positions across a window.
export function axisTicks(from: number, to: number, count = 5): number[] {
  if (!(to > from)) return [];
  const step = (to - from) / count;
  return Array.from({ length: count }, (_, index) => from + step * (index + 0.5));
}

// barAt finds the bar under axis position u.
export function barAt(bars: readonly Bar[], u: number): Bar | undefined {
  return bars.find((bar) => u >= bar.lo && u < bar.hi);
}
