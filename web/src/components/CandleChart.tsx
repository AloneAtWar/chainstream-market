import {
  CandlestickSeries,
  ColorType,
  createChart,
  CrosshairMode,
  HistogramSeries,
  TickMarkType,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";
import { useEffect, useMemo, useRef, useState } from "react";
import type { Candle } from "../lib/api";
import { intervalSeconds } from "../lib/chains";
import { changeClass, formatChange, formatPrice, formatUsd } from "../lib/format";
import { fillGaps } from "../lib/series";

const UP = "#16c784";
const DOWN = "#ea3943";

interface Bar {
  candle: CandlestickData<UTCTimestamp>;
  volume: HistogramData<UTCTimestamp>;
  source: Candle;
}

function toBars(candles: readonly Candle[], interval: string): Bar[] {
  return fillGaps(candles, intervalSeconds(interval)).map((candle) => {
    const open = Number(candle.open);
    const close = Number(candle.close);
    const time = candle.open_time as UTCTimestamp;
    return {
      candle: { time, open, high: Number(candle.high), low: Number(candle.low), close },
      volume: { time, value: Number(candle.volume_usd), color: close >= open ? `${UP}66` : `${DOWN}66` },
      source: candle,
    };
  });
}

function sameBar(left: Bar | undefined, right: Bar | undefined): boolean {
  if (!left || !right) return false;
  const a = left.candle;
  const b = right.candle;
  return (
    a.time === b.time && a.open === b.open && a.high === b.high && a.low === b.low && a.close === b.close &&
    left.volume.value === right.volume.value
  );
}

// priceBase is 10^n for a price step four significant digits below the
// lowest price.
function priceBase(bars: readonly Bar[]): number {
  let lowest = Infinity;
  for (const bar of bars) if (bar.candle.low > 0 && bar.candle.low < lowest) lowest = bar.candle.low;
  if (!Number.isFinite(lowest)) return 100;
  const digits = Math.min(18, Math.max(2, Math.ceil(-Math.log10(lowest)) + 4));
  return 10 ** digits;
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}

// Times are shown in the browser's zone; the chart itself works in UTC.
function tickMark(time: Time, type: TickMarkType): string {
  const date = new Date((time as number) * 1000);
  switch (type) {
    case TickMarkType.Year:
      return String(date.getFullYear());
    case TickMarkType.Month:
      return `${date.getMonth() + 1}月`;
    case TickMarkType.DayOfMonth:
      return `${date.getMonth() + 1}/${date.getDate()}`;
    default:
      return `${pad(date.getHours())}:${pad(date.getMinutes())}`;
  }
}

function crosshairTime(time: Time): string {
  const date = new Date((time as number) * 1000);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

export function CandleChart({
  candles,
  interval,
  onNeedOlder,
}: {
  candles: readonly Candle[];
  interval: string;
  onNeedOlder?: () => void;
}) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const candleSeries = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeries = useRef<ISeriesApi<"Histogram"> | null>(null);
  const shown = useRef<Bar[]>([]);
  const priceBaseRef = useRef(0);
  const needOlder = useRef(onNeedOlder);
  needOlder.current = onNeedOlder;
  const [hovered, setHovered] = useState<Candle | null>(null);

  const bars = useMemo(() => toBars(candles, interval), [candles, interval]);
  const barsRef = useRef(bars);
  barsRef.current = bars;

  useEffect(() => {
    const element = container.current!;
    const instance = createChart(element, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#8a8f98",
        fontFamily: "inherit",
        attributionLogo: false,
      },
      grid: { vertLines: { color: "#1f232b" }, horzLines: { color: "#1f232b" } },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#262a33" },
      timeScale: { borderColor: "#262a33", timeVisible: true, secondsVisible: false, tickMarkFormatter: tickMark },
      localization: { timeFormatter: crosshairTime, priceFormatter: (price: number) => formatPrice(price) },
    });
    const series = instance.addSeries(CandlestickSeries, {
      upColor: UP,
      downColor: DOWN,
      borderVisible: false,
      wickUpColor: UP,
      wickDownColor: DOWN,
    });
    series.priceScale().applyOptions({ scaleMargins: { top: 0.08, bottom: 0.25 } });
    const volume = instance.addSeries(HistogramSeries, {
      priceScaleId: "",
      priceFormat: { type: "custom", formatter: (value: number) => formatUsd(value) },
      lastValueVisible: false,
      priceLineVisible: false,
    });
    volume.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

    instance.subscribeCrosshairMove((param) => {
      if (param.time === undefined) {
        setHovered(null);
        return;
      }
      setHovered(barsRef.current.find((bar) => bar.candle.time === param.time)?.source ?? null);
    });
    instance.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (range && range.from < 20 && shown.current.length > 0) needOlder.current?.();
    });

    chart.current = instance;
    candleSeries.current = series;
    volumeSeries.current = volume;
    return () => {
      instance.remove();
      chart.current = null;
      shown.current = [];
    };
  }, []);

  // A new interval starts a new series, which should open at the latest bars.
  useEffect(() => {
    shown.current = [];
  }, [interval]);

  useEffect(() => {
    const series = candleSeries.current;
    const volume = volumeSeries.current;
    if (!series || !volume) return;
    // The price scale's step has to suit the smallest price shown, which
    // can be far below a cent.
    const base = priceBase(bars);
    if (base !== priceBaseRef.current) {
      priceBaseRef.current = base;
      series.applyOptions({ priceFormat: { type: "custom", formatter: (price: number) => formatPrice(price), base } });
    }
    const previous = shown.current;
    // Live pushes change the last bar or append one; update() keeps the
    // user's scroll position. Anything else resets the data.
    const appendOnly =
      previous.length > 0 &&
      bars.length >= previous.length &&
      bars.length - previous.length <= 2 &&
      previous.slice(0, -1).every((bar, index) => sameBar(bar, bars[index]));
    if (appendOnly) {
      for (let index = previous.length - 1; index < bars.length; index++) {
        const bar = bars[index]!;
        if (sameBar(bar, previous[index])) continue;
        series.update(bar.candle);
        volume.update(bar.volume);
      }
    } else {
      const first = previous[0]?.candle.time;
      const prepended = first !== undefined && bars.length > previous.length && bars[bars.length - previous.length]?.candle.time === first;
      const range = chart.current?.timeScale().getVisibleLogicalRange();
      series.setData(bars.map((bar) => bar.candle));
      volume.setData(bars.map((bar) => bar.volume));
      if (prepended && range) {
        // Older history landed on the left: keep looking at the same bars.
        const added = bars.length - previous.length;
        chart.current?.timeScale().setVisibleLogicalRange({ from: range.from + added, to: range.to + added });
      } else if (previous.length === 0 && bars.length > 0) {
        chart.current?.timeScale().setVisibleLogicalRange({ from: Math.max(0, bars.length - 120), to: bars.length + 5 });
      }
    }
    shown.current = bars;
  }, [bars]);

  const legend = hovered ?? candles[candles.length - 1] ?? null;
  const change = legend ? Number(legend.close) / Number(legend.open) - 1 : null;

  return (
    <div className="chart">
      {legend && (
        <div className="chart-legend mono">
          <span>开 {formatPrice(legend.open)}</span>
          <span>高 {formatPrice(legend.high)}</span>
          <span>低 {formatPrice(legend.low)}</span>
          <span>收 {formatPrice(legend.close)}</span>
          <span className={changeClass(change)}>{formatChange(change)}</span>
          <span>额 {formatUsd(legend.volume_usd)}</span>
          <span>笔 {legend.trades}</span>
        </div>
      )}
      <div ref={container} className="chart-canvas" />
      {candles.length === 0 && <div className="chart-empty">暂无 K 线</div>}
    </div>
  );
}
