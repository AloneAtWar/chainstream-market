// Merging REST history with live push, following market-api's rules:
// candles are replaced by open_time, trades are keyed by
// (block_height, transaction_hash, log_index, operation_index), and a
// reorg drops what the reverted blocks emitted.

import type { Candle, Trade } from "./api";

// applyCandle replaces the bucket with the same open time, or inserts it in
// order. Unchanged buckets keep their identity.
export function applyCandle(candles: readonly Candle[], candle: Candle): Candle[] {
  const last = candles[candles.length - 1];
  if (!last || candle.open_time > last.open_time) return [...candles, candle];
  const index = findIndex(candles, candle.open_time);
  const next = candles.slice();
  if (candles[index]?.open_time === candle.open_time) next[index] = candle;
  else next.splice(index, 0, candle);
  return next;
}

// resetCandles applies a candle_reset: the given bucket replaces its own and
// every newer bucket is dropped; null drops the whole series.
export function resetCandles(candles: readonly Candle[], candle: Candle | null): Candle[] {
  if (candle === null) return [];
  const index = findIndex(candles, candle.open_time);
  return [...candles.slice(0, index), candle];
}

// prependCandles adds older history in front, skipping buckets already held.
export function prependCandles(candles: readonly Candle[], older: readonly Candle[]): Candle[] {
  const first = candles[0];
  if (!first) return older.slice();
  return [...older.filter((candle) => candle.open_time < first.open_time), ...candles];
}

// findIndex returns the first index whose open time is at or after openTime.
function findIndex(candles: readonly Candle[], openTime: number): number {
  let low = 0;
  let high = candles.length;
  while (low < high) {
    const middle = (low + high) >> 1;
    if (candles[middle]!.open_time < openTime) low = middle + 1;
    else high = middle;
  }
  return low;
}

// fillGaps draws buckets without trades flat at the previous close, as the
// API documents, so the chart's time axis keeps a constant step. At most
// maxFill buckets are added per gap.
export function fillGaps(candles: readonly Candle[], intervalSeconds: number, maxFill = 500): Candle[] {
  const filled: Candle[] = [];
  for (const candle of candles) {
    const previous = filled[filled.length - 1];
    if (previous) {
      const missing = Math.min(maxFill, Math.floor((candle.open_time - previous.open_time) / intervalSeconds) - 1);
      for (let step = 1; step <= missing; step++) {
        const price = previous.close;
        filled.push({
          open_time: previous.open_time + step * intervalSeconds,
          open: price,
          high: price,
          low: price,
          close: price,
          volume_base: "0",
          volume_usd: "0",
          trades: 0,
        });
      }
    }
    filled.push(candle);
  }
  return filled;
}

export function tradeKey(trade: Trade): string {
  return `${trade.block_height}:${trade.transaction_hash}:${trade.log_index}:${trade.operation_index}`;
}

function compareTrades(left: Trade, right: Trade): number {
  return (
    right.block_height - left.block_height ||
    right.log_index - left.log_index ||
    right.operation_index - left.operation_index
  );
}

// mergeTrades adds trades to a newest-first list, overwriting ones with the
// same key, and keeps at most limit.
export function mergeTrades(trades: readonly Trade[], incoming: readonly Trade[], limit: number): Trade[] {
  if (incoming.length === 0) return trades.slice(0, limit);
  const byKey = new Map(trades.map((trade) => [tradeKey(trade), trade]));
  for (const trade of incoming) byKey.set(tradeKey(trade), trade);
  return [...byKey.values()].sort(compareTrades).slice(0, limit);
}

// revertTrades drops every trade a block above rollbackTo emitted.
export function revertTrades(trades: readonly Trade[], rollbackTo: number): Trade[] {
  return trades.filter((trade) => (trade.revision_height ?? trade.block_height) <= rollbackTo);
}
