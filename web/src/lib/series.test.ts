import { describe, expect, it } from "vitest";
import type { Candle, Trade } from "./api";
import { applyCandle, fillGaps, mergeTrades, prependCandles, resetCandles, revertTrades } from "./series";

function candle(openTime: number, close = "1", open = close): Candle {
  return { open_time: openTime, open, high: close, low: open, close, volume_base: "1", volume_usd: "1", trades: 1 };
}

function trade(height: number, logIndex: number, revision?: number): Trade {
  return {
    block_height: height,
    block_time: height * 12,
    revision_height: revision,
    transaction_hash: `0x${height}`,
    log_index: logIndex,
    operation_index: 0,
    protocol: "uniswap_v2",
    pool_id: "0xpool",
    trader: "0xtrader",
    side: "buy",
    base_token: "0xbase",
    quote_token: "0xquote",
    base_amount: "1",
    quote_amount: "1",
    price_quote: "1",
    price_usd: "1",
    value_usd: "1",
    outlier: false,
  };
}

describe("candles", () => {
  it("replaces a bucket by open time and keeps the others", () => {
    const history = [candle(60), candle(120)];
    const next = applyCandle(history, candle(120, "2"));
    expect(next.map((c) => c.close)).toEqual(["1", "2"]);
    expect(next[0]).toBe(history[0]);
  });

  it("appends newer buckets and inserts older ones in order", () => {
    const history = [candle(60), candle(180)];
    expect(applyCandle(history, candle(240)).map((c) => c.open_time)).toEqual([60, 180, 240]);
    expect(applyCandle(history, candle(120)).map((c) => c.open_time)).toEqual([60, 120, 180]);
    expect(applyCandle([], candle(60)).map((c) => c.open_time)).toEqual([60]);
  });

  it("resets to the bucket current at rollback_to", () => {
    const history = [candle(60), candle(120), candle(180), candle(240)];
    const reset = resetCandles(history, candle(120, "5"));
    expect(reset.map((c) => [c.open_time, c.close])).toEqual([
      [60, "1"],
      [120, "5"],
    ]);
    expect(resetCandles(history, null)).toEqual([]);
  });

  it("prepends older pages without duplicates", () => {
    const merged = prependCandles([candle(180), candle(240)], [candle(60), candle(120), candle(180, "9")]);
    expect(merged.map((c) => [c.open_time, c.close])).toEqual([
      [60, "1"],
      [120, "1"],
      [180, "1"],
      [240, "1"],
    ]);
  });

  it("fills empty buckets flat at the previous close", () => {
    const filled = fillGaps([candle(60, "2", "1"), candle(240, "3", "2")], 60);
    expect(filled.map((c) => [c.open_time, c.open, c.close, c.trades])).toEqual([
      [60, "1", "2", 1],
      [120, "2", "2", 0],
      [180, "2", "2", 0],
      [240, "2", "3", 1],
    ]);
  });
});

describe("trades", () => {
  it("merges newest first and overwrites the same trade", () => {
    const history = [trade(10, 2), trade(10, 1), trade(9, 5)];
    const replayed = { ...trade(10, 1), price_usd: "2" };
    const merged = mergeTrades(history, [trade(11, 0), replayed], 10);
    expect(merged.map((t) => `${t.block_height}:${t.log_index}`)).toEqual(["11:0", "10:2", "10:1", "9:5"]);
    expect(merged[2]!.price_usd).toBe("2");
    expect(mergeTrades(merged, [], 2)).toHaveLength(2);
  });

  it("drops trades emitted above rollback_to", () => {
    // A late trade keeps its own block height but belongs to its revision.
    const trades = [trade(12, 0), trade(8, 0, 12), trade(10, 0), trade(9, 0)];
    expect(revertTrades(trades, 10).map((t) => t.block_height)).toEqual([10, 9]);
  });
});
