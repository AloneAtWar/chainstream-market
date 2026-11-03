import { describe, expect, it } from "vitest";
import type { LiquiditySegment } from "./api";
import { axisTicks, barAt, currentTick, layout, makeAxis, pickZoom, priceAt, toU, windowFor } from "./liquidity";

function segment(lower: number, upper: number, liquidity: number): LiquiditySegment {
  return { tick_lower: lower, tick_upper: upper, liquidity: String(liquidity), amounts: [] };
}

describe("currentTick", () => {
  it("reads tick 0 from a price of one", () => {
    expect(currentTick(String(2n ** 96n))).toBeCloseTo(0, 6);
  });

  it("reads a USDC/WETH-like price", () => {
    // sqrtPriceX96 of USDC (6) / WETH (18) near $3,400: tick ≈ 195,000.
    const tick = currentTick("1363617745000000000000000000000000")!;
    expect(tick).toBeGreaterThan(190_000);
    expect(tick).toBeLessThan(200_000);
  });

  it("rejects what does not parse", () => {
    expect(currentTick("")).toBeNull();
    expect(currentTick("0")).toBeNull();
    expect(currentTick("x")).toBeNull();
  });
});

describe("prices", () => {
  it("scales by decimals and mirrors when flipped", () => {
    // token0 USDC (6), token1 WETH (18): token0 in token1 at tick t is
    // 1.0001^t × 1e-12.
    const tick = 200_000;
    const plain = makeAxis(false, [6, 18]);
    const flipped = makeAxis(true, [6, 18]);
    const usdcInWeth = priceAt(plain, toU(plain, tick));
    const wethInUsdc = priceAt(flipped, toU(flipped, tick));
    expect(usdcInWeth * wethInUsdc).toBeCloseTo(1, 9);
    expect(wethInUsdc).toBeGreaterThan(2000);
    expect(wethInUsdc).toBeLessThan(2100);
  });

  it("falls back to raw ratios without decimals", () => {
    expect(makeAxis(false, [6, null]).scale).toBe(1);
    expect(makeAxis(true, null).scale).toBe(1);
  });
});

describe("layout", () => {
  const segments = [segment(-120, -60, 3), segment(-60, 60, 8), segment(60, 120, 5)];

  it("splits the range the price sits in by token", () => {
    const axis = makeAxis(false, [18, 18]);
    const result = layout(axis, segments, 10, [-1000, 1000]);
    expect(result.maxLiquidity).toBe(8);
    expect(result.bars.map((bar) => [bar.segment, bar.lo, bar.hi, bar.side])).toEqual([
      [0, -120, -60, 1],
      [1, -60, 10, 1],
      [1, 10, 60, 0],
      [2, 60, 120, 0],
    ]);
  });

  it("mirrors the axis when flipped and keeps each bar's token", () => {
    const axis = makeAxis(true, [18, 18]);
    const result = layout(axis, segments, 10, [-1000, 1000]);
    expect(result.cur).toBe(-10);
    const left = result.bars.filter((bar) => bar.hi <= result.cur);
    expect(left.every((bar) => bar.side === 0)).toBe(true);
    expect(barAt(result.bars, 100)?.segment).toBe(0);
  });

  it("clips to the window and scales to what is visible", () => {
    const axis = makeAxis(false, [18, 18]);
    const result = layout(axis, segments, 0, [70, 1000]);
    expect(result.bars).toEqual([{ segment: 2, lo: 70, hi: 120, liquidity: 5, side: 0 }]);
    expect(result.maxLiquidity).toBe(5);
  });
});

describe("windows", () => {
  it("leaves full-range edges out of the whole view", () => {
    const axis = makeAxis(false, [18, 18]);
    const [from, to] = windowFor(axis, [segment(-887220, -100, 1), segment(-100, 200, 9), segment(200, 887220, 1)], 0, "all");
    expect(from).toBeLessThan(-100);
    expect(from).toBeGreaterThan(-200);
    expect(to).toBeGreaterThan(200);
    expect(to).toBeLessThan(300);
  });

  it("opens a stable pair on its tight band", () => {
    const stable = [segment(-887272, -5, 1), segment(-5, -2, 40), segment(-2, 3, 100), segment(3, 6, 30), segment(6, 887272, 1)];
    expect(pickZoom(stable, 0)).toBe("1.002");
  });

  it("widens until the spread of a volatile pool fits", () => {
    const volatile = [segment(-4000, -2000, 50), segment(-2000, 2000, 100), segment(2000, 6000, 40)];
    expect(pickZoom(volatile, 0)).toBe("1.5");
  });

  it("shows everything when nothing has liquidity", () => {
    expect(pickZoom([], 0)).toBe("all");
  });

  it("spaces axis labels inside the window", () => {
    expect(axisTicks(0, 100, 4)).toEqual([12.5, 37.5, 62.5, 87.5]);
  });
});
