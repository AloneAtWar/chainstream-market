import { describe, expect, it } from "vitest";
import { formatAge, formatChange, formatCompact, formatPrice, formatUsd, scaleRaw, shortAddress } from "./format";

describe("formatPrice", () => {
  it("keeps four significant digits below one", () => {
    expect(formatPrice("0.5")).toBe("0.5");
    expect(formatPrice("0.0123456")).toBe("0.01235");
    expect(formatPrice("0.00012345")).toBe("0.0001235");
  });

  it("collapses long runs of zeros into a subscript", () => {
    expect(formatPrice("0.00000123")).toBe("0.0₅123");
    expect(formatPrice("0.000000000001")).toBe("0.0₁₁1");
  });

  it("rounds carries into the next digit", () => {
    expect(formatPrice("0.0999996")).toBe("0.1");
  });

  it("writes larger prices with separators", () => {
    expect(formatPrice("3456.789")).toBe("3,456.79");
    expect(formatPrice("1.5")).toBe("1.50");
    expect(formatPrice("0")).toBe("0");
    expect(formatPrice(null)).toBe("—");
    expect(formatPrice("not a number")).toBe("—");
  });
});

describe("compact numbers", () => {
  it("adds suffixes", () => {
    expect(formatCompact("1234")).toBe("1.23K");
    expect(formatCompact(2_500_000)).toBe("2.5M");
    expect(formatCompact("1000000000")).toBe("1B");
    expect(formatCompact(12)).toBe("12");
  });

  it("formats USD and changes", () => {
    expect(formatUsd("1234567")).toBe("$1.23M");
    expect(formatUsd("-50")).toBe("-$50");
    expect(formatChange("-0.25")).toBe("-25.00%");
    expect(formatChange("0.1234")).toBe("+12.34%");
    expect(formatChange("0")).toBe("0.00%");
  });
});

describe("scaleRaw", () => {
  it("scales without floating point", () => {
    expect(scaleRaw("1234500000000000000000", 18)).toBe("1234.5");
    expect(scaleRaw("1", 6)).toBe("0.000001");
    expect(scaleRaw("1000000", 6)).toBe("1");
    expect(scaleRaw("0", 18)).toBe("0");
    expect(scaleRaw("42", 0)).toBe("42");
  });
});

describe("misc", () => {
  it("formats ages and addresses", () => {
    expect(formatAge(100, 145)).toBe("45s");
    expect(formatAge(0, 7200)).toBe("2h");
    expect(formatAge(0, 86400 * 3)).toBe("3d");
    expect(formatAge(null)).toBe("—");
    expect(shortAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")).toBe("0xc02a…6cc2");
  });
});
