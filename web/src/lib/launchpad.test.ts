import { describe, expect, it } from "vitest";
import type { LaunchpadToken } from "./api";
import { chainById } from "./chains";
import { formatRaised } from "./launchpad";

const bsc = chainById(56)!;
const fist = "0xc9882def23bc42d53895b8361d0b1edc7570bc6a";

function curve(quote: string, quoteRaised: string, quoteSymbol: string | null, quoteDecimals: number | null): LaunchpadToken {
  return {
    token: "0xaaaa000000000000000000000000000000008888",
    protocol: "flap",
    pool_id: "0xpool",
    manager: "0xmanager",
    quote,
    quote_symbol: quoteSymbol,
    quote_decimals: quoteDecimals,
    creator: null,
    name: null,
    symbol: null,
    decimals: null,
    meta_uri: null,
    status: "bonding",
    launch_height: 1,
    launch_time: 1,
    launch_seen: true,
    progress: "0.5",
    base_remaining: null,
    quote_raised: quoteRaised,
    price: null,
    trades: 1,
    buys: 1,
    sells: 0,
    last_trade_time: null,
    completed_time: null,
    migrated_time: null,
    target_pool_id: null,
    price_usd: null,
    volume_24h_usd: null,
    holders: null,
  };
}

describe("formatRaised", () => {
  it("scales a quote by its registry decimals", () => {
    expect(formatRaised(bsc, curve(fist, "1000000", "FIST", 6))).toBe("1 FIST");
  });

  it("assumes 18 decimals only for the chain's coin", () => {
    expect(formatRaised(bsc, curve(bsc.wrappedNative, "12400000000000000000", null, null))).toBe("12.4 BNB");
    expect(formatRaised(bsc, curve("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "2000000000000000000", null, null))).toBe("2 BNB");
  });

  it("marks the raw amount of a quote whose decimals are unknown", () => {
    expect(formatRaised(bsc, curve(fist, "1000000", "FIST", null))).toBe("1M FIST 最小单位（精度未知）");
  });
});
