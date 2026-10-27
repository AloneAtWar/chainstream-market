// @vitest-environment jsdom
import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Candle, Trade } from "./api";
import type { ChainMessage, ChannelMessage } from "./live";

// A stand-in for the shared live client: tests push messages by hand.
const fakeLive = vi.hoisted(() => {
  const channelListeners = new Map<string, Set<(message: unknown) => void>>();
  const chainListeners = new Set<(message: unknown) => void>();
  return {
    status: "open",
    channelListeners,
    chainListeners,
    subscribe(channel: string, listener: (message: unknown) => void) {
      if (!channelListeners.has(channel)) channelListeners.set(channel, new Set());
      channelListeners.get(channel)!.add(listener);
      return () => channelListeners.get(channel)?.delete(listener);
    },
    onChain(listener: (message: unknown) => void) {
      chainListeners.add(listener);
      return () => chainListeners.delete(listener);
    },
    onOpen() {
      return () => {};
    },
    onStatus() {
      return () => {};
    },
  };
});

vi.mock("./live", async (importOriginal) => ({ ...(await importOriginal<typeof import("./live")>()), live: fakeLive }));

const fakeApi = vi.hoisted(() => ({ candles: vi.fn(), trades: vi.fn() }));
vi.mock("./api", () => ({ api: fakeApi }));

const { useLiveCandles, useLiveTrades, CANDLE_PAGE } = await import("./hooks");

function candle(openTime: number, close: string): Candle {
  return { open_time: openTime, open: close, high: close, low: close, close, volume_base: "1", volume_usd: "1", trades: 1 };
}

function trade(height: number, revision?: number): Trade {
  return {
    block_height: height,
    block_time: height,
    revision_height: revision,
    transaction_hash: `0x${height}`,
    log_index: 0,
    operation_index: 0,
    protocol: "uniswap_v2",
    pool_id: "0xpool",
    trader: "0xtrader",
    side: "buy",
    base_token: "0xaaa",
    quote_token: "0xquote",
    base_amount: "1",
    quote_amount: "1",
    price_quote: "1",
    price_usd: "1",
    value_usd: "1",
    outlier: false,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => (resolve = done));
  return { promise, resolve };
}

function sendChain(message: ChainMessage) {
  for (const listener of fakeLive.chainListeners) listener(message);
}

function sendChannel(message: ChannelMessage) {
  for (const listener of fakeLive.channelListeners.get(message.channel) ?? []) listener(message);
}

beforeEach(() => {
  fakeApi.candles.mockReset();
  fakeApi.trades.mockReset();
  fakeLive.channelListeners.clear();
  fakeLive.chainListeners.clear();
});

describe("useLiveCandles", () => {
  it("drops an older page that returns after the channel changed", async () => {
    const olderPage = deferred<Candle[]>();
    fakeApi.candles.mockImplementation((_chain, _subject, key: string, _interval, _limit, to?: number) => {
      if (to !== undefined) return olderPage.promise;
      return Promise.resolve(key === "0xaaa" ? [candle(120, "10")] : [candle(3600, "900")]);
    });
    const { result, rerender } = renderHook(({ key, interval }) => useLiveCandles(1, "token", key, interval), {
      initialProps: { key: "0xaaa", interval: "1m" },
    });
    await waitFor(() => expect(result.current.data).toHaveLength(1));

    let paging!: Promise<void>;
    act(() => {
      paging = result.current.loadOlder();
    });
    rerender({ key: "0xbbb", interval: "1h" });
    await waitFor(() => expect(result.current.data[0]?.close).toBe("900"));

    await act(async () => {
      olderPage.resolve([candle(60, "10")]);
      await paging;
    });
    expect(result.current.data.map((c) => [c.open_time, c.close])).toEqual([[3600, "900"]]);

    // The new channel can still page back: the stale page set no exhaustion.
    fakeApi.candles.mockResolvedValueOnce(Array.from({ length: CANDLE_PAGE }, (_, index) => candle(index * 3600 - CANDLE_PAGE * 3600, "1")));
    await act(() => result.current.loadOlder());
    expect(result.current.data).toHaveLength(CANDLE_PAGE + 1);
  });
});

describe("useLiveTrades", () => {
  it("reloads history after a revert", async () => {
    // Height 90 was written late, at 101, and REST does not say so.
    fakeApi.trades.mockResolvedValueOnce([trade(100), trade(90)]);
    const { result } = renderHook(() => useLiveTrades(1, "token", "0xaaa"));
    await waitFor(() => expect(result.current.data).toHaveLength(2));

    sendChannel({ type: "trades", channel: "trades:1:token:0xaaa", height: 102, trades: [trade(102, 102)] });
    await waitFor(() => expect(result.current.data).toHaveLength(3));

    fakeApi.trades.mockResolvedValueOnce([]);
    act(() => sendChain({ type: "revert", chain_id: 1, rollback_to: 99 }));
    // The live trade above rollback_to goes at once; the reload drops the rest.
    await waitFor(() => expect(result.current.data).toEqual([]));
    expect(fakeApi.trades).toHaveBeenCalledTimes(2);
  });

  it("ignores reverts of other chains", async () => {
    fakeApi.trades.mockResolvedValueOnce([trade(100)]);
    const { result } = renderHook(() => useLiveTrades(1, "token", "0xaaa"));
    await waitFor(() => expect(result.current.data).toHaveLength(1));
    act(() => sendChain({ type: "revert", chain_id: 56, rollback_to: 10 }));
    expect(result.current.data).toHaveLength(1);
    expect(fakeApi.trades).toHaveBeenCalledTimes(1);
  });
});
