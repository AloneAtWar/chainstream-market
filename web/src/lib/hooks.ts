import { useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import { api, type Candle, type Trade } from "./api";
import { chainById, NATIVE_ADDRESS } from "./chains";
import { channels, chainOfChannel, live, type ChainMessage, type ChannelMessage, type LiveStatus } from "./live";
import { applyCandle, mergeTrades, prependCandles, resetCandles, revertTrades } from "./series";

// useStream subscribes to a channel first and loads history second, then
// applies the messages that arrived meanwhile, as market-api recommends.
// It reloads after every reconnect, since the hub does not replay.
function useStream<T>(
  channel: string | null,
  load: () => Promise<T>,
  apply: (state: T, message: ChannelMessage) => T,
  revert: (state: T, rollbackTo: number) => T,
  empty: T,
): { data: T; loading: boolean; error: Error | null; update: (change: (state: T) => T) => void } {
  const [data, setData] = useState<T>(empty);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const loadRef = useRef(load);
  const applyRef = useRef(apply);
  const revertRef = useRef(revert);
  loadRef.current = load;
  applyRef.current = apply;
  revertRef.current = revert;

  useEffect(() => {
    if (channel === null) return;
    const chain = chainOfChannel(channel);
    let generation = 0;
    let pending: ((state: T) => T)[] | null = [];
    const push = (change: (state: T) => T) => {
      if (pending) pending.push(change);
      else setData(change);
    };
    const reload = () => {
      const current = ++generation;
      pending = [];
      setLoading(true);
      loadRef
        .current()
        .then((history) => {
          if (current !== generation) return;
          const queued = pending ?? [];
          pending = null;
          setData(queued.reduce((state, change) => change(state), history));
          setError(null);
        })
        .catch((reason: unknown) => {
          if (current !== generation) return;
          pending = null;
          setError(reason instanceof Error ? reason : new Error(String(reason)));
        })
        .finally(() => {
          if (current === generation) setLoading(false);
        });
    };
    setData(empty);
    const unsubscribe = live.subscribe(channel, (message) => push((state) => applyRef.current(state, message)));
    const offChain = live.onChain((message) => {
      if (message.type === "revert" && message.chain_id === chain) {
        push((state) => revertRef.current(state, message.rollback_to));
      }
    });
    // Load now so the page fills even without a live stream, and again
    // whenever the socket (re)opens, after the subscription was sent.
    const offOpen = live.onOpen(reload);
    reload();
    return () => {
      generation++;
      unsubscribe();
      offChain();
      offOpen();
    };
    // load, apply and revert are read through refs; empty is a constant.
  }, [channel]);

  const update = useCallback((change: (state: T) => T) => setData(change), []);
  return { data, loading, error, update };
}

const NO_CANDLES: Candle[] = [];
const NO_TRADES: Trade[] = [];
export const CANDLE_PAGE = 500;
export const TRADE_LIMIT = 200;

export function useLiveCandles(chain: number, subject: "token" | "pool", key: string, interval: string) {
  const channel = channels.candles(chain, subject, key, interval);
  const stream = useStream<Candle[]>(
    channel,
    () => api.candles(chain, subject, key, interval, CANDLE_PAGE),
    (candles, message) => {
      if (message.type === "candle") return applyCandle(candles, message.candle);
      if (message.type === "candle_reset") return resetCandles(candles, message.candle);
      return candles;
    },
    (candles) => candles, // a candle_reset per series follows every revert
    NO_CANDLES,
  );
  const [exhausted, setExhausted] = useState(false);
  const loadingOlder = useRef(false);
  useEffect(() => setExhausted(false), [channel]);

  // loadOlder pages back from the oldest bucket held.
  const first = stream.data[0]?.open_time;
  const loadOlder = useCallback(async () => {
    if (first === undefined || exhausted || loadingOlder.current) return;
    loadingOlder.current = true;
    try {
      const older = await api.candles(chain, subject, key, interval, CANDLE_PAGE, first);
      if (older.length < CANDLE_PAGE) setExhausted(true);
      if (older.length > 0) stream.update((candles) => prependCandles(candles, older));
    } finally {
      loadingOlder.current = false;
    }
  }, [chain, subject, key, interval, first, exhausted, stream]);

  return { ...stream, loadOlder };
}

export function useLiveTrades(chain: number, subject: "token" | "pool", key: string) {
  return useStream<Trade[]>(
    channels.trades(chain, subject, key),
    () => api.trades(chain, subject, key, TRADE_LIMIT),
    (trades, message) => (message.type === "trades" ? mergeTrades(trades, message.trades, TRADE_LIMIT) : trades),
    revertTrades,
    NO_TRADES,
  );
}

export function useLiveStatus(): LiveStatus {
  return useSyncExternalStore(
    (notify) => live.onStatus(notify),
    () => live.status,
  );
}

const heads = new Map<number, ChainMessage & { type: "head" }>();
const headListeners = new Set<() => void>();
live.onChain((message) => {
  if (message.type !== "head") return;
  heads.set(message.chain_id, message);
  for (const listener of headListeners) listener();
});

// useHead returns the newest pushed block of a chain, once a subscription
// on it has delivered one.
export function useHead(chain: number) {
  return useSyncExternalStore(
    (notify) => {
      headListeners.add(notify);
      return () => headListeners.delete(notify);
    },
    () => heads.get(chain),
  );
}

// useTokenMeta reads registry metadata, which rarely changes. The native
// pseudo-address falls back to the chain's coin.
export function useTokenMeta(chain: number, token: string | undefined) {
  return useQuery({
    queryKey: ["token", chain, token],
    queryFn: () => api.token(chain, token!),
    enabled: Boolean(token),
    staleTime: 5 * 60_000,
  });
}

export function useTokenSymbol(chain: number, token: string): string {
  const meta = useTokenMeta(chain, token);
  if (meta.data?.symbol) return meta.data.symbol;
  if (token === NATIVE_ADDRESS) return chainById(chain)?.nativeSymbol ?? "native";
  return token.slice(0, 6);
}

export function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now() / 1000);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now() / 1000), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs]);
  return now;
}
