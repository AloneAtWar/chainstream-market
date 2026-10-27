import { useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import { api, type Candle, type Trade } from "./api";
import { chainById, NATIVE_ADDRESS } from "./chains";
import { channels, chainOfChannel, live, type ChainMessage, type ChannelMessage, type LiveStatus } from "./live";
import { applyCandle, mergeTrades, prependCandles, resetCandles, revertTrades } from "./series";

interface Stream<T> {
  data: T;
  loading: boolean;
  error: Error | null;
  update: (change: (state: T) => T) => void;
  // epoch changes whenever the data may no longer extend what an earlier
  // request saw: on a channel switch, a reload, a revert or a candle reset.
  // A request made in an older epoch is stale.
  epoch: () => number;
}

// useStream follows market-api's recipe for joining history and live data:
// subscribe, wait for the hub to confirm the channel, load history, then
// apply the messages that arrived meanwhile. It loads again on every
// confirmation, since the hub does not replay what it pushed while the
// socket was down, and after every revert of the channel's chain, since
// market-engine rewinds the store before it publishes the revert and REST
// rows do not say which block wrote them.
function useStream<T>(
  channel: string | null,
  load: () => Promise<T>,
  apply: (state: T, message: ChannelMessage) => T,
  revert: (state: T, rollbackTo: number) => T,
  empty: T,
): Stream<T> {
  const [data, setData] = useState<T>(empty);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const loadRef = useRef(load);
  const applyRef = useRef(apply);
  const revertRef = useRef(revert);
  loadRef.current = load;
  applyRef.current = apply;
  revertRef.current = revert;
  const epochRef = useRef(0);

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
      epochRef.current++;
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
    const unsubscribe = live.subscribe(
      channel,
      (message) => {
        if (message.type === "candle_reset") epochRef.current++;
        push((state) => applyRef.current(state, message));
      },
      reload,
    );
    const offChain = live.onChain((message) => {
      if (message.type !== "revert" || message.chain_id !== chain) return;
      push((state) => revertRef.current(state, message.rollback_to));
      reload();
    });
    // Also load before the confirmation, so the page fills without a live
    // stream; the confirmed load replaces it.
    reload();
    return () => {
      generation++;
      epochRef.current++;
      unsubscribe();
      offChain();
    };
    // load, apply and revert are read through refs; empty is a constant.
  }, [channel]);

  const update = useCallback((change: (state: T) => T) => setData(change), []);
  const epoch = useCallback(() => epochRef.current, []);
  return { data, loading, error, update, epoch };
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
    // A candle_reset per series follows every revert, and the reload
    // brings back what it resets to.
    (candles) => candles,
    NO_CANDLES,
  );
  // Both hold the epoch they apply to, so a channel switch or a reload
  // clears them.
  const [exhaustedAt, setExhaustedAt] = useState<number | null>(null);
  const loadingOlder = useRef<number | null>(null);

  // loadOlder pages back from the oldest bucket held. A page that returns
  // after the history was replaced belongs to other data and is dropped.
  const first = stream.data[0]?.open_time;
  const { epoch, update } = stream;
  const loadOlder = useCallback(async () => {
    const requested = epoch();
    if (first === undefined || exhaustedAt === requested || loadingOlder.current === requested) return;
    loadingOlder.current = requested;
    try {
      const older = await api.candles(chain, subject, key, interval, CANDLE_PAGE, first);
      if (epoch() !== requested) return;
      if (older.length < CANDLE_PAGE) setExhaustedAt(requested);
      if (older.length > 0) update((candles) => prependCandles(candles, older));
    } finally {
      if (loadingOlder.current === requested) loadingOlder.current = null;
    }
  }, [chain, subject, key, interval, first, exhaustedAt, epoch, update]);

  return { ...stream, loadOlder };
}

// useLiveTrades drops reverted live trades at once, before the reload
// replaces the rest: REST trades carry no revision_height, so a late trade
// loaded over REST cannot be told apart from one of its own block.
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
