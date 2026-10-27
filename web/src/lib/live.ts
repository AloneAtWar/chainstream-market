// One WebSocket to market-api's /v1/stream, shared by every view. Views
// subscribe to channels and are told each time the hub confirms the
// subscription, on every (re)connect, so they can reload history: the hub
// only pushes what happens after it registered the channel.

import type { Candle, Trade } from "./api";

export type LiveMessage =
  | { type: "trades"; channel: string; height: number; trades: Trade[] }
  | { type: "candle"; channel: string; candle: Candle }
  | { type: "candle_reset"; channel: string; candle: Candle | null }
  | { type: "head"; chain_id: number; height: number; finalized_height: number }
  | { type: "revert"; chain_id: number; rollback_to: number }
  | { type: "subscribed" | "unsubscribed"; channels?: string[] }
  | { type: "error"; error?: string };

export type ChannelMessage = Extract<LiveMessage, { channel: string }>;
export type ChainMessage = Extract<LiveMessage, { chain_id: number }>;
export type LiveStatus = "connecting" | "open" | "closed";

type ChannelListener = (message: ChannelMessage) => void;
type ChainListener = (message: ChainMessage) => void;
type ReadyListener = () => void;
type StatusListener = (status: LiveStatus) => void;

export const channels = {
  trades: (chain: number, subject: "token" | "pool", key: string) => `trades:${chain}:${subject}:${key.toLowerCase()}`,
  candles: (chain: number, subject: "token" | "pool", key: string, interval: string) =>
    `candles:${chain}:${subject}:${key.toLowerCase()}:${interval}`,
};

export function chainOfChannel(channel: string): number {
  return Number(channel.split(":")[1]);
}

export class LiveClient {
  private socket: WebSocket | null = null;
  private readonly subscriptions = new Map<string, Set<ChannelListener>>();
  private readonly chainListeners = new Set<ChainListener>();
  private readonly readyListeners = new Map<string, Set<ReadyListener>>();
  // Subscribe requests per channel the hub has not answered yet; a channel
  // is confirmed once its count is back to zero on the current socket.
  private readonly unanswered = new Map<string, number>();
  private readonly confirmed = new Set<string>();
  private readonly statusListeners = new Set<StatusListener>();
  private retry = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private idle: ReturnType<typeof setTimeout> | null = null;
  status: LiveStatus = "closed";

  // url is read on each connect, so the module loads outside a browser.
  constructor(private readonly url: () => string) {}

  // subscribe routes the channel's messages to listener. onReady runs each
  // time the hub confirms the channel, after which every change it stores
  // is pushed: history loaded from then on misses nothing.
  subscribe(channel: string, listener: ChannelListener, onReady?: ReadyListener): () => void {
    let listeners = this.subscriptions.get(channel);
    if (!listeners) {
      listeners = new Set();
      this.subscriptions.set(channel, listeners);
      this.readyListeners.set(channel, new Set());
      this.requestSubscribe([channel]);
    }
    listeners.add(listener);
    if (onReady) {
      this.readyListeners.get(channel)!.add(onReady);
      if (this.confirmed.has(channel)) {
        queueMicrotask(() => {
          if (this.readyListeners.get(channel)?.has(onReady)) onReady();
        });
      }
    }
    this.ensureConnected();
    return () => {
      const current = this.subscriptions.get(channel);
      if (!current) return;
      current.delete(listener);
      if (onReady) this.readyListeners.get(channel)?.delete(onReady);
      if (current.size === 0) {
        this.subscriptions.delete(channel);
        this.readyListeners.delete(channel);
        this.confirmed.delete(channel);
        this.send({ op: "unsubscribe", channels: [channel] });
        this.closeWhenIdle();
      }
    };
  }

  // onChain receives head and revert messages of every subscribed chain.
  onChain(listener: ChainListener): () => void {
    this.chainListeners.add(listener);
    return () => this.chainListeners.delete(listener);
  }

  onStatus(listener: StatusListener): () => void {
    this.statusListeners.add(listener);
    return () => this.statusListeners.delete(listener);
  }

  private setStatus(status: LiveStatus) {
    this.status = status;
    for (const listener of this.statusListeners) listener(status);
  }

  private send(request: { op: "subscribe" | "unsubscribe"; channels: string[] }): boolean {
    if (this.socket?.readyState !== WebSocket.OPEN) return false;
    this.socket.send(JSON.stringify(request));
    return true;
  }

  private requestSubscribe(channels: string[]) {
    if (channels.length === 0 || !this.send({ op: "subscribe", channels })) return;
    for (const channel of channels) this.unanswered.set(channel, (this.unanswered.get(channel) ?? 0) + 1);
  }

  private resetConfirmations() {
    this.unanswered.clear();
    this.confirmed.clear();
  }

  private ensureConnected() {
    if (this.idle) {
      clearTimeout(this.idle);
      this.idle = null;
    }
    if (this.socket || this.timer) return;
    this.connect();
  }

  // closeWhenIdle closes the socket shortly after the last subscription
  // goes, so navigating between pages keeps it open.
  private closeWhenIdle() {
    if (this.subscriptions.size > 0 || this.idle) return;
    this.idle = setTimeout(() => {
      this.idle = null;
      if (this.subscriptions.size > 0) return;
      const socket = this.socket;
      this.socket = null;
      this.resetConfirmations();
      socket?.close();
      this.setStatus("closed");
    }, 5000);
  }

  private connect() {
    this.setStatus("connecting");
    const socket = new WebSocket(this.url());
    this.socket = socket;
    socket.onopen = () => {
      this.retry = 0;
      this.resetConfirmations();
      this.setStatus("open");
      this.requestSubscribe([...this.subscriptions.keys()]);
    };
    socket.onmessage = (event) => {
      let message: LiveMessage;
      try {
        message = JSON.parse(String(event.data)) as LiveMessage;
      } catch {
        return;
      }
      this.dispatch(message);
    };
    socket.onclose = () => {
      if (this.socket !== socket) return;
      this.socket = null;
      this.resetConfirmations();
      this.setStatus("closed");
      if (this.subscriptions.size === 0) return;
      // Back off up to 30s; a slow-client close (1013) reconnects the same way.
      const delay = Math.min(30000, 500 * 2 ** this.retry++);
      this.timer = setTimeout(() => {
        this.timer = null;
        if (this.subscriptions.size > 0) this.connect();
      }, delay);
    };
  }

  private dispatch(message: LiveMessage) {
    if ("channel" in message) {
      for (const listener of this.subscriptions.get(message.channel) ?? []) listener(message);
      return;
    }
    if ("chain_id" in message) {
      for (const listener of this.chainListeners) listener(message);
      return;
    }
    if (message.type === "subscribed") {
      for (const channel of message.channels ?? []) this.confirm(channel);
      return;
    }
    if (message.type === "error") console.warn("live stream:", message.error);
  }

  // confirm counts one subscribed reply. A reply to an earlier request on
  // the channel, sent before an unsubscribe, does not confirm a later one.
  private confirm(channel: string) {
    const left = (this.unanswered.get(channel) ?? 0) - 1;
    if (left > 0) {
      this.unanswered.set(channel, left);
      return;
    }
    this.unanswered.delete(channel);
    if (left < 0 || !this.subscriptions.has(channel)) return;
    this.confirmed.add(channel);
    for (const listener of this.readyListeners.get(channel) ?? []) listener();
  }
}

function streamUrl(): string {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}/v1/stream`;
}

export const live = new LiveClient(streamUrl);
