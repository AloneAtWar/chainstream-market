// One WebSocket to market-api's /v1/stream, shared by every view. Views
// subscribe to channels and are told when the connection (re)opens, so they
// can reload history: the hub only pushes what happens while it is
// connected.

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
type OpenListener = () => void;
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
  private readonly openListeners = new Set<OpenListener>();
  private readonly statusListeners = new Set<StatusListener>();
  private retry = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private idle: ReturnType<typeof setTimeout> | null = null;
  status: LiveStatus = "closed";

  // url is read on each connect, so the module loads outside a browser.
  constructor(private readonly url: () => string) {}

  subscribe(channel: string, listener: ChannelListener): () => void {
    let listeners = this.subscriptions.get(channel);
    if (!listeners) {
      listeners = new Set();
      this.subscriptions.set(channel, listeners);
      this.send({ op: "subscribe", channels: [channel] });
    }
    listeners.add(listener);
    this.ensureConnected();
    return () => {
      const current = this.subscriptions.get(channel);
      if (!current) return;
      current.delete(listener);
      if (current.size === 0) {
        this.subscriptions.delete(channel);
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

  // onOpen runs each time the socket opens with the subscriptions resent.
  onOpen(listener: OpenListener): () => void {
    this.openListeners.add(listener);
    return () => this.openListeners.delete(listener);
  }

  onStatus(listener: StatusListener): () => void {
    this.statusListeners.add(listener);
    return () => this.statusListeners.delete(listener);
  }

  private setStatus(status: LiveStatus) {
    this.status = status;
    for (const listener of this.statusListeners) listener(status);
  }

  private send(request: { op: "subscribe" | "unsubscribe"; channels: string[] }) {
    if (this.socket?.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify(request));
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
      this.setStatus("open");
      if (this.subscriptions.size > 0) {
        socket.send(JSON.stringify({ op: "subscribe", channels: [...this.subscriptions.keys()] }));
      }
      for (const listener of this.openListeners) listener();
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
    if (message.type === "error") console.warn("live stream:", message.error);
  }
}

function streamUrl(): string {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}/v1/stream`;
}

export const live = new LiveClient(streamUrl);
