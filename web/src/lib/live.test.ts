import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LiveClient, type ChannelMessage } from "./live";

class FakeSocket {
  static OPEN = 1;
  static sockets: FakeSocket[] = [];
  readyState = 0;
  sent: unknown[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;

  constructor(readonly url: string) {
    FakeSocket.sockets.push(this);
  }
  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  close() {
    this.readyState = 3;
    this.onclose?.();
  }
  open() {
    this.readyState = FakeSocket.OPEN;
    this.onopen?.();
  }
  receive(message: unknown) {
    this.onmessage?.({ data: JSON.stringify(message) });
  }
}

describe("LiveClient", () => {
  beforeEach(() => {
    FakeSocket.sockets = [];
    vi.stubGlobal("WebSocket", FakeSocket);
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("subscribes once per channel and routes messages to its listeners", () => {
    const client = new LiveClient(() => "ws://test/v1/stream");
    const first: ChannelMessage[] = [];
    const second: ChannelMessage[] = [];
    const offFirst = client.subscribe("trades:1:token:0xa", (message) => first.push(message));
    client.subscribe("trades:1:token:0xa", (message) => second.push(message));
    const socket = FakeSocket.sockets[0]!;
    socket.open();
    expect(socket.sent).toEqual([{ op: "subscribe", channels: ["trades:1:token:0xa"] }]);

    socket.receive({ type: "trades", channel: "trades:1:token:0xa", height: 5, trades: [] });
    socket.receive({ type: "trades", channel: "trades:1:token:0xb", height: 5, trades: [] });
    expect(first).toHaveLength(1);
    expect(second).toHaveLength(1);

    offFirst();
    expect(socket.sent).toHaveLength(1);
  });

  it("unsubscribes when the last listener leaves and closes when idle", () => {
    const client = new LiveClient(() => "ws://test/v1/stream");
    const off = client.subscribe("candles:1:token:0xa:1m", () => {});
    const socket = FakeSocket.sockets[0]!;
    socket.open();
    off();
    expect(socket.sent.at(-1)).toEqual({ op: "unsubscribe", channels: ["candles:1:token:0xa:1m"] });
    vi.advanceTimersByTime(5000);
    expect(client.status).toBe("closed");
    expect(FakeSocket.sockets).toHaveLength(1);
  });

  it("reports a channel ready only when the hub confirms it", () => {
    const client = new LiveClient(() => "ws://test/v1/stream");
    let ready = 0;
    client.subscribe("trades:56:pool:0xp", () => {}, () => ready++);
    const socket = FakeSocket.sockets[0]!;
    socket.open();
    expect(ready).toBe(0);
    socket.receive({ type: "subscribed", channels: ["trades:56:pool:0xp"] });
    expect(ready).toBe(1);
  });

  it("reconnects, resubscribes and reports ready again after the new confirmation", () => {
    const client = new LiveClient(() => "ws://test/v1/stream");
    let ready = 0;
    client.subscribe("trades:56:pool:0xp", () => {}, () => ready++);
    FakeSocket.sockets[0]!.open();
    FakeSocket.sockets[0]!.receive({ type: "subscribed", channels: ["trades:56:pool:0xp"] });
    FakeSocket.sockets[0]!.close();
    expect(client.status).toBe("closed");
    vi.advanceTimersByTime(500);
    const second = FakeSocket.sockets[1]!;
    second.open();
    expect(second.sent).toEqual([{ op: "subscribe", channels: ["trades:56:pool:0xp"] }]);
    expect(ready).toBe(1);
    second.receive({ type: "subscribed", channels: ["trades:56:pool:0xp"] });
    expect(ready).toBe(2);
  });

  it("does not take the reply to an earlier subscribe as confirming a later one", () => {
    const client = new LiveClient(() => "ws://test/v1/stream");
    const socket = (() => {
      const off = client.subscribe("trades:1:token:0xa", () => {});
      const opened = FakeSocket.sockets[0]!;
      opened.open();
      off();
      return opened;
    })();
    let ready = 0;
    client.subscribe("trades:1:token:0xa", () => {}, () => ready++);
    // Replies arrive in request order: subscribe, unsubscribe, subscribe.
    socket.receive({ type: "subscribed", channels: ["trades:1:token:0xa"] });
    socket.receive({ type: "unsubscribed", channels: ["trades:1:token:0xa"] });
    expect(ready).toBe(0);
    socket.receive({ type: "subscribed", channels: ["trades:1:token:0xa"] });
    expect(ready).toBe(1);
  });

  it("reports a channel already confirmed to a later listener", async () => {
    const client = new LiveClient(() => "ws://test/v1/stream");
    client.subscribe("trades:1:token:0xa", () => {});
    const socket = FakeSocket.sockets[0]!;
    socket.open();
    socket.receive({ type: "subscribed", channels: ["trades:1:token:0xa"] });
    let ready = 0;
    client.subscribe("trades:1:token:0xa", () => {}, () => ready++);
    await Promise.resolve();
    expect(ready).toBe(1);
    expect(socket.sent).toHaveLength(1);
  });

  it("passes head and revert messages to chain listeners", () => {
    const client = new LiveClient(() => "ws://test/v1/stream");
    const seen: unknown[] = [];
    client.onChain((message) => seen.push(message));
    client.subscribe("trades:1:token:0xa", () => {});
    const socket = FakeSocket.sockets[0]!;
    socket.open();
    socket.receive({ type: "head", chain_id: 1, height: 10, finalized_height: 2 });
    socket.receive({ type: "revert", chain_id: 1, rollback_to: 9 });
    socket.receive({ type: "subscribed", channels: ["trades:1:token:0xa"] });
    expect(seen).toEqual([
      { type: "head", chain_id: 1, height: 10, finalized_height: 2 },
      { type: "revert", chain_id: 1, rollback_to: 9 },
    ]);
  });
});
