// A stand-in for market-api with generated data, so the frontend can be
// developed and demoed without the backend stack. It serves the same REST
// routes and /v1/stream protocol, mints one block per second per chain, and
// pushes the trades and candles each block makes.
//
//   node mock/server.mjs            # listens on :8080, like market-api
//   PORT=8090 node mock/server.mjs

import { createServer } from "node:http";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT ?? 8080);
const INTERVALS = { "1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400 };
const HISTORY_MINUTES = 3 * 24 * 60;

let seed = 42;
function random() {
  seed = (seed + 0x6d2b79f5) | 0;
  let t = seed;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const pick = (items) => items[Math.floor(random() * items.length)];
const hex = (length) => Array.from({ length }, () => Math.floor(random() * 16).toString(16)).join("");
const address = () => `0x${hex(40)}`;

const CHAINS = {
  1: { native: "WETH", wrapped: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", nativePrice: 3400, usdc: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" },
  56: { native: "WBNB", wrapped: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", nativePrice: 610, usdc: "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d" },
  8453: { native: "WETH", wrapped: "0x4200000000000000000000000000000000000006", nativePrice: 3400, usdc: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913" },
};

const NAMES = [
  ["PEPE", "Pepe"], ["DOGE2", "Doge 2.0"], ["MOG", "Mog Coin"], ["TURBO", "Turbo"], ["BRETT", "Brett"],
  ["WIF", "dogwifhat"], ["NEIRO", "Neiro"], ["SPX", "SPX6900"], ["GIGA", "Gigachad"], ["APU", "Apu Apustaja"],
  ["MOODENG", "Moo Deng"], ["TOSHI", "Toshi"], ["DEGEN", "Degen"], ["HIGHER", "Higher"], ["KEYCAT", "Keyboard Cat"],
  ["LINK", "Chainlink"], ["UNI", "Uniswap"], ["AAVE", "Aave"], ["LDO", "Lido DAO"], ["ONDO", "Ondo"],
  ["CAKE", "PancakeSwap"], ["FLOKI", "Floki"], ["BABYDOGE", "Baby Doge"], ["SHIB", "Shiba Inu"], ["ENA", "Ethena"],
  ["AERO", "Aerodrome"], ["VIRTUAL", "Virtuals"], ["AIXBT", "aixbt"], ["ZORA", "Zora"], ["CHAD", "Chad"],
];

const PROTOCOLS = ["uniswap_v2", "uniswap_v3", "uniswap_v4", "pancakeswap_v3", "aerodrome"];
const now = () => Math.floor(Date.now() / 1000);

const state = {};
for (const [chainId, chain] of Object.entries(CHAINS)) {
  const id = Number(chainId);
  const height = { 1: 23_650_000, 56: 66_100_000, 8453: 37_400_000 }[id];
  const tokens = [];
  const pools = [];
  const quoteTokens = [
    { address: chain.wrapped, symbol: chain.native, name: `Wrapped ${chain.native.slice(1)}`, decimals: 18, price: chain.nativePrice },
    { address: chain.usdc, symbol: "USDC", name: "USD Coin", decimals: id === 56 ? 18 : 6, price: 1 },
  ];
  for (const quote of quoteTokens) tokens.push(makeToken(quote, height, true));
  for (const [symbol, name] of NAMES) {
    const price = 10 ** (random() * 9 - 8) * (1 + random() * 9);
    tokens.push(makeToken({ address: address(), symbol, name, decimals: pick([9, 18, 18, 18]), price }, height, false));
  }
  // Every token trades against the wrapped native coin, some also against
  // USDC; the wrapped native coin trades against USDC.
  const pairs = [[tokens[0], [quoteTokens[1]]], ...tokens.slice(2).map((token) => [token, random() < 0.4 ? quoteTokens : [quoteTokens[0]]])];
  for (const [token, quotes] of pairs) {
    for (const quote of quotes) {
      const pool = {
        pool_id: address(),
        protocol: pick(PROTOCOLS),
        dex_address: address(),
        tokens: [token.address, quote.address],
        created: true,
        source_height: height - Math.floor(random() * 100000),
        source_time: now() - Math.floor(random() * 86400 * 30),
        first_height: height - Math.floor(random() * 100000),
        updated_height: height,
        metadata: { fee: pick([100, 500, 3000, 10000]), tick_spacing: pick([1, 10, 60, 200]) },
        quote,
        base: token,
      };
      token.pools.push(pool);
      pools.push(pool);
    }
  }
  state[id] = { id, height, finalized: height - 64, tokens, pools, chain, wallets: null };
  for (const token of tokens) seedHistory(state[id], token);
}

// --- Wallets, like market-wallet's ledger and stats ---

const WINDOWS = [7, 30, 90];

function makeWallets(tokens) {
  const tradable = tokens.filter((token) => !token.quote);
  return Array.from({ length: 300 }, (_, index) => {
    // A few wallets are very good, most are about even, a tail loses.
    const skill = index < 40 ? 0.6 + random() * 0.4 : random() * 1.2 - 0.6;
    const bot = index >= 40 && random() < 0.08;
    const size = 10 ** (2.5 + random() * 2.5);
    const stats = {};
    for (const days of WINDOWS) {
      const scale = days / 30;
      const tokensTraded = Math.max(1, Math.round((bot ? 60 : 4 + random() * 30) * Math.sqrt(scale)));
      const bought = size * tokensTraded * (0.5 + random()) * scale;
      const roi = skill * (0.3 + random() * 1.5) * (bot ? 0.05 : 1);
      const counted = Math.max(1, Math.round(tokensTraded * (1.5 + random() * 3) * (bot ? 30 : 1)));
      const winRate = Math.min(0.95, Math.max(0.05, 0.5 + skill * 0.35 + (random() - 0.5) * 0.15));
      const wins = Math.round(counted * winRate);
      const spread = [0.08, 0.17, 0.4, 0.22, 0.13].map((share, bucket) => share * (1 + skill * (bucket < 2 ? 1.5 : bucket > 2 ? -0.8 : 0)));
      const sum = spread.reduce((total, value) => total + value, 0);
      const counts = spread.map((share) => Math.round((share / sum) * tokensTraded));
      const closes = Math.round(tokensTraded * (0.4 + random() * 0.5) * (bot ? 20 : 1));
      stats[days] = {
        window_days: days,
        realized_usd: decimalString(bought * roi),
        bought_usd: decimalString(bought),
        sold_usd: decimalString(bought * (1 + roi) * (0.6 + random() * 0.4)),
        roi: decimalString(roi),
        buys: Math.round(counted * (1 + random())),
        sells: counted,
        win_rate: wins / counted,
        wins,
        counted_sells: counted,
        tokens: tokensTraded,
        roi_distribution: { over_5x: counts[0], from_2x_to_5x: counts[1], from_0_to_2x: counts[2], loss_0_to_50: counts[3], loss_over_50: counts[4] },
        closes,
        average_hold_seconds: Math.round(bot ? 3 + random() * 30 : 600 + random() * 86400 * 4),
        fast_closes: bot ? Math.round(closes * 0.7) : Math.round(closes * random() * 0.1),
      };
    }
    const labels = [];
    const thirty = stats[30];
    if (bot) labels.push("bot");
    else if (Number(thirty.realized_usd) >= 5000 && thirty.win_rate >= 0.5 && thirty.tokens >= 10 && thirty.roi_distribution.loss_over_50 <= thirty.tokens * 0.3) labels.push("smart_money");
    const held = Array.from({ length: 3 + Math.floor(random() * 10) }, () => pick(tradable)).filter((token, at, all) => all.indexOf(token) === at);
    return { wallet: address(), labels, stats, size, skill, held, positions: null, trades: null };
  });
}

function walletStats(entry, days) {
  return entry.stats[days] ?? entry.stats[30];
}

// positionsOf builds a wallet's positions once, priced at the tokens' current prices.
function positionsOf(chain, entry) {
  if (!entry.positions) {
    entry.positions = entry.held.map((token, index) => {
      const open = index < Math.ceil(entry.held.length / 2);
      const bought = entry.size * (0.5 + random() * 2);
      const average = token.price * (1 - entry.skill * 0.5 + (random() - 0.5) * 0.4);
      const quantityBought = bought / average;
      const sold = open ? quantityBought * random() * 0.6 : quantityBought;
      const quantity = quantityBought - sold;
      const exit = average * (1 + entry.skill * (0.5 + random()) + (random() - 0.5) * 0.3);
      const opened = now() - Math.floor(random() * 86400 * 20);
      const sells = sold > 0 ? 1 + Math.floor(random() * 4) : 0;
      const wins = exit > average ? sells : Math.floor(sells * random() * 0.5);
      return {
        token,
        open,
        quantity,
        average,
        realized: sold * (exit - average),
        bought,
        boughtQuantity: quantityBought,
        sold,
        soldUsd: sold * exit,
        buys: 1 + Math.floor(random() * 5),
        sells,
        wins,
        losses: sells - wins,
        opened,
        last: opened + Math.floor(random() * (now() - opened)),
        firstBuyHeight: chain.height - Math.floor((now() - opened) / 12),
      };
    }).sort((left, right) => right.last - left.last);
  }
  return entry.positions;
}

function positionJSON(chain, entry, position) {
  const price = position.token.price;
  return {
    wallet: entry.wallet,
    token: position.token.address,
    symbol: position.token.symbol,
    status: position.open ? "open" : "closed",
    quantity: decimalString(position.open ? position.quantity : 0),
    average_cost_usd: decimalString(position.open ? position.average : 0),
    cost_usd: decimalString(position.open ? position.quantity * position.average : 0),
    price_usd: decimalString(price),
    realized_usd: decimalString(position.realized),
    unrealized_usd: position.open ? decimalString(position.quantity * (price - position.average)) : null,
    bought_quantity: decimalString(position.boughtQuantity),
    sold_quantity: decimalString(position.sold),
    bought_usd: decimalString(position.bought),
    sold_usd: decimalString(position.soldUsd),
    buys: position.buys,
    sells: position.sells,
    wins: position.wins,
    losses: position.losses,
    first_buy_height: position.firstBuyHeight,
    first_buy_time: position.opened,
    opened_at: position.open ? position.opened : null,
    last_trade_time: position.last,
  };
}

// ledgerOf builds a wallet's ledger trades from its positions, newest first.
function ledgerOf(chain, entry) {
  if (!entry.trades) {
    const trades = [];
    for (const position of positionsOf(chain, entry)) {
      const pool = position.token.pools[0];
      const steps = [{ side: "buy", event: "open", share: 0.6, time: position.opened }];
      if (position.buys > 1) steps.push({ side: "buy", event: "add", share: 0.4, time: position.opened + (position.last - position.opened) * 0.3 });
      if (position.sold > 0) steps.push({ side: "sell", event: position.open ? "reduce" : "close", share: 1, time: position.last });
      let quantity = 0;
      for (const step of steps) {
        const buy = step.side === "buy";
        const amount = buy ? position.boughtQuantity * step.share : position.sold;
        const price = buy ? position.average : position.soldUsd / Math.max(position.sold, 1e-18);
        quantity += buy ? amount : -amount;
        trades.push({
          block_height: chain.height - Math.floor((now() - step.time) / 12),
          block_time: Math.floor(step.time),
          transaction_hash: `0x${hex(64)}`,
          log_index: Math.floor(random() * 400),
          operation_index: 0,
          pool_id: pool?.pool_id ?? address(),
          token: position.token.address,
          quote_token: pool?.quote.address ?? chain.chain.wrapped,
          side: step.side,
          event: step.event,
          amount: decimalString(amount),
          value_usd: decimalString(amount * price),
          price_usd: decimalString(price),
          realized_usd: decimalString(buy ? 0 : position.realized),
          counted: !buy,
          hold_seconds: step.event === "close" ? Math.floor(position.last - position.opened) : 0,
          quantity_after: decimalString(Math.max(0, quantity)),
          cost_after_usd: decimalString(Math.max(0, quantity) * position.average),
        });
      }
    }
    entry.trades = trades.sort((a, b) => b.block_height - a.block_height || b.log_index - a.log_index || b.operation_index - a.operation_index);
  }
  return entry.trades;
}

function findWallet(chain, value) {
  return chain.wallets.find((entry) => entry.wallet === value.toLowerCase());
}

function labelsOf(chain, wallet) {
  return findWallet(chain, wallet)?.labels ?? [];
}

function makeToken(info, height, quote) {
  const created = now() - Math.floor(random() * 86400 * (quote ? 2000 : 60));
  return {
    ...info,
    quote,
    created_time: created,
    created_height: height - Math.floor((now() - created) / 12),
    holders: Math.floor(10 ** (2 + random() * 4)),
    liquidity: info.price === 1 || quote ? 5e8 : 10 ** (3 + random() * 4),
    m1: [],
    trades: [],
    pools: [],
    topHolders: Array.from({ length: 60 }, () => ({ holder: address(), units: 10 ** (3 + random() * 7) })).sort((a, b) => b.units - a.units),
    transfers: [],
  };
}

function seedHistory(chain, token) {
  const start = Math.floor(now() / 60) * 60 - HISTORY_MINUTES * 60;
  const stable = token.price === 1 && token.quote;
  let price = stable ? 1 : token.price * (0.6 + random() * 0.8);
  const volatility = stable ? 0.0002 : token.quote ? 0.002 : 0.02;
  for (let minute = 0; minute < HISTORY_MINUTES; minute++) {
    if (random() < (token.quote ? 0.05 : 0.35)) continue; // a minute without trades
    const open = price;
    const close = stable ? 1 + (random() - 0.5) * volatility : Math.max(1e-12, open * Math.exp((random() - 0.5) * volatility * 2));
    const high = Math.max(open, close) * (1 + random() * volatility);
    const low = Math.min(open, close) / (1 + random() * volatility);
    const trades = 1 + Math.floor(random() * 12);
    const volume = (token.liquidity / 400) * (0.2 + random() * 2) * trades;
    token.m1.push({ open_time: start + minute * 60, open, high, low, close, volume_usd: volume, volume_base: volume / close, trades });
    price = close;
  }
  token.price = price;
  // Recent trades and transfers for the REST history.
  for (let index = 0; index < 120; index++) {
    const trade = makeTrade(chain, token, chain.height - 120 + index, now() - (120 - index) * 12);
    if (trade) token.trades.unshift(trade);
  }
  for (let index = 0; index < 80; index++) {
    token.transfers.unshift(makeTransfer(chain, token, chain.height - 80 + index, now() - (80 - index) * 12));
  }
}

function makeTrade(chain, token, height, time, revision) {
  const pool = token.pools.length > 0 ? pick(token.pools) : null;
  if (!pool) return null;
  const side = random() < 0.08 ? pick(["liquidity_added", "liquidity_removed"]) : random() < 0.52 ? "buy" : "sell";
  const value = (token.liquidity / 1000) * (0.05 + random() ** 3 * 8);
  const price = token.price * (1 + (random() - 0.5) * 0.004);
  return {
    block_height: height,
    block_time: time,
    ...(revision ? { revision_height: revision } : {}),
    transaction_hash: `0x${hex(64)}`,
    log_index: Math.floor(random() * 400),
    operation_index: 0,
    protocol: pool.protocol,
    pool_id: pool.pool_id,
    ...traderOf(chain),
    side,
    base_token: token.address,
    quote_token: pool.quote.address,
    base_amount: String(value / price),
    quote_amount: String(value / pool.quote.price),
    price_quote: String(price / pool.quote.price),
    price_usd: String(price),
    value_usd: String(value),
    outlier: random() < 0.01,
  };
}

// traderOf picks the trader: often one of the ranked wallets, so labeled
// trades show up; otherwise a fresh address.
function traderOf(chain) {
  if (!chain.wallets || random() < 0.6) return { trader: address() };
  // Smart money wallets are the first 40 and trade more often.
  const entry = random() < 0.5 ? chain.wallets[Math.floor(random() * 40)] : pick(chain.wallets);
  return { trader: entry.wallet, ...(entry.labels.length ? { trader_labels: entry.labels } : {}) };
}

function makeTransfer(chain, token, height, time) {
  const holders = token.topHolders;
  const units = 10 ** (random() * 6);
  return {
    block_height: height,
    block_time: time,
    transaction_hash: `0x${hex(64)}`,
    transaction_index: Math.floor(random() * 200),
    log_index: Math.floor(random() * 400),
    token: token.address,
    from: random() < 0.05 ? "0x0000000000000000000000000000000000000000" : pick(holders).holder,
    to: pick(holders).holder,
    amount_raw: BigInt(Math.floor(units * 1e6)).toString() + "0".repeat(Math.max(0, token.decimals - 6)),
  };
}

// candlesFor aggregates the minute candles into buckets of interval seconds.
function candlesFor(token, seconds) {
  const buckets = [];
  for (const candle of token.m1) {
    const openTime = Math.floor(candle.open_time / seconds) * seconds;
    const last = buckets[buckets.length - 1];
    if (last && last.open_time === openTime) {
      last.high = Math.max(last.high, candle.high);
      last.low = Math.min(last.low, candle.low);
      last.close = candle.close;
      last.volume_usd += candle.volume_usd;
      last.volume_base += candle.volume_base;
      last.trades += candle.trades;
    } else {
      buckets.push({ ...candle, open_time: openTime, open: last ? last.close : candle.open });
    }
  }
  return buckets;
}

// Like market-api, decimals are plain strings without an exponent.
const decimalString = (value) => value.toLocaleString("en-US", { maximumSignificantDigits: 12, useGrouping: false });

function candleJSON(candle, height) {
  return {
    open_time: candle.open_time,
    open: decimalString(candle.open),
    high: decimalString(candle.high),
    low: decimalString(candle.low),
    close: decimalString(candle.close),
    volume_base: decimalString(candle.volume_base),
    volume_usd: decimalString(candle.volume_usd),
    trades: candle.trades,
    ...(height ? { update_height: height } : {}),
  };
}

function ranking(chain, token) {
  const since = now() - 86400;
  const window = token.m1.filter((candle) => candle.open_time >= since);
  const volume = window.reduce((sum, candle) => sum + candle.volume_usd, 0);
  const trades = window.reduce((sum, candle) => sum + candle.trades, 0);
  const open = window[0]?.open ?? token.price;
  return {
    token: token.address,
    name: token.name,
    symbol: token.symbol,
    decimals: token.decimals,
    ranked: true,
    price_usd: decimalString(token.price),
    price_height: chain.height,
    price_change_24h: decimalString(token.price / open - 1),
    volume_24h_usd: decimalString(volume),
    trades_24h: trades,
    liquidity_usd: decimalString(token.liquidity),
    holders: token.holders,
    created_height: token.created_height,
    created_time: token.created_time,
  };
}

function tokenMeta(chain, token) {
  return {
    chain_id: chain.id,
    address: token.address,
    standard: "erc20",
    recognition: "recognized",
    metadata_complete: true,
    name: token.name,
    symbol: token.symbol,
    decimals: token.decimals,
    created: !token.quote,
    ...(token.quote
      ? {}
      : {
          deployment: {
            block_height: token.created_height,
            block_time: token.created_time,
            transaction_index: 3,
            transaction_hash: `0x${token.address.slice(2)}${"ab".repeat(12)}`,
            trace_id: "0",
          },
        }),
    source_height: chain.height,
    first_height: token.created_height,
    updated_height: chain.height,
    metadata: { name: token.name, symbol: token.symbol, decimals: token.decimals },
  };
}

function poolJSON(chain, pool) {
  const { quote, base, ...rest } = pool;
  return { chain_id: chain.id, ...rest };
}

// --- HTTP ---

function send(response, status, body) {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(body));
}

const routes = [
  [/^\/v1\/chains\/(\d+)\/launchpad\/tokens$/, (chain, _match, query) => {
    const column = query.get("column");
    const protocol = query.get("protocol") ?? "";
    const limit = Number(query.get("limit") ?? 50);
    const time = now();
    let rows = chain.launchpad.filter((token) => !protocol || token.protocol === protocol);
    if (column === "new") rows = rows.filter((token) => token.status === "bonding" && token.launch_time >= time - 86400);
    else if (column === "bonding") {
      rows = rows
        .filter((token) => token.status !== "migrated" && token.last_trade_time !== null && token.last_trade_time >= time - 3600)
        .sort((left, right) => right.progress - left.progress || right.last_trade_time - left.last_trade_time);
    } else if (column === "migrated") {
      rows = rows.filter((token) => token.status === "migrated").sort((left, right) => right.migrated_time - left.migrated_time);
    } else return [400, { error: "column must be new, bonding or migrated" }];
    const tokens = rows.slice(0, limit).map((token) => launchpadJSON(chain, token));
    return [200, { chain_id: chain.id, column, head_time: LAUNCHPADS[chain.id] ? time : null, tokens }];
  }],
  [/^\/v1\/chains\/(\d+)\/launchpad\/tokens\/(0x[0-9a-f]+)$/, (chain, match) => {
    const token = chain.launchpad.find((item) => item.token === match[2]);
    if (!token) return [404, { error: "not a launchpad token" }];
    return [200, { chain_id: chain.id, head_time: now(), token: launchpadJSON(chain, token) }];
  }],
  [/^\/v1\/chains\/(\d+)\/tokens$/, (chain, _match, query) => {
    const sort = query.get("sort") ?? "volume_24h";
    const ascending = query.get("order") === "asc";
    const q = (query.get("q") ?? "").toLowerCase();
    const minLiquidity = Number(query.get("min_liquidity_usd") ?? 0);
    const limit = Number(query.get("limit") ?? 200);
    const offset = Number(query.get("offset") ?? 0);
    const field = { volume_24h: "volume_24h_usd", price_change_24h: "price_change_24h", liquidity: "liquidity_usd", holders: "holders", created: "created_height" }[sort];
    if (!field) return [400, { error: "sort must be one of volume_24h, price_change_24h, liquidity, holders, created" }];
    let rows = chain.tokens.map((token) => ranking(chain, token));
    if (q) rows = rows.filter((row) => (q.startsWith("0x") && q.length === 42 ? row.token === q : row.symbol.toLowerCase().startsWith(q)));
    rows = rows.filter((row) => Number(row.liquidity_usd) >= minLiquidity);
    rows.sort((left, right) => (Number(right[field]) - Number(left[field])) * (ascending ? -1 : 1));
    return [200, { chain_id: chain.id, sort, order: ascending ? "asc" : "desc", refreshed_at: now() - 7, tokens: rows.slice(offset, offset + limit) }];
  }],
  [/^\/v1\/chains\/(\d+)\/tokens\/(0x[0-9a-f]+)$/, (chain, match) => {
    const token = findToken(chain, match[2]);
    return token ? [200, tokenMeta(chain, token)] : [404, { error: "unknown token" }];
  }],
  [/^\/v1\/chains\/(\d+)\/tokens\/(0x[0-9a-f]+)\/price$/, (chain, match) => {
    const token = findToken(chain, match[2]);
    if (!token) return [404, { error: "no price for token" }];
    return [200, {
      chain_id: chain.id, token: token.address, price_usd: decimalString(token.price),
      source: token.quote ? "anchor" : "graph", hops: token.quote ? 0 : 1,
      liquidity_usd: decimalString(token.liquidity), block_height: chain.height, block_time: now(),
    }];
  }],
  [/^\/v1\/chains\/(\d+)\/(tokens|pools)\/(0x[0-9a-f]+)\/candles$/, (chain, match, query) => {
    const token = match[2] === "tokens" ? findToken(chain, match[3]) : findPool(chain, match[3])?.base;
    const seconds = INTERVALS[query.get("interval") ?? ""];
    if (!seconds) return [400, { error: `interval must be one of ${Object.keys(INTERVALS).join(", ")}` }];
    const to = Number(query.get("to") ?? Infinity);
    const from = Number(query.get("from") ?? 0);
    const limit = Number(query.get("limit") ?? 200);
    const candles = token ? candlesFor(token, seconds).filter((candle) => candle.open_time >= from && candle.open_time < to).slice(-limit) : [];
    return [200, { interval: query.get("interval"), candles: candles.length ? candles.map((candle) => candleJSON(candle)) : null }];
  }],
  [/^\/v1\/chains\/(\d+)\/(tokens|pools)\/(0x[0-9a-f]+)\/trades$/, (chain, match, query) => {
    const limit = Number(query.get("limit") ?? 200);
    const before = Number(query.get("before_height") ?? Infinity);
    let trades = [];
    if (match[2] === "tokens") trades = findToken(chain, match[3])?.trades ?? [];
    else trades = findPool(chain, match[3])?.base.trades.filter((trade) => trade.pool_id === match[3]) ?? [];
    return [200, { trades: trades.filter((trade) => trade.block_height < before).slice(0, limit).map(({ revision_height, ...trade }) => trade) }];
  }],
  [/^\/v1\/chains\/(\d+)\/tokens\/(0x[0-9a-f]+)\/holders$/, (chain, match, query) => {
    const token = findToken(chain, match[2]);
    const limit = Number(query.get("limit") ?? 200);
    const holders = (token?.topHolders ?? []).slice(0, limit).map((row) => {
      const raw = BigInt(Math.floor(row.units * 1e6)).toString() + "0".repeat(Math.max(0, token.decimals - 6));
      const labels = labelsOf(chain, row.holder);
      return { holder: row.holder, balance_raw: raw, balance: decimalString(row.units), ...(labels.length ? { labels } : {}) };
    });
    return [200, { chain_id: chain.id, token: match[2], ...(token ? { decimals: token.decimals } : {}), holder_count: token?.holders ?? 0, holders }];
  }],
  [/^\/v1\/chains\/(\d+)\/tokens\/(0x[0-9a-f]+)\/pools$/, (chain, match) => {
    const pools = chain.pools.filter((pool) => pool.tokens.includes(match[2])).slice(0, 50);
    return [200, { pools: pools.map((pool) => poolJSON(chain, pool)) }];
  }],
  [/^\/v1\/chains\/(\d+)\/pools\/(0x[0-9a-f]+)$/, (chain, match) => {
    const pool = findPool(chain, match[2]);
    return pool ? [200, poolJSON(chain, pool)] : [404, { error: "unknown pool" }];
  }],
  [/^\/v1\/chains\/(\d+)\/wallets$/, (chain, _match, query) => {
    const days = Number(query.get("window") ?? 30);
    if (!Number.isInteger(days) || days < 1 || days > 90) return [400, { error: "window must be days between 1 and 90" }];
    const sort = query.get("sort") || "realized";
    const key = { realized: (s) => Number(s.realized_usd), winrate: (s) => s.win_rate, roi: (s) => Number(s.roi), volume: (s) => Number(s.bought_usd) + Number(s.sold_usd) }[sort];
    if (!key) return [400, { error: "sort must be one of realized, winrate, roi, volume" }];
    const label = query.get("label") ?? "";
    const limit = Number(query.get("limit") ?? 200);
    const rows = chain.wallets
      .filter((entry) => !label || entry.labels.includes(label))
      .map((entry) => ({ wallet: entry.wallet, labels: entry.labels, ...walletStats(entry, days), window_days: days }))
      .sort((left, right) => key(right) - key(left))
      .slice(0, limit);
    return [200, { chain_id: chain.id, window_days: days, sort, label, refreshed_at: rows.length ? now() - 240 : null, wallets: rows }];
  }],
  [/^\/v1\/chains\/(\d+)\/wallets\/(0x[0-9a-f]+)$/, (chain, match) => {
    const entry = findWallet(chain, match[2]);
    const stats = entry ? WINDOWS.map((days) => entry.stats[days]) : [];
    return [200, { chain_id: chain.id, wallet: match[2], labels: entry?.labels ?? [], refreshed_at: entry ? now() - 240 : null, stats }];
  }],
  [/^\/v1\/chains\/(\d+)\/wallets\/(0x[0-9a-f]+)\/positions$/, (chain, match, query) => {
    const entry = findWallet(chain, match[2]);
    const status = query.get("status");
    const limit = Number(query.get("limit") ?? 200);
    const positions = entry ? positionsOf(chain, entry).filter((position) => !status || (status === "open") === position.open) : [];
    return [200, { chain_id: chain.id, wallet: match[2], positions: positions.slice(0, limit).map((position) => positionJSON(chain, entry, position)) }];
  }],
  [/^\/v1\/chains\/(\d+)\/wallets\/(0x[0-9a-f]+)\/trades$/, (chain, match, query) => {
    const entry = findWallet(chain, match[2]);
    const limit = Number(query.get("limit") ?? 200);
    const token = query.get("token");
    const cursor = query.has("before_height")
      ? [Number(query.get("before_height")), Number(query.get("before_log_index") ?? Infinity), Number(query.get("before_operation_index") ?? Infinity)]
      : null;
    const before = (trade) =>
      !cursor ||
      trade.block_height < cursor[0] ||
      (trade.block_height === cursor[0] && (trade.log_index < cursor[1] || (trade.log_index === cursor[1] && trade.operation_index < cursor[2])));
    const trades = (entry ? ledgerOf(chain, entry) : []).filter((trade) => (!token || trade.token === token) && before(trade));
    const page = trades.slice(0, limit);
    const last = page[page.length - 1];
    const next = page.length === limit && last ? { before_height: last.block_height, before_log_index: last.log_index, before_operation_index: last.operation_index } : undefined;
    return [200, { chain_id: chain.id, wallet: match[2], trades: page, ...(next ? { next } : {}) }];
  }],
  [/^\/v1\/chains\/(\d+)\/tokens\/(0x[0-9a-f]+)\/traders$/, (chain, match, query) => {
    const limit = Number(query.get("limit") ?? 200);
    const rows = [];
    for (const entry of chain.wallets) {
      if (!entry.held.some((token) => token.address === match[2])) continue;
      const position = positionsOf(chain, entry).find((item) => item.token.address === match[2]);
      if (position) rows.push({ labels: entry.labels, ...positionJSON(chain, entry, position) });
    }
    rows.sort((left, right) => Number(right.realized_usd) - Number(left.realized_usd));
    return [200, { chain_id: chain.id, token: match[2], traders: rows.slice(0, limit) }];
  }],
  [/^\/v1\/chains\/(\d+)\/(tokens|holders)\/(0x[0-9a-f]+)\/transfers$/, (chain, match, query) => {
    const limit = Number(query.get("limit") ?? 200);
    const before = Number(query.get("before_height") ?? Infinity);
    let transfers;
    if (match[2] === "tokens") transfers = findToken(chain, match[3])?.transfers ?? [];
    else transfers = chain.tokens.flatMap((token) => token.transfers.filter((row) => row.from === match[3] || row.to === match[3]));
    transfers = transfers.filter((row) => row.block_height < before).sort((a, b) => b.block_height - a.block_height || b.log_index - a.log_index);
    const page = transfers.slice(0, limit);
    const last = page[page.length - 1];
    return [200, { chain_id: chain.id, transfers: page, ...(page.length === limit && last ? { next: { before_height: last.block_height, before_log_index: last.log_index } } : {}) }];
  }],
];

// --- Launchpad ---
//
// four.meme runs on BSC and Flap on BSC and Base; each mock block may launch
// a curve, and every open curve trades a little, sells out at 100% and
// migrates a few blocks later.

const LAUNCHPADS = { 56: ["fourmeme", "flap"], 8453: ["flap"] };
const LAUNCH_NAMES = ["CAT", "MOON", "FROG", "AI", "PUMP", "BULL", "GEM", "RICH", "DRAGON", "PANDA", "ROCKET", "LUCKY"];

function launchCurve(chain, time) {
  const protocols = LAUNCHPADS[chain.id];
  if (!protocols) return;
  const symbol = `${pick(LAUNCH_NAMES)}${Math.floor(random() * 1000)}`;
  const token = {
    token: address(), protocol: pick(protocols), manager: address(), quote: chain.chain.wrapped,
    quote_symbol: chain.chain.native, quote_decimals: 18, creator: address(), name: `${symbol} Coin`, symbol,
    decimals: 18, meta_uri: `ipfs://bafy${hex(20)}`, status: "bonding", launch_height: chain.height,
    launch_time: time, launch_seen: true, progress: 0, trades: 0, buys: 0, sells: 0, last_trade_time: null,
    completed_time: null, migrated_time: null, target_pool_id: null, holders: 1,
    // Some curves catch on, most fade.
    heat: random() < 0.25 ? 0.03 + random() * 0.05 : random() * 0.01,
  };
  token.pool_id = `${token.manager}:${token.token}`;
  chain.launchpad.unshift(token);
}

function stepCurves(chain, time) {
  if (!LAUNCHPADS[chain.id]) return;
  if (random() < 0.5) launchCurve(chain, time);
  for (const token of chain.launchpad) {
    if (token.status === "completed" && time - token.completed_time >= 3) {
      token.status = "migrated";
      token.migrated_time = time;
      token.target_pool_id = address();
    }
    if (token.status !== "bonding" || random() > 0.6) continue;
    const buy = random() < 0.62;
    token.progress = Math.min(1, Math.max(0, token.progress + (buy ? 1 : -0.6) * token.heat * random()));
    token.trades++;
    if (buy) token.buys++;
    else token.sells++;
    token.holders += buy && random() < 0.7 ? 1 : 0;
    token.last_trade_time = time;
    if (token.progress >= 1) {
      token.status = "completed";
      token.completed_time = time;
    }
  }
  chain.launchpad = chain.launchpad.slice(0, 400);
}

function launchpadJSON(chain, token) {
  const { heat, progress, ...rest } = token;
  // The curve raises about 24 of the chain's coin; price follows progress.
  const raised = BigInt(Math.floor(progress * 24e6)) * 10n ** 12n;
  const priceUsd = 4e-6 * (1 + progress * 9) * (chain.id === 56 ? 1 : 2);
  return {
    ...rest,
    progress: decimalString(progress),
    base_remaining: (BigInt(Math.floor((1 - progress) * 8e8)) * 10n ** 18n).toString(),
    quote_raised: raised.toString(),
    price: String(Math.floor(priceUsd * 1e18 / chain.chain.nativePrice)),
    price_usd: token.trades > 0 ? decimalString(priceUsd) : null,
    volume_24h_usd: token.trades > 0 ? decimalString(token.trades * 37.5) : null,
  };
}

function seedCurves(chain) {
  for (let second = 600; second > 0; second--) stepCurves(chain, now() - second);
}

for (const chain of Object.values(state)) {
  chain.launchpad = [];
  seedCurves(chain);
}

function findToken(chain, value) {
  return chain.tokens.find((token) => token.address === value.toLowerCase());
}

function findPool(chain, value) {
  return chain.pools.find((pool) => pool.pool_id === value.toLowerCase());
}

const server = createServer((request, response) => {
  const url = new URL(request.url ?? "/", "http://localhost");
  if (url.pathname === "/healthz") return void response.writeHead(204).end();
  for (const [pattern, handler] of routes) {
    const match = pattern.exec(url.pathname.toLowerCase());
    if (!match) continue;
    const chain = state[Number(match[1])];
    if (!chain) return send(response, 400, { error: "chain must be a positive chain id" });
    const [status, body] = handler(chain, match, url.searchParams);
    return send(response, status, body);
  }
  send(response, 404, { error: "not found" });
});

// --- Live stream ---

const sockets = new Map(); // socket -> Set of channels
const wss = new WebSocketServer({ server, path: "/v1/stream" });
wss.on("connection", (socket) => {
  sockets.set(socket, new Set());
  socket.on("message", (data) => {
    let request;
    try {
      request = JSON.parse(String(data));
    } catch {
      return void socket.send(JSON.stringify({ type: "error", error: "invalid request" }));
    }
    const channels = (request.channels ?? []).map((channel) => channel.toLowerCase());
    const held = sockets.get(socket);
    for (const channel of channels) {
      if (request.op === "subscribe") held.add(channel);
      else held.delete(channel);
    }
    socket.send(JSON.stringify({ type: request.op === "subscribe" ? "subscribed" : "unsubscribed", channels }));
  });
  socket.on("close", () => sockets.delete(socket));
});

function publish(channel, message) {
  const text = JSON.stringify(message);
  for (const [socket, channels] of sockets) if (channels.has(channel)) socket.send(text);
}

function publishChain(chainId, message) {
  const text = JSON.stringify(message);
  for (const [socket, channels] of sockets) {
    if ([...channels].some((channel) => channel.split(":")[1] === String(chainId))) socket.send(text);
  }
}

function mineBlock(chain) {
  chain.height += 1;
  chain.finalized = chain.height - 64;
  const time = now();
  stepCurves(chain, time);
  const active = chain.tokens.filter(() => random() < 0.08);
  for (const token of active) {
    if (token.symbol === "USDC") token.price = 1 + (random() - 0.5) * 0.0002;
    else token.price *= Math.exp((random() - 0.5) * (token.quote ? 0.001 : 0.01));
    const trades = [];
    for (let count = 1 + Math.floor(random() * 3); count > 0; count--) {
      const trade = makeTrade(chain, token, chain.height, time, chain.height);
      if (!trade) continue;
      trades.push(trade);
      token.trades.unshift(trade);
      if (trade.side === "buy" || trade.side === "sell") addToCandle(token, trade);
    }
    token.trades.length = Math.min(token.trades.length, 500);
    if (random() < 0.3) token.transfers.unshift(makeTransfer(chain, token, chain.height, time));
    if (trades.length === 0) continue;
    publish(`trades:${chain.id}:token:${token.address}`, { type: "trades", channel: `trades:${chain.id}:token:${token.address}`, height: chain.height, trades });
    for (const trade of trades) {
      for (const channel of [`trades:${chain.id}:wallet:${trade.trader}`, ...(trade.trader_labels ?? []).map((label) => `trades:${chain.id}:label:${label}`)]) {
        publish(channel, { type: "trades", channel, height: chain.height, trades: [trade] });
      }
    }
    for (const pool of token.pools) {
      const poolTrades = trades.filter((trade) => trade.pool_id === pool.pool_id);
      if (poolTrades.length > 0) {
        const channel = `trades:${chain.id}:pool:${pool.pool_id}`;
        publish(channel, { type: "trades", channel, height: chain.height, trades: poolTrades });
      }
    }
    for (const [name, seconds] of Object.entries(INTERVALS)) {
      const current = candlesFor({ m1: token.m1.filter((candle) => candle.open_time >= Math.floor(time / seconds) * seconds - seconds) }, seconds).at(-1);
      if (!current) continue;
      const candle = candleJSON(current, chain.height);
      for (const [subject, key] of [["token", token.address], ...token.pools.map((pool) => ["pool", pool.pool_id])]) {
        const channel = `candles:${chain.id}:${subject}:${key}:${name}`;
        publish(channel, { type: "candle", channel, candle });
      }
    }
  }
  publishChain(chain.id, { type: "head", chain_id: chain.id, height: chain.height, finalized_height: chain.finalized });
}

function addToCandle(token, trade) {
  const price = Number(trade.price_usd);
  const openTime = Math.floor(trade.block_time / 60) * 60;
  const last = token.m1.at(-1);
  if (last && last.open_time === openTime) {
    last.high = Math.max(last.high, price);
    last.low = Math.min(last.low, price);
    last.close = price;
    last.volume_usd += Number(trade.value_usd);
    last.volume_base += Number(trade.base_amount);
    last.trades += 1;
  } else {
    const open = last ? last.close : price;
    token.m1.push({
      open_time: openTime, open, high: Math.max(open, price), low: Math.min(open, price), close: price,
      volume_usd: Number(trade.value_usd), volume_base: Number(trade.base_amount), trades: 1,
    });
  }
}

// Wallets are made once every helper above is defined; the seeded trades
// then get some of them as traders.
for (const chain of Object.values(state)) {
  chain.wallets = makeWallets(chain.tokens);
  for (const token of chain.tokens) {
    for (const trade of token.trades) {
      delete trade.trader_labels;
      Object.assign(trade, traderOf(chain));
    }
  }
}

setInterval(() => {
  for (const chain of Object.values(state)) mineBlock(chain);
}, 1000);

server.listen(PORT, () => {
  const sample = state[1].tokens[2];
  console.log(`mock market-api on http://localhost:${PORT}`);
  console.log(`  e.g. /v1/chains/1/tokens, /v1/chains/1/tokens/${sample.address}/candles?interval=1m`);
});
