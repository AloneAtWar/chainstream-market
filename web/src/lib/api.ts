// Types and fetchers for market-api's REST routes. Decimal values arrive as
// strings and stay strings until they are formatted.

export interface RankedToken {
  token: string;
  name: string | null;
  symbol: string | null;
  decimals: number | null;
  ranked: boolean;
  price_usd: string | null;
  price_height: number | null;
  price_change_24h: string;
  volume_24h_usd: string;
  trades_24h: number;
  liquidity_usd: string;
  holders: number;
  created_height: number | null;
  created_time: number | null;
}

export interface TokenList {
  chain_id: number;
  sort: string;
  order: "asc" | "desc";
  refreshed_at: number | null;
  tokens: RankedToken[];
}

export type RankingSort = "volume_24h" | "price_change_24h" | "liquidity" | "holders" | "created";

export interface TokenListQuery {
  sort?: RankingSort;
  order?: "asc" | "desc";
  q?: string;
  minLiquidityUsd?: string;
  limit?: number;
  offset?: number;
}

export interface Deployment {
  block_height: number;
  block_time: number;
  transaction_index: number;
  transaction_hash: string;
  trace_id: string;
}

export interface TokenMeta {
  chain_id: number;
  address: string;
  standard: string;
  recognition: string;
  metadata_complete: boolean;
  name: string | null;
  symbol: string | null;
  decimals: number | null;
  created: boolean;
  deployment?: Deployment;
  source_height: number;
  first_height: number;
  updated_height: number;
  metadata?: unknown;
}

export interface Pool {
  chain_id: number;
  pool_id: string;
  protocol: string;
  dex_address: string;
  tokens: string[];
  created: boolean;
  source_height: number;
  source_time: number;
  first_height: number;
  updated_height: number;
  metadata?: unknown;
}

// TokenAmount is a raw integer amount and, when the token's decimals are
// known, its value in whole tokens.
export interface TokenAmount {
  raw: string;
  value: string | null;
}

export interface LiquiditySegment {
  tick_lower: number;
  tick_upper: number;
  liquidity: string;
  amounts: TokenAmount[];
}

// PoolLiquidity is a concentrated pool's tick table expanded into price
// ranges at its current price. Segments ascend by tick and hold only the
// ranges with liquidity; amounts and decimals follow the pool's token order.
export interface PoolLiquidity {
  chain_id: number;
  pool_id: string;
  protocol: string;
  fee: number;
  snapshot_height: number;
  sqrt_price_x96: string;
  tokens: string[] | null;
  decimals: (number | null)[] | null;
  usable: boolean;
  stale: boolean;
  swap_delta_hooks: boolean;
  amounts: TokenAmount[];
  depth: { band_bps: number; amounts: TokenAmount[] };
  segments: LiquiditySegment[];
}

export interface Price {
  chain_id: number;
  token: string;
  price_usd: string;
  source: string;
  hops: number;
  liquidity_usd: string;
  block_height: number;
  block_time: number;
}

export interface Candle {
  open_time: number;
  open: string;
  high: string;
  low: string;
  close: string;
  volume_base: string;
  volume_usd: string;
  trades: number;
  update_height?: number;
}

export type Side = "buy" | "sell" | "liquidity_added" | "liquidity_removed" | "unknown";

export interface Trade {
  block_height: number;
  block_time: number;
  // revision_height is the block that emitted a live trade; REST trades
  // leave it out.
  revision_height?: number;
  transaction_hash: string;
  log_index: number;
  operation_index: number;
  protocol: string;
  pool_id: string;
  trader: string;
  side: Side;
  base_token: string;
  quote_token: string;
  base_amount: string;
  quote_amount: string;
  price_quote: string;
  price_usd: string;
  value_usd: string;
  outlier: boolean;
  // trader_labels are market-wallet's labels of the trader, such as
  // smart_money or bot, when it has any.
  trader_labels?: string[];
}

export interface Holders {
  chain_id: number;
  token: string;
  decimals?: number;
  holder_count: number;
  holders: { holder: string; balance_raw: string; balance?: string; labels?: string[] }[];
}

export interface Transfer {
  block_height: number;
  block_time: number;
  transaction_hash: string;
  transaction_index: number;
  log_index: number;
  token: string;
  from: string;
  to: string;
  amount_raw: string;
}

export interface TransferCursor {
  before_height: number;
  before_log_index: number;
}

export interface Transfers {
  chain_id: number;
  transfers: Transfer[];
  next?: TransferCursor;
}

// Wallet stats, positions and ledger trades come from market-wallet. Every
// stat covers one window of days counted back from the newest trade.

export interface RoiDistribution {
  over_5x: number;
  from_2x_to_5x: number;
  from_0_to_2x: number;
  loss_0_to_50: number;
  loss_over_50: number;
}

export interface WalletStats {
  window_days: number;
  realized_usd: string;
  bought_usd: string;
  sold_usd: string;
  roi: string;
  buys: number;
  sells: number;
  // win_rate is the share of PnL-counted sells that made money.
  win_rate: number;
  wins: number;
  counted_sells: number;
  tokens: number;
  roi_distribution: RoiDistribution;
  closes: number;
  average_hold_seconds: number;
  fast_closes: number;
}

export interface RankedWallet extends WalletStats {
  wallet: string;
  labels: string[];
}

export type WalletSort = "realized" | "winrate" | "roi" | "volume";

export interface WalletList {
  chain_id: number;
  window_days: number;
  sort: WalletSort;
  label: string;
  refreshed_at: number | null;
  wallets: RankedWallet[];
}

export interface WalletProfile {
  chain_id: number;
  wallet: string;
  labels: string[];
  refreshed_at: number | null;
  stats: WalletStats[];
}

export interface Position {
  wallet: string;
  token: string;
  symbol: string | null;
  status: "open" | "closed";
  quantity: string;
  average_cost_usd: string;
  cost_usd: string;
  price_usd: string | null;
  realized_usd: string;
  unrealized_usd: string | null;
  bought_quantity: string;
  sold_quantity: string;
  bought_usd: string;
  sold_usd: string;
  buys: number;
  sells: number;
  wins: number;
  losses: number;
  first_buy_height: number | null;
  first_buy_time: number | null;
  opened_at: number | null;
  last_trade_time: number;
}

export interface Trader extends Position {
  labels: string[];
}

export type WalletEvent = "open" | "add" | "reduce" | "close" | "untracked" | "unknown";

export interface WalletTrade {
  block_height: number;
  block_time: number;
  transaction_hash: string;
  log_index: number;
  operation_index: number;
  pool_id: string;
  token: string;
  quote_token: string;
  side: Side;
  event: WalletEvent;
  amount: string;
  value_usd: string;
  price_usd: string;
  realized_usd: string;
  // counted marks a sell that counts toward realized PnL and win rate.
  counted: boolean;
  hold_seconds: number;
  quantity_after: string;
  cost_after_usd: string;
}

export interface WalletTradeCursor {
  before_height: number;
  before_log_index: number;
  before_operation_index: number;
}

export interface WalletTrades {
  chain_id: number;
  wallet: string;
  trades: WalletTrade[];
  next?: WalletTradeCursor;
}

export interface WalletListQuery {
  window?: number;
  sort?: WalletSort;
  label?: string;
  limit?: number;
}

export type LaunchpadColumn = "new" | "bonding" | "migrated";
export type LaunchpadStatus = "bonding" | "completed" | "migrated";

// LaunchpadToken is a bonding-curve token from market-launchpad. progress is
// a fraction (1 = sold out); base_remaining, quote_raised and price are raw
// integers, price in raw quote units per 1e18 raw token units.
export interface LaunchpadToken {
  token: string;
  protocol: string;
  pool_id: string;
  manager: string;
  quote: string;
  quote_symbol: string | null;
  quote_decimals: number | null;
  creator: string | null;
  name: string | null;
  symbol: string | null;
  decimals: number | null;
  meta_uri: string | null;
  status: LaunchpadStatus;
  launch_height: number;
  launch_time: number;
  launch_seen: boolean;
  progress: string | null;
  base_remaining: string | null;
  quote_raised: string | null;
  price: string | null;
  trades: number;
  buys: number;
  sells: number;
  last_trade_time: number | null;
  completed_time: number | null;
  migrated_time: number | null;
  target_pool_id: string | null;
  price_usd: string | null;
  volume_24h_usd: string | null;
  holders: number | null;
}

export interface LaunchpadList {
  chain_id: number;
  column: LaunchpadColumn;
  head_time: number | null;
  tokens: LaunchpadToken[];
}

export interface LaunchpadQuery {
  protocol?: string;
  limit?: number;
  maxAge?: string;
  stale?: string;
  migratedAge?: string;
  minProgress?: string;
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function get<T>(path: string, params: Record<string, string | number | undefined> = {}): Promise<T> {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") query.set(key, String(value));
  }
  const url = query.size > 0 ? `${path}?${query}` : path;
  const response = await fetch(url);
  if (!response.ok) {
    let message = response.statusText;
    try {
      message = ((await response.json()) as { error?: string }).error ?? message;
    } catch {
      // Not JSON, e.g. a proxy error page.
    }
    throw new ApiError(response.status, message);
  }
  return (await response.json()) as T;
}

// orNull turns a 404 into null, for lookups of things that may not exist yet.
async function orNull<T>(request: Promise<T>): Promise<T | null> {
  try {
    return await request;
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}

const base = (chain: number) => `/v1/chains/${chain}`;

export const api = {
  tokenList: (chain: number, query: TokenListQuery) =>
    get<TokenList>(`${base(chain)}/tokens`, {
      sort: query.sort,
      order: query.order,
      q: query.q,
      min_liquidity_usd: query.minLiquidityUsd,
      limit: query.limit,
      offset: query.offset,
    }),
  token: (chain: number, token: string) => orNull(get<TokenMeta>(`${base(chain)}/tokens/${token}`)),
  price: (chain: number, token: string) => orNull(get<Price>(`${base(chain)}/tokens/${token}/price`)),
  tokenPools: (chain: number, token: string, limit = 50) =>
    get<{ pools: Pool[] }>(`${base(chain)}/tokens/${token}/pools`, { limit }).then((body) => body.pools),
  pool: (chain: number, pool: string) => orNull(get<Pool>(`${base(chain)}/pools/${pool}`)),
  // poolLiquidity is null for pools without a tick table: anything that is
  // not a concentrated-liquidity (V3/V4-style) pool.
  poolLiquidity: (chain: number, pool: string) => orNull(get<PoolLiquidity>(`${base(chain)}/pools/${pool}/liquidity`)),
  candles: (chain: number, subject: "token" | "pool", key: string, interval: string, limit: number, to?: number) =>
    get<{ interval: string; candles: Candle[] | null }>(`${base(chain)}/${subject}s/${key}/candles`, {
      interval,
      limit,
      to,
    }).then((body) => body.candles ?? []),
  trades: (chain: number, subject: "token" | "pool", key: string, limit = 100, beforeHeight?: number) =>
    get<{ trades: Trade[] }>(`${base(chain)}/${subject}s/${key}/trades`, {
      limit,
      before_height: beforeHeight,
    }).then((body) => body.trades),
  launchpadList: (chain: number, column: LaunchpadColumn, query: LaunchpadQuery = {}) =>
    get<LaunchpadList>(`${base(chain)}/launchpad/tokens`, {
      column,
      protocol: query.protocol,
      limit: query.limit,
      max_age: query.maxAge,
      stale: query.stale,
      migrated_age: query.migratedAge,
      min_progress: query.minProgress,
    }),
  launchpadToken: (chain: number, token: string) =>
    orNull(get<{ chain_id: number; head_time: number; token: LaunchpadToken }>(`${base(chain)}/launchpad/tokens/${token}`)).then(
      (body) => body?.token ?? null,
    ),
  holders: (chain: number, token: string, limit = 100) =>
    get<Holders>(`${base(chain)}/tokens/${token}/holders`, { limit }),
  transfers: (
    chain: number,
    subject: "token" | "holder",
    key: string,
    options: { limit?: number; cursor?: TransferCursor; token?: string; holder?: string } = {},
  ) =>
    get<Transfers>(`${base(chain)}/${subject}s/${key}/transfers`, {
      limit: options.limit ?? 50,
      before_height: options.cursor?.before_height,
      before_log_index: options.cursor?.before_log_index,
      token: options.token,
      holder: options.holder,
    }),
  walletList: (chain: number, query: WalletListQuery) =>
    get<WalletList>(`${base(chain)}/wallets`, {
      window: query.window,
      sort: query.sort,
      label: query.label,
      limit: query.limit,
    }),
  wallet: (chain: number, wallet: string) => get<WalletProfile>(`${base(chain)}/wallets/${wallet}`),
  walletPositions: (chain: number, wallet: string, status?: "open" | "closed", limit = 100) =>
    get<{ positions: Position[] }>(`${base(chain)}/wallets/${wallet}/positions`, { status, limit }).then(
      (body) => body.positions,
    ),
  walletTrades: (chain: number, wallet: string, options: { limit?: number; token?: string; cursor?: WalletTradeCursor } = {}) =>
    get<WalletTrades>(`${base(chain)}/wallets/${wallet}/trades`, {
      limit: options.limit ?? 50,
      token: options.token,
      before_height: options.cursor?.before_height,
      before_log_index: options.cursor?.before_log_index,
      before_operation_index: options.cursor?.before_operation_index,
    }),
  tokenTraders: (chain: number, token: string, limit = 100) =>
    get<{ traders: Trader[] }>(`${base(chain)}/tokens/${token}/traders`, { limit }).then((body) => body.traders),
};
