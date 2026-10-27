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
}

export interface Holders {
  chain_id: number;
  token: string;
  decimals?: number;
  holder_count: number;
  holders: { holder: string; balance_raw: string; balance?: string }[];
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
};
