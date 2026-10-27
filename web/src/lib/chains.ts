// The chains market-api serves, as configured in its chains section.

export interface Chain {
  id: number;
  name: string;
  short: string;
  nativeSymbol: string;
  wrappedNative: string;
  explorer: string;
}

// NATIVE_ADDRESS is chainstream-evm's pseudo-address for the chain's coin.
export const NATIVE_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

export const CHAINS: Chain[] = [
  {
    id: 1,
    name: "Ethereum",
    short: "ETH",
    nativeSymbol: "ETH",
    wrappedNative: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    explorer: "https://etherscan.io",
  },
  {
    id: 56,
    name: "BNB Chain",
    short: "BSC",
    nativeSymbol: "BNB",
    wrappedNative: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    explorer: "https://bscscan.com",
  },
  {
    id: 8453,
    name: "Base",
    short: "Base",
    nativeSymbol: "ETH",
    wrappedNative: "0x4200000000000000000000000000000000000006",
    explorer: "https://basescan.org",
  },
];

export const DEFAULT_CHAIN = CHAINS[0]!;

export function chainById(id: number): Chain | undefined {
  return CHAINS.find((chain) => chain.id === id);
}

// canonicalToken maps the native pseudo-address to the wrapped token, the
// way market-api does for prices, candles, trades and live channels.
export function canonicalToken(chain: Chain, address: string): string {
  const lower = address.toLowerCase();
  return lower === NATIVE_ADDRESS ? chain.wrappedNative : lower;
}

// INTERVALS are market-api's default api.intervals (24h), named the way
// the API names them.
export const INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d"] as const;
export type Interval = (typeof INTERVALS)[number];

export function intervalSeconds(interval: string): number {
  const match = /^(\d+)([smhd])$/.exec(interval);
  if (!match) return 60;
  const unit = { s: 1, m: 60, h: 3600, d: 86400 }[match[2] as "s" | "m" | "h" | "d"];
  return Number(match[1]) * unit;
}
