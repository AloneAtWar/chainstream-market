// Display helpers for market-launchpad's bonding-curve tokens.

import type { LaunchpadToken } from "./api";
import type { Chain } from "./chains";
import { formatCompact, scaleRaw } from "./format";

// LAUNCHPAD_PROTOCOLS names the launchpads chainstream-evm parses, with the
// chains each runs on.
export const LAUNCHPAD_PROTOCOLS: { value: string; label: string; chains: number[] }[] = [
  { value: "fourmeme", label: "four.meme", chains: [56] },
  { value: "flap", label: "Flap", chains: [56, 8453] },
];

export function protocolLabel(protocol: string): string {
  return LAUNCHPAD_PROTOCOLS.find((item) => item.value === protocol)?.label ?? protocol;
}

// quoteSymbol names the quote a curve raises: the chain's coin for its
// wrapped token, the registry symbol otherwise.
export function quoteSymbol(chain: Chain, token: LaunchpadToken): string {
  if (token.quote === chain.wrappedNative) return chain.nativeSymbol;
  return token.quote_symbol ?? "quote";
}

// formatRaised writes how much quote a curve has raised, like 12.4 BNB.
export function formatRaised(chain: Chain, token: LaunchpadToken): string {
  if (token.quote_raised === null) return "—";
  const amount = scaleRaw(token.quote_raised, token.quote_decimals ?? 18);
  return `${formatCompact(amount, 2)} ${quoteSymbol(chain, token)}`;
}

// metaLink turns a launch's metadata reference into a link, when it is one.
export function metaLink(uri: string | null): string | null {
  if (!uri) return null;
  if (uri.startsWith("ipfs://")) return `https://ipfs.io/ipfs/${uri.slice("ipfs://".length)}`;
  if (/^https?:\/\//.test(uri)) return uri;
  return null;
}
