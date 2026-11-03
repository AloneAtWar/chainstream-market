// Display helpers for market-launchpad's bonding-curve tokens.

import type { LaunchpadToken } from "./api";
import { canonicalToken, type Chain } from "./chains";
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

// isNativeQuote reports a curve raising the chain's coin, given as either
// the wrapped token or the native pseudo-address.
function isNativeQuote(chain: Chain, token: LaunchpadToken): boolean {
  return canonicalToken(chain, token.quote) === chain.wrappedNative;
}

// quoteSymbol names the quote a curve raises: the chain's coin for its
// wrapped token, the registry symbol otherwise.
export function quoteSymbol(chain: Chain, token: LaunchpadToken): string {
  if (isNativeQuote(chain, token)) return chain.nativeSymbol;
  return token.quote_symbol ?? "quote";
}

// formatRaised writes how much quote a curve has raised, like 12.4 BNB. Only
// the chain's coin is known to have 18 decimals; another quote the registry
// has not described yet shows its raw amount, marked as such, since guessing
// its decimals can be off by many orders of magnitude.
export function formatRaised(chain: Chain, token: LaunchpadToken): string {
  if (token.quote_raised === null) return "—";
  const decimals = token.quote_decimals ?? (isNativeQuote(chain, token) ? 18 : null);
  if (decimals === null) return `${formatCompact(token.quote_raised, 2)} ${quoteSymbol(chain, token)} 最小单位（精度未知）`;
  return `${formatCompact(scaleRaw(token.quote_raised, decimals), 2)} ${quoteSymbol(chain, token)}`;
}

// metaLink turns a launch's metadata reference into a link, when it is one.
export function metaLink(uri: string | null): string | null {
  if (!uri) return null;
  if (uri.startsWith("ipfs://")) return `https://ipfs.io/ipfs/${uri.slice("ipfs://".length)}`;
  if (/^https?:\/\//.test(uri)) return uri;
  return null;
}
