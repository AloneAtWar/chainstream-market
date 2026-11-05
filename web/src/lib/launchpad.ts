// Display helpers for market-launchpad's tokens: bonding curves and direct
// launches.

import type { LaunchpadToken } from "./api";
import { canonicalToken, type Chain } from "./chains";
import { formatCompact, scaleRaw } from "./format";

// PROTOCOL_LABELS names the launchpads chainstream-evm parses. Which of them
// a chain has comes from market-api; a platform missing here shows its id.
const PROTOCOL_LABELS: Record<string, string> = {
  fourmeme: "four.meme",
  flap: "Flap",
  pons: "Pons",
  bankr: "Bankr",
  longxyz: "Long.xyz",
  clanker: "Clanker",
  poolstrade: "pools.trade",
};

export function protocolLabel(protocol: string): string {
  return PROTOCOL_LABELS[protocol] ?? protocol;
}

// progressBasis says what a token's progress measures: its curve, its
// launch window, or, for a direct launch without one, its market cap
// against the platform's target.
// unlistedProtocol reports whether tokens carry a platform missing from
// protocols, so a page that polls the tokens knows its platform list is stale.
export function unlistedProtocol(tokens: Pick<LaunchpadToken, "protocol">[], protocols: string[]): boolean {
  return tokens.some((token) => token.protocol !== "" && !protocols.includes(token.protocol));
}

export function progressBasis(token: LaunchpadToken): string {
  if (!token.direct) return "曲线进度";
  if (token.launch_start !== null && token.launch_end !== null) return "发射窗口进度";
  return "市值 / 目标市值";
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
  if (token.direct) return "直发，无曲线";
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
