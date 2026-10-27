// Number and time formatting for the market tables and charts.

const SUBSCRIPT = "₀₁₂₃₄₅₆₇₈₉";

function toNumber(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = typeof value === "number" ? value : Number(value);
  return Number.isFinite(number) ? number : null;
}

function subscript(count: number): string {
  return [...String(count)].map((digit) => SUBSCRIPT[Number(digit)]).join("");
}

// formatPrice keeps four significant digits. Prices below 0.0001 collapse
// their leading zeros into a subscript count, so 0.00000123 reads 0.0₅123.
export function formatPrice(value: string | number | null | undefined): string {
  const number = toNumber(value);
  if (number === null) return "—";
  if (number === 0) return "0";
  const sign = number < 0 ? "-" : "";
  const magnitude = Math.abs(number);
  if (magnitude >= 1) {
    const digits = magnitude >= 1000 ? 2 : 4;
    return sign + magnitude.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: 2 });
  }
  // The zeros between the point and the first significant digit.
  const zeros = Math.ceil(-Math.log10(magnitude)) - 1;
  const significant = Math.round(magnitude * 10 ** (zeros + 4));
  // Rounding can carry into another digit, as 0.0999996 does.
  if (significant >= 10000) return sign + formatPrice(Number(magnitude.toPrecision(4)));
  const digits = String(significant).replace(/0+$/, "") || "0";
  if (zeros >= 4) return `${sign}0.0${subscript(zeros)}${digits}`;
  return `${sign}0.${"0".repeat(zeros)}${digits}`;
}

const UNITS: [number, string][] = [
  [1e12, "T"],
  [1e9, "B"],
  [1e6, "M"],
  [1e3, "K"],
];

// formatCompact writes large numbers with a K/M/B/T suffix.
export function formatCompact(value: string | number | null | undefined, digits = 2): string {
  const number = toNumber(value);
  if (number === null) return "—";
  const magnitude = Math.abs(number);
  for (const [size, unit] of UNITS) {
    if (magnitude >= size) return `${(number / size).toFixed(digits).replace(/\.?0+$/, "")}${unit}`;
  }
  if (magnitude > 0 && magnitude < 1) return formatPrice(number);
  return number.toFixed(digits).replace(/\.?0+$/, "");
}

export function formatUsd(value: string | number | null | undefined, digits = 2): string {
  const number = toNumber(value);
  if (number === null) return "—";
  const text = formatCompact(Math.abs(number), digits);
  return `${number < 0 ? "-" : ""}$${text}`;
}

// formatChange formats a ratio such as -0.25 as -25.00%.
export function formatChange(ratio: string | number | null | undefined): string {
  const number = toNumber(ratio);
  if (number === null) return "—";
  const percent = number * 100;
  const text = Math.abs(percent) >= 10000 ? formatCompact(Math.abs(percent), 1) : Math.abs(percent).toFixed(2);
  return `${percent > 0 ? "+" : percent < 0 ? "-" : ""}${text}%`;
}

export function changeClass(ratio: string | number | null | undefined): string {
  const number = toNumber(ratio);
  if (number === null || number === 0) return "flat";
  return number > 0 ? "up" : "down";
}

// formatAge writes how long ago a unix time was, like 45s, 12m, 3h or 5d.
export function formatAge(unix: number | null | undefined, now = Date.now() / 1000): string {
  if (unix === null || unix === undefined) return "—";
  const seconds = Math.max(0, Math.floor(now - unix));
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  if (seconds < 86400 * 365) return `${Math.floor(seconds / 86400)}d`;
  return `${Math.floor(seconds / (86400 * 365))}y`;
}

export function formatTime(unix: number): string {
  const date = new Date(unix * 1000);
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

export function shortAddress(address: string, head = 6, tail = 4): string {
  if (address.length <= head + tail + 1) return address;
  return `${address.slice(0, head)}…${address.slice(-tail)}`;
}

// scaleRaw converts a raw integer amount to whole tokens as a string,
// without going through floating point.
export function scaleRaw(raw: string, decimals: number): string {
  const negative = raw.startsWith("-");
  const digits = (negative ? raw.slice(1) : raw).replace(/^0+/, "") || "0";
  if (decimals <= 0) return (negative ? "-" : "") + digits;
  const padded = digits.padStart(decimals + 1, "0");
  const whole = padded.slice(0, -decimals);
  const fraction = padded.slice(-decimals).replace(/0+$/, "");
  return (negative ? "-" : "") + whole + (fraction ? `.${fraction}` : "");
}

export function formatInteger(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString("en-US");
}
