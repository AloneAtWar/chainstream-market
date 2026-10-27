import { useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import type { Chain } from "../lib/chains";
import { shortAddress } from "../lib/format";

type AddressKind = "token" | "pool" | "holder" | "tx";

// Address shows a shortened address that links inside the app (or to the
// explorer for transactions), with a copy button and an explorer link.
export function Address({ chain, value, kind, label }: { chain: Chain; value: string; kind: AddressKind; label?: string }) {
  const text = label ?? shortAddress(value);
  const explorer = `${chain.explorer}/${kind === "tx" ? "tx" : kind === "token" ? "token" : "address"}/${value}`;
  return (
    <span className="address">
      {kind === "tx" ? (
        <a href={explorer} target="_blank" rel="noreferrer" className="mono">
          {text}
        </a>
      ) : (
        <Link to={`/${chain.id}/${kind}/${value}`} className="mono">
          {text}
        </Link>
      )}
      <CopyButton value={value} />
      {kind !== "tx" && (
        <a href={explorer} target="_blank" rel="noreferrer" className="icon-link" title="在区块浏览器中查看">
          ↗
        </a>
      )}
    </span>
  );
}

export function CopyButton({ value }: { value: string }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    void navigator.clipboard?.writeText(value).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    });
  };
  return (
    <button className="icon-button" onClick={copy} title="复制">
      {copied ? "✓" : "⧉"}
    </button>
  );
}

const AVATAR_COLORS = ["#f0b90b", "#16c784", "#3b82f6", "#a855f7", "#ec4899", "#f97316", "#14b8a6", "#ef4444"];

// TokenAvatar draws a colored initial; the API has no logos.
export function TokenAvatar({ symbol, address, size = 28 }: { symbol: string | null; address: string; size?: number }) {
  let hash = 0;
  for (const char of address) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  const color = AVATAR_COLORS[hash % AVATAR_COLORS.length];
  const letter = (symbol ?? "?").replace(/^[^A-Za-z0-9]+/, "").charAt(0).toUpperCase() || "?";
  return (
    <span className="avatar" style={{ width: size, height: size, background: color, fontSize: size * 0.45 }}>
      {letter}
    </span>
  );
}

export function Tabs<T extends string>({
  value,
  options,
  onChange,
  small,
}: {
  value: T;
  options: readonly { value: T; label: ReactNode }[];
  onChange: (value: T) => void;
  small?: boolean;
}) {
  return (
    <div className={small ? "tabs small" : "tabs"} role="tablist">
      {options.map((option) => (
        <button
          key={option.value}
          role="tab"
          aria-selected={option.value === value}
          className={option.value === value ? "tab active" : "tab"}
          onClick={() => onChange(option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

export function Status({ loading, error, empty, children }: { loading?: boolean; error?: Error | null; empty?: boolean; children?: ReactNode }) {
  if (error) return <div className="status error">加载失败：{error.message}</div>;
  if (loading) return <div className="status">加载中…</div>;
  if (empty) return <div className="status">{children ?? "暂无数据"}</div>;
  return null;
}

export function Stat({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="stat">
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

export function Info({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="info-row">
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}
