import { useState, type FormEvent } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { CHAINS, type Chain } from "../lib/chains";
import { formatInteger } from "../lib/format";
import { useHead, useLiveStatus } from "../lib/hooks";

const STATUS_TEXT = { open: "实时", connecting: "连接中", closed: "未连接" } as const;

export function Header({ chain }: { chain: Chain }) {
  const navigate = useNavigate();
  const location = useLocation();
  const [query, setQuery] = useState("");
  const status = useLiveStatus();
  const head = useHead(chain.id);

  const search = (event: FormEvent) => {
    event.preventDefault();
    const value = query.trim();
    if (!value) return;
    // A full address opens the token; anything else searches symbols.
    if (/^0x[0-9a-fA-F]{40}$/.test(value)) navigate(`/${chain.id}/token/${value.toLowerCase()}`);
    else navigate(`/${chain.id}?q=${encodeURIComponent(value)}`);
    setQuery("");
  };

  // Switching chains keeps the token list's query string but leaves detail
  // pages, whose addresses belong to the old chain.
  const switchChain = (id: number) => {
    const onList = location.pathname === `/${chain.id}`;
    navigate(`/${id}${onList ? location.search : ""}`);
  };

  return (
    <header className="header">
      <Link to={`/${chain.id}`} className="brand">
        <span className="brand-mark" aria-hidden>
          <svg viewBox="0 0 32 32" width="22" height="22">
            <rect width="32" height="32" rx="8" fill="var(--up)" />
            <path d="M7 21l6-7 5 4 7-9" stroke="#fff" strokeWidth="3" fill="none" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </span>
        <span>Chainstream</span>
      </Link>
      <nav className="chains">
        {CHAINS.map((item) => (
          <button
            key={item.id}
            className={item.id === chain.id ? "chain active" : "chain"}
            onClick={() => switchChain(item.id)}
          >
            {item.short}
          </button>
        ))}
      </nav>
      <form className="search" onSubmit={search}>
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="搜索代币符号或粘贴合约地址"
          spellCheck={false}
        />
      </form>
      <div className={`live-status ${status}`} title={head ? `已确认高度 ${formatInteger(head.finalized_height)}` : undefined}>
        <span className="dot" />
        {STATUS_TEXT[status]}
        {head && <span className="head">#{formatInteger(head.height)}</span>}
      </div>
    </header>
  );
}
