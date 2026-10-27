import { useParams } from "react-router-dom";
import { CopyButton } from "../components/common";
import { TransfersTable } from "../components/tables";
import type { Chain } from "../lib/chains";

export function HolderPage({ chain }: { chain: Chain }) {
  const address = useParams().address!.toLowerCase();
  return (
    <div className="page">
      <section className="card token-header">
        <div>
          <h1>地址</h1>
          <div className="muted token-address">
            <span className="mono">{address}</span>
            <CopyButton value={address} />
            <a href={`${chain.explorer}/address/${address}`} target="_blank" rel="noreferrer" className="icon-link">
              ↗
            </a>
          </div>
        </div>
      </section>
      <section className="card">
        <div className="card-head">
          <h3>ERC-20 转账</h3>
        </div>
        <TransfersTable chain={chain} subject="holder" value={address} />
      </section>
    </div>
  );
}
