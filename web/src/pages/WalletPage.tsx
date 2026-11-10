import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useParams } from "react-router-dom";
import { CopyButton, Status, Tabs } from "../components/common";
import { TransfersTable } from "../components/tables";
import { TradesTable } from "../components/TradesTable";
import { PositionsTable, WalletLabels, WalletStatsGrid, WalletTradesTable } from "../components/wallets";
import { api } from "../lib/api";
import type { Chain } from "../lib/chains";
import { formatAge } from "../lib/format";
import { useLiveFeed, useNow } from "../lib/hooks";
import { channels } from "../lib/live";

type Panel = "open" | "closed" | "ledger" | "live" | "transfers";

const PANELS: { value: Panel; label: string }[] = [
  { value: "open", label: "当前持仓" },
  { value: "closed", label: "已清仓" },
  { value: "ledger", label: "交易记录" },
  { value: "live", label: "实时成交" },
  { value: "transfers", label: "ERC-20 转账" },
];

// WalletPage is a wallet's profile: market-wallet's labels and stats per
// window, its positions with PnL, its ledger trades, and its transfers.
export function WalletPage({ chain }: { chain: Chain }) {
  const address = useParams().address!.toLowerCase();
  const now = useNow(10_000);
  const [panel, setPanel] = useState<Panel>("open");
  const [window, setWindow] = useState<number | null>(null);
  const profile = useQuery({
    queryKey: ["wallet", chain.id, address],
    queryFn: () => api.wallet(chain.id, address),
    refetchInterval: 60_000,
  });
  const stats = profile.data?.stats ?? [];
  // Default to the 30-day window, or whichever the API has.
  const selected = stats.find((entry) => entry.window_days === (window ?? 30)) ?? stats[0];

  return (
    <div className="page">
      <section className="card token-header">
        <div>
          <h1 className="wallet-title">
            钱包
            <WalletLabels labels={profile.data?.labels} />
          </h1>
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
          <h3>交易表现</h3>
          <div className="toolbar-right">
            {profile.data?.refreshed_at && <span className="muted">统计更新于 {formatAge(profile.data.refreshed_at, now)} 前</span>}
            {stats.length > 0 && (
              <Tabs
                small
                value={String(selected?.window_days ?? 30)}
                options={stats.map((entry) => ({ value: String(entry.window_days), label: `${entry.window_days} 天` }))}
                onChange={(value) => setWindow(Number(value))}
              />
            )}
          </div>
        </div>
        {selected ? (
          <WalletStatsGrid stats={selected} />
        ) : (
          <Status loading={profile.isLoading} error={profile.error} empty={!profile.isLoading}>
            这个钱包在统计窗口内没有买卖
          </Status>
        )}
      </section>
      <section className="card">
        <div className="card-head">
          <Tabs value={panel} options={PANELS} onChange={setPanel} />
        </div>
        {panel === "open" && <PositionsTable chain={chain} wallet={address} status="open" />}
        {panel === "closed" && <PositionsTable chain={chain} wallet={address} status="closed" />}
        {panel === "ledger" && <WalletTradesTable chain={chain} wallet={address} />}
        {panel === "live" && <WalletLiveTrades chain={chain} wallet={address} />}
        {panel === "transfers" && <TransfersTable chain={chain} subject="holder" value={address} />}
      </section>
    </div>
  );
}

function WalletLiveTrades({ chain, wallet }: { chain: Chain; wallet: string }) {
  const trades = useLiveFeed(channels.walletTrades(chain.id, wallet));
  return (
    <>
      <div className="table-note muted">打开页面后这个钱包的新成交会实时出现在这里，历史见“交易记录”。</div>
      <TradesTable chain={chain} trades={trades} loading={false} error={null} showPool />
    </>
  );
}
