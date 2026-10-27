import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type Pool, type Transfer } from "../lib/api";
import type { Chain } from "../lib/chains";
import { formatAge, formatCompact, formatInteger, scaleRaw, shortAddress } from "../lib/format";
import { useNow, useTokenMeta, useTokenSymbol } from "../lib/hooks";
import { Address, Status } from "./common";

export function HoldersTable({ chain, token }: { chain: Chain; token: string }) {
  const holders = useQuery({
    queryKey: ["holders", chain.id, token],
    queryFn: () => api.holders(chain.id, token, 100),
    refetchInterval: 30_000,
  });
  const rows = holders.data?.holders ?? [];
  const largest = Number(rows[0]?.balance ?? rows[0]?.balance_raw ?? 0);
  return (
    <div className="table-scroll">
      {holders.data && (
        <div className="table-note muted">
          共 {formatInteger(holders.data.holder_count)} 个持有人，显示余额最多的 {rows.length} 个
          {holders.data.decimals === undefined && "（精度未知，余额为原始单位）"}
        </div>
      )}
      <table className="table">
        <thead>
          <tr>
            <th className="rank">#</th>
            <th>地址</th>
            <th className="num">余额</th>
            <th className="bar-col" />
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => {
            const amount = row.balance ?? row.balance_raw;
            const share = largest > 0 ? Number(amount) / largest : 0;
            return (
              <tr key={row.holder}>
                <td className="rank muted">{index + 1}</td>
                <td>
                  <Address chain={chain} value={row.holder} kind="holder" />
                </td>
                <td className="num mono" title={amount}>
                  {formatCompact(amount)}
                </td>
                <td className="bar-col">
                  <div className="bar" style={{ width: `${Math.max(1, share * 100)}%` }} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <Status loading={holders.isLoading} error={holders.error} empty={!holders.isLoading && rows.length === 0} />
    </div>
  );
}

export function PoolsTable({ chain, token }: { chain: Chain; token: string }) {
  const now = useNow(10_000);
  const pools = useQuery({
    queryKey: ["token-pools", chain.id, token],
    queryFn: () => api.tokenPools(chain.id, token, 50),
  });
  const rows = pools.data ?? [];
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>交易对</th>
            <th>协议</th>
            <th>池子</th>
            <th>DEX</th>
            <th className="num">最近更新</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((pool) => (
            <tr key={pool.pool_id}>
              <td>
                <Link to={`/${chain.id}/pool/${pool.pool_id}`}>
                  <PairName chain={chain} pool={pool} />
                </Link>
              </td>
              <td>
                <span className="badge">{pool.protocol}</span>
              </td>
              <td>
                <Address chain={chain} value={pool.pool_id} kind="pool" />
              </td>
              <td>{pool.dex_address ? <Address chain={chain} value={pool.dex_address} kind="holder" /> : "—"}</td>
              <td className="num muted">{pool.source_time ? formatAge(pool.source_time, now) : `#${pool.source_height}`}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <Status loading={pools.isLoading} error={pools.error} empty={!pools.isLoading && rows.length === 0} />
    </div>
  );
}

export function PairName({ chain, pool }: { chain: Chain; pool: Pool }) {
  return (
    <span className="pair">
      {pool.tokens.map((token, index) => (
        <span key={token}>
          {index > 0 && <span className="muted"> / </span>}
          <TokenSymbol chain={chain} token={token} />
        </span>
      ))}
    </span>
  );
}

export function TokenSymbol({ chain, token }: { chain: Chain; token: string }) {
  return <>{useTokenSymbol(chain.id, token)}</>;
}

// TransfersTable pages through a token's or a holder's ERC-20 transfers.
export function TransfersTable({
  chain,
  subject,
  value,
}: {
  chain: Chain;
  subject: "token" | "holder";
  value: string;
}) {
  const now = useNow();
  const transfers = useInfiniteQuery({
    queryKey: ["transfers", chain.id, subject, value],
    queryFn: ({ pageParam }) => api.transfers(chain.id, subject, value, { limit: 50, cursor: pageParam }),
    initialPageParam: undefined as { before_height: number; before_log_index: number } | undefined,
    getNextPageParam: (page) => page.next,
  });
  const rows = transfers.data?.pages.flatMap((page) => page.transfers) ?? [];
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>时间</th>
            {subject === "holder" && <th>代币</th>}
            <th>从</th>
            <th>到</th>
            <th className="num">数量</th>
            <th>交易</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((transfer) => (
            <tr key={`${transfer.block_height}:${transfer.log_index}`}>
              <td className="muted" title={`#${transfer.block_height}`}>
                {formatAge(transfer.block_time, now)}
              </td>
              {subject === "holder" && (
                <td>
                  <Link to={`/${chain.id}/token/${transfer.token}`}>
                    <TokenSymbol chain={chain} token={transfer.token} />
                  </Link>
                </td>
              )}
              <td>
                <Party chain={chain} address={transfer.from} self={subject === "holder" ? value : undefined} />
              </td>
              <td>
                <Party chain={chain} address={transfer.to} self={subject === "holder" ? value : undefined} />
              </td>
              <td className="num mono">
                <TransferAmount chain={chain} transfer={transfer} holder={subject === "holder" ? value : undefined} />
              </td>
              <td>
                <Address chain={chain} value={transfer.transaction_hash} kind="tx" label={`${transfer.transaction_hash.slice(0, 10)}…`} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <Status loading={transfers.isLoading} error={transfers.error} empty={!transfers.isLoading && rows.length === 0} />
      {transfers.hasNextPage && (
        <div className="pager">
          <button disabled={transfers.isFetchingNextPage} onClick={() => void transfers.fetchNextPage()}>
            {transfers.isFetchingNextPage ? "加载中…" : "加载更多"}
          </button>
        </div>
      )}
    </div>
  );
}

const ZERO = "0x0000000000000000000000000000000000000000";

function Party({ chain, address, self }: { chain: Chain; address: string; self?: string }) {
  if (address === ZERO) return <span className="muted">铸造/销毁</span>;
  if (address === self) return <span className="muted mono">{shortAddress(address, 4, 4)}（本地址）</span>;
  return <Address chain={chain} value={address} kind="holder" label={shortAddress(address, 4, 4)} />;
}

function TransferAmount({ chain, transfer, holder }: { chain: Chain; transfer: Transfer; holder?: string }) {
  const meta = useTokenMeta(chain.id, transfer.token);
  const decimals = meta.data?.decimals;
  const amount = decimals === null || decimals === undefined ? transfer.amount_raw : scaleRaw(transfer.amount_raw, decimals);
  // On a holder's page, incoming amounts are green and outgoing red.
  const direction = holder === undefined ? "" : transfer.to === holder ? "up" : transfer.from === holder ? "down" : "";
  return (
    <span className={direction} title={amount}>
      {direction === "down" ? "-" : direction === "up" ? "+" : ""}
      {formatCompact(amount)}
      {decimals === null || decimals === undefined ? <span className="muted"> raw</span> : null}
    </span>
  );
}
