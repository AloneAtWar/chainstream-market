import { Navigate, Route, Routes, useParams } from "react-router-dom";
import { Header } from "./components/Header";
import { chainById, DEFAULT_CHAIN } from "./lib/chains";
import { HolderPage } from "./pages/HolderPage";
import { PoolPage } from "./pages/PoolPage";
import { TokenListPage } from "./pages/TokenListPage";
import { TokenPage } from "./pages/TokenPage";

export function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to={`/${DEFAULT_CHAIN.id}`} replace />} />
      <Route path="/:chain/*" element={<ChainRoutes />} />
    </Routes>
  );
}

function ChainRoutes() {
  const chain = chainById(Number(useParams().chain));
  if (!chain) return <Navigate to={`/${DEFAULT_CHAIN.id}`} replace />;
  return (
    <div className="app">
      <Header chain={chain} />
      <main>
        <Routes>
          <Route index element={<TokenListPage chain={chain} />} />
          <Route path="token/:address" element={<TokenPage chain={chain} />} />
          <Route path="pool/:pool" element={<PoolPage chain={chain} />} />
          <Route path="holder/:address" element={<HolderPage chain={chain} />} />
          <Route path="*" element={<Navigate to={`/${chain.id}`} replace />} />
        </Routes>
      </main>
    </div>
  );
}
