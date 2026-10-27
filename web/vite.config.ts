/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The dev server proxies the API and its WebSocket to market-api, so the
// page and the API share an origin as they do behind nginx.
const api = process.env.MARKET_API ?? "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/v1": { target: api, ws: true },
    },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks: { react: ["react", "react-dom", "react-router-dom", "@tanstack/react-query"], charts: ["lightweight-charts"] },
      },
    },
  },
  test: {
    include: ["src/**/*.test.ts"],
  },
});
