import path from "node:path";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

/** Same-origin proxies — matches AeroStream README default ports. Override targets via env if needed. */
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
    passWithNoTests: false,
  },
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      "/svc/producer": {
        target: process.env.VITE_PROXY_PRODUCER ?? "http://127.0.0.1:8090",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/svc\/producer/, ""),
      },
      "/svc/processor": {
        target: process.env.VITE_PROXY_PROCESSOR ?? "http://127.0.0.1:8091",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/svc\/processor/, ""),
      },
      "/svc/registry": {
        target: process.env.VITE_PROXY_REGISTRY ?? "http://127.0.0.1:8081",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/svc\/registry/, ""),
      },
      "/svc/connect": {
        target: process.env.VITE_PROXY_CONNECT ?? "http://127.0.0.1:8083",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/svc\/connect/, ""),
      },
      "/svc/ksql": {
        target: process.env.VITE_PROXY_KSQL ?? "http://127.0.0.1:8088",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/svc\/ksql/, ""),
      },
      "/svc/ml": {
        target: process.env.VITE_PROXY_ML ?? "http://127.0.0.1:8099",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/svc\/ml/, ""),
      },
      "/svc/bff": {
        target: process.env.VITE_PROXY_BFF ?? "http://127.0.0.1:8089",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/svc\/bff/, ""),
      },
    },
  },
});
