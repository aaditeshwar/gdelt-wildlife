import path from "path";
import { fileURLToPath } from "url";
import { defineConfig, loadEnv, type ProxyOptions } from "vite";
import react from "@vitejs/plugin-react";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** Loads ``frontend/.env`` (see ``.env.example``). Restart Vite after changes. */
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, __dirname, "");
  const apiHost = env.VITE_API_HOST || "127.0.0.1";
  const apiPort = env.VITE_API_PORT || env.PORT || "8000";
  const rawBase = env.VITE_BASE_PATH || "/";
  const base =
    rawBase === "/" ? "/" : rawBase.endsWith("/") ? rawBase : `${rawBase}/`;
  const baseNoSlash = base.replace(/\/+$/, "");

  const proxy: Record<string, string | ProxyOptions> =
    baseNoSlash === ""
      ? { "/api": `http://${apiHost}:${apiPort}` }
      : {
          [`${baseNoSlash}/api`]: {
            target: `http://${apiHost}:${apiPort}`,
            changeOrigin: true,
            rewrite: (p: string) => p.replace(new RegExp(`^${baseNoSlash}`), ""),
          },
        };

  return {
    base,
    plugins: [react()],
    server: {
      proxy,
    },
  };
});
