import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** Loads ``frontend/.env`` (see ``.env.example``). Restart Vite after changes. */
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, __dirname, "");
  const apiHost = env.VITE_API_HOST || "127.0.0.1";
  const apiPort = env.VITE_API_PORT || env.PORT || "8000";

  return {
    plugins: [react()],
    server: {
      proxy: {
        "/api": `http://${apiHost}:${apiPort}`,
      },
    },
  };
});
