/** Resolve API path against Vite `base` (e.g. `/gdelt-wildlife/api/...` when deployed under a subpath). */
export function apiUrl(path: string): string {
  const base = import.meta.env.BASE_URL || "/";
  const p = path.startsWith("/") ? path : `/${path}`;
  if (base === "/" || base === "") return p;
  const root = base.replace(/\/+$/, "");
  return `${root}${p}`;
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(apiUrl(path), { ...init, credentials: "include" });
  if (!r.ok) {
    const err = await r.text();
    throw new Error(err || r.statusText);
  }
  const ct = r.headers.get("content-type") || "";
  if (ct.includes("application/json")) return r.json() as Promise<T>;
  return undefined as T;
}
