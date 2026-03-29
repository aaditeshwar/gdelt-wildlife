# Human–wildlife conflict (India) — GDELT pipeline

This project pulls India HWC-related articles from GDELT, enriches them with GKG metadata, optionally runs full-text extraction with a local LLM, and exports map-ready GeoJSON.

## Repository layout

| Path | Purpose |
|------|---------|
| `data/` | CSV inputs and intermediate outputs (default paths for scripts) |
| `outputs/` | GeoJSON, QGIS QML styles, and the narrative text report from full-text extraction |
| `meta/` | Domain metadata: GDELT keywords, GKG theme sets, LLM prompts, GeoJSON/QML (`hwc_india_conflict_meta.json`, `event_domain_template.json`). For new domains, see **[META_GENERATION_README.md](meta/META_GENERATION_README.md)**. |
| `scripts/domain_meta.py` | Shared loader for meta JSON (used by fetch, enrich, full-text scripts) |
| `scripts/domain_paths.py` | `output_prefix(meta)` and standard `data/` / `outputs/` filenames; deterministic `event_id` backfill |
| `scripts/` | Runnable pipeline steps |
| `server/` | FastAPI app: layers, GeoJSON, styles, suggested edits, moderator apply (`python -m uvicorn server.main:app`) |
| `frontend/` | Vite + React + MapLibre map UI (`npm install` / `npm run dev` or `npm run build`) |
| `deploy/` | Example **systemd** unit and **Apache** vhost for production |

Run scripts from the **repository root** (paths below assume that).

### Filename prefix from `--meta`

For each script, the default CSV/GeoJSON paths use a short **prefix** derived from the meta JSON filename:

- `prefix = meta_stem.split("_")[0].lower()` (e.g. `hwc_india_conflict_meta` → `hwc`).
- If the stem has **no** underscore, use the first three characters of the stem (lower‑cased).

Defaults use `--meta` (default `meta/hwc_india_conflict_meta.json`), so existing HWC runs keep `hwc_*` filenames.

### Stable `event_id`

- **Fetch** (`gdelt-fetch-urls.py`): each deduplicated row gets a new UUID string in column `event_id`.
- **Downstream scripts** keep `event_id` on the article side; missing values are filled deterministically with `uuid5` over `url` (see `domain_paths.ensure_event_id_column`).
- **GeoJSON** (`convert_csv_to_geojson.py`): each point feature includes `properties.event_id` and RFC 7946 `Feature.id` when present.

### One-off migration (existing files)

```bash
pip install -r requirements.txt
python scripts/migrate_add_event_ids.py --dry-run
python scripts/migrate_add_event_ids.py --backup   # optional .bak copies
```

---

## Execution order

### 1. `scripts/gdelt-fetch-urls.py`

Queries the GDELT DOC API for keywords, deduplicates by URL.

- **Inputs:** `--meta` (default `meta/hwc_india_conflict_meta.json`) supplies `gdelt_doc_fetch`: keywords, country, language, query limits, date windows.
- **Outputs:** `data/{prefix}_urls.csv`, `data/{prefix}_urls_summary.txt` (with default meta, `hwc_*`).

### 2. GKG enrichment (choose one)

#### Option A — `scripts/gdelt-enrich-urls.py` (local GKG files)

Downloads GKG zip files and joins by URL. No GCP account required.

- **Inputs:** `data/hwc_urls.csv` (must include `url`, `seendate`); `--meta` for `gkg_theme_sets` and `gkg_geography`.
- **Outputs:** `data/{prefix}_urls_enriched.csv`, `data/{prefix}_urls_geocoded.csv`, `data/{prefix}_urls_high_confidence.csv`.

#### Option B — `scripts/gdelt-enrich-urls-bigquery.py`

Uploads URLs to a temporary BigQuery table and joins `gkg_partitioned`. Requires GCP project, BigQuery API, and `gcloud auth application-default login` (or service account).

- **Inputs:** same as Option A (`data/hwc_urls.csv` by default); `--meta` for theme sets and geography (same as Option A).
- **Outputs:** same three CSVs as Option A, plus `data/{prefix}_urls_unmatched.csv` (articles with no GKG row).

### 3. `scripts/gdelt-get-full-text.py` (first run)

Fetches article text (Jina / trafilatura), runs the local Ollama model for structured extraction, optional Google geocoding.

- **Inputs:** default `data/{prefix}_urls_geocoded.csv` (GDELT/GKG columns; no `fetch_method` column); `--meta` for `llm_extraction` prompts and `gkg_theme_sets.high_confidence_theme_score_min` (used when sampling by `theme_score`).
- **Outputs:** `data/{prefix}_final_report.csv`, `outputs/{prefix}_final_report.txt`.

### 4. `scripts/gdelt-get-full-text.py` (optional second run — Selenium retry)

Retries rows where `fetch_method` indicates failure, using Chrome/Selenium, then merges with the rest of the previous run.

- **Inputs:** `--retry-failed-from` → default `data/{prefix}_final_report.csv` (must include `fetch_method`). The script merges location hints from `data/{prefix}_urls_geocoded.csv` (prefix inferred from the final report CSV filename).
- **Outputs:** default `data/{prefix}_final_report_updated.csv`.

### 5. `scripts/convert_csv_to_geojson.py`

Builds a point GeoJSON from rows marked as HWC events with valid coordinates; optional QGIS categorized style.

- **Inputs:** default `data/{prefix}_final_report_updated.csv`; `meta/hwc_india_conflict_meta.json` (or your domain meta via `--meta`).
- **Outputs:** default `outputs/{prefix}_points.geojson`; with `--write-qml`, default `outputs/{prefix}_india_points.qml`.

---

## Web map (FastAPI + React)

1. Install Python deps: `pip install -r requirements.txt`.
2. **API env:** copy `server/.env.example` to `server/.env` and edit. Run from repo root: `python -m server.main` (reads `HOST` / `PORT` from `server/.env`) or `python -m uvicorn server.main:app --reload --host 127.0.0.1 --port 8000`.
3. **Frontend dev env:** copy `frontend/.env.example` to `frontend/.env` so the Vite dev proxy matches the API (`VITE_API_HOST`, `VITE_API_PORT`). Then `cd frontend && npm install && npm run dev`. Restart Vite after changing `frontend/.env`.
4. **Production-style**: build the SPA then serve with Uvicorn (API + static files). If you deploy under a **subpath** (e.g. `https://servername/gdelt-wildlife/`), set `VITE_BASE_PATH` when building — see **Production: Apache** below.

**Where env vars are read**

| Location | Mechanism |
|----------|-----------|
| **Server** | `server/settings.py` calls `load_dotenv(server/.env)` first, then builds `Settings()` from `os.environ`. Process env vars **override** values from `server/.env` if both are set. Restart Uvicorn after editing `server/.env`. |
| **Frontend (dev proxy)** | `frontend/vite.config.ts` uses Vite `loadEnv()` so variables in `frontend/.env` are available when the dev server starts. Restart `npm run dev` after editing. |

Templates: `server/.env.example`, `frontend/.env.example`.

| Variable | File / scope | Meaning |
|----------|----------------|----------|
| `REPO_ROOT` | server `.env` | Repository root (defaults to parent of `server/`) |
| `HOST` | server `.env` | Bind address for `python -m server.main` (default `127.0.0.1`) |
| `PORT` | server `.env` | HTTP port (default `8000`) |
| `UVICORN_PORT` | server `.env` | Used only if `PORT` is unset |
| `SESSION_SECRET` | server `.env` | Secret for signed session cookies (production: change) |
| `GIT_AUTO_COMMIT` | server `.env` | `1` to run `git commit` after apply |
| `VITE_BASE_PATH` | frontend `.env` | Vite app base URL; `frontend/.env.example` defaults to `/gdelt-wildlife/` (match Apache `ProxyPass`). Use `/` for dev at site root only. |
| `VITE_API_HOST` | frontend `.env` | Host for `/api` proxy (default `127.0.0.1`) |
| `VITE_API_PORT` | frontend `.env` | Port for `/api` proxy (must match server `PORT`) |

Moderators are listed in `data/moderators.json` (bcrypt hashes only). Generate a hash with:

```bash
python scripts/hash_password.py
```

The sample file ships with user `admin` and password **`changeme`** (change before any real deployment).

### Production: Apache reverse proxy + systemd (Uvicorn)

Deploy the **built** SPA and run **Uvicorn bound to localhost** only. **Apache** can (recommended) **serve `frontend/dist` with `Alias` + `FallbackResource`** and **proxy only `/gdelt-wildlife/api`** to Uvicorn — the same pattern as other apps on your host. Alternatively, Apache can proxy **all** of `/gdelt-wildlife/` to Uvicorn and let FastAPI’s `StaticFiles` serve the SPA (simpler, fewer moving parts). Example templates:

| File | Purpose |
|------|---------|
| `deploy/gdelt-wildlife.service.example` | systemd unit for Uvicorn |
| `deploy/apache-vhost.conf.example` | Apache: `Alias` → `frontend/dist` + `ProxyPass` → `/api` only (with commented alternative) |

**1. On the server (paths are examples — use your own)**

- Clone or copy the repo (e.g. `/srv/gdelt-wildlife`).
- Create a venv and install deps: `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`.
- Build the frontend with the **same URL path** Apache will use (see `deploy/apache-vhost.conf.example`). For **`https://SERVERNAME/gdelt-wildlife/`**:
  ```bash
  cd frontend && npm ci && VITE_BASE_PATH=/gdelt-wildlife/ npm run build
  ```
  For the app at the **site root** (`/`), set `VITE_BASE_PATH=/` in `frontend/.env` (or override for a one-off build) before `npm run build`.
- Copy `server/.env.example` → `server/.env` and set at least `SESSION_SECRET` to a long random string. Optionally set `REPO_ROOT` if the app root is not the default parent of `server/`.
- Ensure the service user can read `data/`, `outputs/`, `meta/`, and write `data/edits/`, `data/edit_log.jsonl`, and GeoJSON under `outputs/` (e.g. `chown -R www-data:www-data` or a dedicated `gdelt` user used in both systemd and file ownership).

**2. systemd**

```bash
sudo cp deploy/gdelt-wildlife.service.example /etc/systemd/system/gdelt-wildlife.service
# Edit the file: WorkingDirectory, User, paths to .venv and server/.env, and --port if not 8000
sudo systemctl daemon-reload
sudo systemctl enable --now gdelt-wildlife.service
sudo systemctl status gdelt-wildlife.service
journalctl -u gdelt-wildlife.service -f   # logs
```

The unit runs Uvicorn on **`127.0.0.1:8000`** with **`--proxy-headers`** and **`--forwarded-allow-ips=127.0.0.1`** so `X-Forwarded-*` from Apache are trusted. Do **not** use `--reload` in production.

**3. Apache**

Enable modules, install the vhost, and reload:

```bash
sudo a2enmod proxy proxy_http headers ssl
sudo cp deploy/apache-vhost.conf.example /etc/apache2/sites-available/gdelt-wildlife.conf
# Edit ServerName, SSL paths, and ProxyPass port to match Uvicorn
sudo a2ensite gdelt-wildlife
sudo apache2ctl configtest && sudo systemctl reload apache2
```

Obtain certificates (e.g. Certbot) and point `SSLCertificateFile` / `SSLCertificateKeyFile` at your PEM files.

**Recommended vhost (in `deploy/apache-vhost.conf.example`):** **`ProxyPass /gdelt-wildlife/api`** → **`http://127.0.0.1:8000/api`**, and **`Alias /gdelt-wildlife`** → **`…/frontend/dist`** with **`FallbackResource /gdelt-wildlife/index.html`** for client-side routes. Put **`ProxyPass` lines before `Alias`** so API requests are not served as static files. You can merge those blocks into an existing `VirtualHost` alongside archive-search / agromet-advisory.

**Alternative:** proxy **`/gdelt-wildlife/`** entirely to Uvicorn (`ProxyPass /gdelt-wildlife/ http://127.0.0.1:8000/`); FastAPI then serves both the API and static files from `frontend/dist`. No `Alias` needed.

Users open **`http(s)://SERVERNAME/gdelt-wildlife/`**. Edit `ServerName`, `Alias` paths, and SSL blocks to match your server.

**4. Port and path consistency**

- **`ExecStart` `--port`**, **`ProxyPass`** target port (`127.0.0.1:8000`), and firewall rules must match.
- **`VITE_BASE_PATH`** at build time must match the public path (e.g. `/gdelt-wildlife/` for both `Alias` and asset URLs in `index.html`).

**5. Operations**

```bash
sudo systemctl restart gdelt-wildlife.service   # after code or .env changes
```

---

## Environment

Each script documents its Python dependencies in the module docstring (`pip install ...`). Typical needs include `pandas`, `requests`, `tqdm`, `trafilatura`, and for full-text extraction `ollama` reachable at the configured base URL plus optional `GOOGLE_MAPS_API_KEY` for geocoding.

---

## Overriding paths

All scripts accept explicit `--input`, `--output`, and related flags. Defaults point at `data/` and `outputs/` under the repository root so you can relocate files without editing code.

## Domain configuration (`meta/*.json`)

Copy `meta/event_domain_template.json` or adapt `meta/hwc_india_conflict_meta.json`. Scripts read:

- **`gdelt_doc_fetch`** — DOC API keywords, country/language, `max_records`, sleep, years/windows, summary titles.
- **`gkg_theme_sets`** — `wildlife_themes` and `conflict_themes` (GKG V2Themes codes), `high_confidence_theme_score_min`.
- **`gkg_geography`** — `location_country_codes`, `subnational_loc_types` for GKG V2Locations filtering.
- **`llm_extraction`** — use **`system_prompt_lines`** and **`extraction_prompt_lines`** (arrays of strings) so long prompts do not require heavy JSON escaping. The extraction template must include `{pub_date}`, `{url}`, `{gdelt_locations}`, and `{article_text}`. Literal `{` / `}` in the embedded JSON example must appear as doubled `{{` / `}}` in the joined string (same rule as Python `str.format`).
