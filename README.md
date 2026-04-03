# Human–wildlife conflict (India) — GDELT pipeline

This project pulls India HWC-related articles from GDELT, enriches them with GKG metadata, optionally runs full-text extraction with a local LLM, and exports map-ready GeoJSON.

## Repository layout

| Path | Purpose |
|------|---------|
| `data/` | CSV inputs and intermediate outputs (default paths for scripts) |
| `outputs/` | GeoJSON, QGIS QML styles, and the narrative text report from full-text extraction |
| `meta/` | Domain metadata: GDELT keywords, GKG theme sets, LLM prompts, GeoJSON/QML (`hwc_india_conflict_meta.json`, `event_domain_template.json`). For new domains, see **[META_GENERATION_README.md](meta/META_GENERATION_README.md)**. |
| `scripts/` | Runnable pipeline steps |
| `server/` | FastAPI app: layers, GeoJSON, styles, suggested edits, moderator apply (`python -m uvicorn server.main:app`) |
| `frontend/` | Vite + React + MapLibre map UI (`npm install` / `npm run dev` or `npm run build`) |
| `deploy/` | Example **systemd** unit and **Apache** vhost for production |

Run scripts from the **repository root** (paths below assume that).

**Environment variables:** copy **[`.env.example`](.env.example)** to `.env` at the repo root for pipeline defaults (Google Maps geocoding, Ollama URL/model, BigQuery project). OS env vars override file values. The web stack uses **`server/.env`** and **`frontend/.env`** separately (`server/.env.example`, `frontend/.env.example`).

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

Deduplicates by URL and writes `data/{prefix}_urls.csv` + summary.

- **Recommended:** **`--source doc`** (default, omit the flag). Uses the GDELT DOC 2.0 API with **`gdelt_doc_fetch.keywords`** (plus country, language, **`fetch_start_date`** / optional **`fetch_end_date`** (defaults to today), **`window_months`** chunking, `sleep_between_queries_seconds`, etc.). Then run **step 2** (`gdelt-enrich-urls-bigquery.py` or local enrich) on that CSV — enrichment **adds GKG fields** to your URL list; it does **not** replace keyword-based discovery.
- **Optional (`--source bigquery`):** Queries public ``gdelt-bq.gdeltv2.gkg_partitioned``. **Default** in meta: **`gkg_theme_sets`** + **`gkg_geography`** (V2Themes + V2Locations LIKE). **`gdelt_doc_fetch.keywords` are not used in theme mode.** **Optional** domain meta section **`bigquery_gkg_fetch`** with **`"mode": "url_keywords"`** matches **`DocumentIdentifier LIKE`** (URL slug) **or** **`Extras LIKE`** using phrases derived from **`url_keyword_patterns`** and **`gdelt_doc_fetch.keywords`**, plus a hard **`V2Locations`** filter (e.g. `%#IN#%`), optional **`document_identifier_not_like`** exclusions, and **`partition_time_start` / `partition_time_end`** on `_PARTITIONTIME`. Omit `bigquery_gkg_fetch` or set **`mode`** to **`themes`** for theme-only discovery. The fetch CSV includes prefetched GKG columns (`gkg_v2_themes`, `gkg_v2_locations`, `gkg_v2_tone`, and when present `gkg_v2_persons`) — no `socialimage` column. Requires **`--project`** / `GOOGLE_CLOUD_PROJECT`; BigQuery bills for bytes scanned. Optional: `gdelt_doc_fetch.bigquery_max_rows` (default 50_000); optional `bigquery_gkg_fetch.extras_match_max_terms` caps Extras phrases. The script prints a short notice when this mode is selected.
- **Inputs:** `--meta` supplies `gdelt_doc_fetch` (keywords used only in **doc** mode) and `gkg_theme_sets` + `gkg_geography` (for BigQuery fetch and for enrich scripts).
- **Smoke test:** `--dry-run` — DOC: one window, ≤50 articles; BigQuery: `LIMIT` capped the same way.
- **DOC API logging & recovery:** Each keyword×date-window call appends one JSON line to `data/{prefix}_fetch_log.jsonl` (override with `--fetch-log`). Rate limits and transient HTTP errors are retried with exponential backoff (`--max-retries`, `--backoff-base`, `--backoff-max`). After a run with failures, **`--retry-failed`** re-executes only pairs whose *last* log line is still `failed`, then appends new URLs not already in `data/{prefix}_urls.csv` (incremental; new rows get new `event_id`s).
- **Outputs:** `data/{prefix}_urls.csv`, `data/{prefix}_urls_summary.txt` (with default meta, `hwc_*`), and in DOC mode `data/{prefix}_fetch_log.jsonl`.

### 2. GKG enrichment (choose one)

#### Option A — `scripts/gdelt-enrich-urls.py` (local GKG files)

Downloads GKG zip files and joins by URL. No GCP account required.

- **Inputs:** `data/hwc_urls.csv` (must include `url`, `seendate`); `--meta` for `gkg_theme_sets` and `gkg_geography`.
- **Outputs:** `data/{prefix}_urls_enriched.csv`, `data/{prefix}_urls_geocoded.csv`, `data/{prefix}_urls_high_confidence.csv`.

#### Option B — `scripts/gdelt-enrich-urls-bigquery.py`

Uploads URLs to a temporary BigQuery table and joins `gkg_partitioned` **unless** the input CSV already has prefetched columns **`gkg_v2_themes`**, **`gkg_v2_locations`**, and **`gkg_v2_tone`** (e.g. from **`gdelt-fetch-urls.py --source bigquery`**). In that case enrichment reuses those fields and skips the join (no BigQuery query). Pass **`--force-bigquery`** to always run the join. Requires GCP project, BigQuery API, and `gcloud auth application-default login` (or service account) when a join runs.

- **Inputs:** same as Option A (`data/hwc_urls.csv` by default); `--meta` for theme sets and geography (same as Option A).
- **Outputs:** same three CSVs as Option A, plus `data/{prefix}_urls_unmatched.csv` (articles with no GKG row).
- **Optional — story deduplication:** **`--dedupe-mode MODE`** with **`MODE`** = **`path`** (same normalized URL path across hosts), **`jaccard`** (similar path tokens from alphanumeric tokens + date window), or **`path,jaccard`** (run **path** first, then **jaccard** on what remains). Omit **`--dedupe-mode`** for **no** dedupe; optional meta **`gdelt_enrich_url_dedupe.modes`** applies when the flag is omitted (CLI overrides meta). **`--dedupe-report path.csv`** writes dropped rows with **`kept_url`** and **`dedupe_stage`** (`path` or `jaccard`) only when a dedupe mode is active.

### 3. `scripts/gdelt-get-full-text.py` (first run)

Fetches article text (Jina / trafilatura), runs the local Ollama model for structured extraction, optional Google geocoding.

- **Inputs:** default `data/{prefix}_urls_geocoded.csv` (GDELT/GKG columns; no `fetch_method` column); `--meta` for `llm_extraction` prompts and `gkg_theme_sets.high_confidence_theme_score_min` (used when sampling by `theme_score`).
- **Outputs:** `data/{prefix}_final_report.csv`, `outputs/{prefix}_final_report.txt`.
- **Parallel fetch:** **`--fetch-workers N`** (default **1**) runs **Jina + trafilatura** in up to **N** threads; **Ollama + geocode** still run **one row at a time** in order. Values **greater than 1** increase concurrent load on Jina and remote sites; use **1** to keep the previous sequential behavior (including the delay between URL fetches).

### 4. `scripts/gdelt-get-full-text.py` (optional second run — Selenium retry)

Retries rows where `fetch_method` indicates failure, using Chrome/Selenium, then merges with the rest of the previous run.

- **Inputs:** `--retry-failed-from` → default `data/{prefix}_final_report.csv` (must include `fetch_method`). The script merges location hints from `data/{prefix}_urls_geocoded.csv` (prefix inferred from the final report CSV filename).
- **Outputs:** default `data/{prefix}_final_report_updated.csv`.
- **Parallel Selenium:** **`--selenium-workers N`** (default **1**) uses **N** workers, each with its **own Chrome driver**, splitting failed rows across workers. DataFrame updates and CSV writes use a lock. Higher **N** uses more RAM and CPU; **1** matches the original single-browser loop.

### 5. `scripts/convert_csv_to_geojson.py`

Builds a point GeoJSON from rows marked as HWC events with valid coordinates; optional QGIS categorized style.

- **Inputs:** default `data/hwc_final_report_updated.csv` (or pass one or more `--input` CSVs); `meta/hwc_india_conflict_meta.json` (or your domain meta via `--meta`). With **multiple** `--input` files, URLs are de-duplicated (first wins) before building GeoJSON.
- **Outputs:** `--output-geojson` (default `outputs/hwc_points.geojson`). With multiple inputs, **`--output-csv` is required** for the merged de-duplicated CSV. With `--write-qml`, default `outputs/{prefix}_india_points.qml`.

The web map (`frontend`) includes a **legend with category checkboxes** (filter points), **Download GeoJSON**, and **Download QGIS style (QML)** when `outputs/{prefix}_india_points.qml` exists, plus a **Dashboard** link (opens `/dashboard?layer=…` in a new tab) with charts and methodology text from the layer meta.

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

Most scripts accept explicit `--input`, output paths (`--output`, `--output-geojson`, etc.), and related flags. Defaults point at `data/` and `outputs/` under the repository root so you can relocate files without editing code.

## Domain configuration (`meta/*.json`)

Copy `meta/event_domain_template.json` or adapt `meta/hwc_india_conflict_meta.json`. Scripts read:

- **`gdelt_doc_fetch`** — DOC API keywords, country/language, `max_records`, sleep, **`fetch_start_date`** / optional **`fetch_end_date`**, `window_months`, summary titles.
- **`gkg_theme_sets`** — `primary_themes` and `secondary_themes` (GKG V2Themes codes; domain vs harm/incident signal), `high_confidence_theme_score_min`.
- **`gkg_geography`** — `location_country_codes`, `subnational_loc_types` for GKG V2Locations filtering.
- **`llm_extraction`** — use **`system_prompt_lines`** and **`extraction_prompt_lines`** (arrays of strings) so long prompts do not require heavy JSON escaping. The extraction template must include `{pub_date}`, `{url}`, `{gdelt_locations}`, and `{article_text}`. Literal `{` / `}` in the embedded JSON example must appear as doubled `{{` / `}}` in the joined string (same rule as Python `str.format`).
