# Human–wildlife conflict (India) — GDELT pipeline

This project pulls India HWC-related articles from GDELT, enriches them with GKG metadata, optionally runs full-text extraction with a local LLM, and exports map-ready GeoJSON.

## Repository layout

| Path | Purpose |
|------|---------|
| `data/` | CSV inputs and intermediate outputs (default paths for scripts) |
| `outputs/` | GeoJSON, QGIS QML styles, and the narrative text report from full-text extraction |
| `meta/` | Domain metadata: GDELT keywords, GKG theme sets, LLM prompts, GeoJSON/QML (`hwc_india_conflict_meta.json`, `event_domain_template.json`) |
| `scripts/domain_meta.py` | Shared loader for meta JSON (used by fetch, enrich, full-text scripts) |
| `scripts/domain_paths.py` | `output_prefix(meta)` and standard `data/` / `outputs/` filenames; deterministic `event_id` backfill |
| `scripts/` | Runnable pipeline steps |
| `server/` | FastAPI app: layers, GeoJSON, styles, suggested edits, moderator apply (`python -m uvicorn server.main:app`) |
| `frontend/` | Vite + React + MapLibre map UI (`npm install` / `npm run dev` or `npm run build`) |

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

- **Inputs:** `--retry-failed-from` → default `data/{prefix}_final_report.csv` (must include `fetch_method`). The script merges location hints from `data/{prefix}_urls_geocoded.csv` (prefix inferred from the pilot CSV filename).
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
4. **Production-style**: `cd frontend && npm run build` then open `http://127.0.0.1:<PORT>/` (API serves `frontend/dist` when present).

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
| `VITE_API_HOST` | frontend `.env` | Host for `/api` proxy (default `127.0.0.1`) |
| `VITE_API_PORT` | frontend `.env` | Port for `/api` proxy (must match server `PORT`) |

Moderators are listed in `data/moderators.json` (bcrypt hashes only). Generate a hash with:

```bash
python scripts/hash_password.py
```

The sample file ships with user `admin` and password **`changeme`** (change before any real deployment).

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
