# Human–wildlife conflict (India) — GDELT pipeline

This project pulls India HWC-related articles from GDELT, enriches them with GKG metadata, optionally runs full-text extraction with a local LLM, and exports map-ready GeoJSON.

## Repository layout

| Path | Purpose |
|------|---------|
| `data/` | CSV inputs and intermediate outputs (default paths for scripts) |
| `outputs/` | GeoJSON, QGIS QML styles, and the narrative text report from full-text extraction |
| `meta/` | Domain metadata: GDELT keywords, GKG theme sets, LLM prompts, GeoJSON/QML (`hwc_india_conflict_meta.json`, `event_domain_template.json`) |
| `scripts/domain_meta.py` | Shared loader for meta JSON (used by fetch, enrich, full-text scripts) |
| `scripts/` | Runnable pipeline steps |

Run scripts from the **repository root** (paths below assume that).

---

## Execution order

### 1. `scripts/gdelt-fetch-urls.py`

Queries the GDELT DOC API for keywords, deduplicates by URL.

- **Inputs:** `--meta` (default `meta/hwc_india_conflict_meta.json`) supplies `gdelt_doc_fetch`: keywords, country, language, query limits, date windows.
- **Outputs:** `data/hwc_urls.csv`, `data/hwc_urls_summary.txt`.

### 2. GKG enrichment (choose one)

#### Option A — `scripts/gdelt-enrich-urls.py` (local GKG files)

Downloads GKG zip files and joins by URL. No GCP account required.

- **Inputs:** `data/hwc_urls.csv` (must include `url`, `seendate`); `--meta` for `gkg_theme_sets` and `gkg_geography`.
- **Outputs:** `data/hwc_urls_enriched.csv`, `data/hwc_urls_geocoded.csv`, `data/hwc_urls_high_confidence.csv`.

#### Option B — `scripts/gdelt-enrich-urls-bigquery.py`

Uploads URLs to a temporary BigQuery table and joins `gkg_partitioned`. Requires GCP project, BigQuery API, and `gcloud auth application-default login` (or service account).

- **Inputs:** same as Option A (`data/hwc_urls.csv` by default); `--meta` for theme sets and geography (same as Option A).
- **Outputs:** same three CSVs as Option A, plus `data/hwc_urls_unmatched.csv` (articles with no GKG row).

### 3. `scripts/gdelt-get-full-text.py` (first run)

Fetches article text (Jina / trafilatura), runs the local Ollama model for structured extraction, optional Google geocoding.

- **Inputs:** `data/hwc_urls_geocoded.csv` (GDELT/GKG columns; no `fetch_method` column); `--meta` for `llm_extraction` prompts and `gkg_theme_sets.high_confidence_theme_score_min` (used when sampling by `theme_score`).
- **Outputs:** `data/hwc_final_report.csv`, `outputs/hwc_final_report.txt`.

### 4. `scripts/gdelt-get-full-text.py` (optional second run — Selenium retry)

Retries rows where `fetch_method` indicates failure, using Chrome/Selenium, then merges with the rest of the previous run.

- **Inputs:** `--retry-failed-from` → default `data/hwc_final_report.csv` (must include `fetch_method`). The script merges location hints from `data/hwc_urls_geocoded.csv` when present beside the pilot CSV.
- **Outputs:** default `data/hwc_final_report_updated.csv`.

### 5. `scripts/convert_csv_to_geojson.py`

Builds a point GeoJSON from rows marked as HWC events with valid coordinates; optional QGIS categorized style.

- **Inputs:** default `data/hwc_final_report_updated.csv` (or `data/hwc_final_report.csv` if you have not run the retry step); `meta/hwc_india_conflict_meta.json`.
- **Outputs:** default `outputs/hwc_points.geojson`; with `--write-qml`, default `outputs/hwc_india_points.qml`.

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
