---
name: Jaccard URL dedupe enrich
overview: Add optional jaccard dedupe in `gdelt-enrich-urls-bigquery.py` (token sets, blocking, union-find, date-aware thresholds). Support running **path dedupe first, then jaccard** on the surviving rows when both are enabled—cheap exact syndication collapse, then fuzzy cross-outlet matches.
todos:
  - id: tokenize-jaccard
    content: Add url_path_token_set, jaccard score, Option A threshold + date days
    status: pending
  - id: block-unionfind
    content: Inverted-index candidates within date_window_days; union-find; pick earliest seendate
    status: pending
  - id: wire-cli-meta
    content: "Only --dedupe-mode (path|jaccard|path,jaccard) enables dedupe; remove --dedupe-story-urls; meta modes when CLI absent; optional --dedupe-report"
    status: pending
  - id: readme-dedupe
    content: "README: --dedupe-mode only; meta modes fallback; remove old dedupe flags doc"
    status: pending
isProject: false
---

# Jaccard + date–window story dedupe (BigQuery enrich)

## Problem

Rows in [`data/avianmortality_urls_geocoded.csv`](data/avianmortality_urls_geocoded.csv) (lines 6–17) are the same wire story with **different URL shapes** (Reuters `idUSKBN…`, agriculture.com slug-only, nasdaq with date suffix, etc.). The current [`story_dedupe_key`](scripts/gdelt-enrich-urls-bigquery.py) only matches **identical normalized paths**, so these are not collapsed.

## Approach

1. **Tokenize** each URL from **path only** (ignore scheme/host/query): `unquote`, lowercase, split on `/`, `-`, `_`, `.`; drop tokens that are empty, pure digits, or shorter than a configurable min length (e.g. 2–3) to reduce noise from `id`, `cms`, `html`.

2. **Jaccard similarity** between token sets `A`, `B`: `|A ∩ B| / |A ∪ B|`.

3. **Date proximity**: parse `seendate` as already done (`%Y%m%d%H%M%S`). Define `days_apart = abs(dt_i - dt_j).days`. Combine with similarity in one of two equivalent ways (pick one for implementation clarity):
   - **Option A (threshold curve):** mark duplicate if `jaccard >= j_high` **or** `(jaccard >= j_low and days_apart <= max_days)` with defaults like `j_high=0.88`, `j_low=0.72`, `max_days=3`.
   - **Option B (weighted score):** `score = jaccard + w * exp(-days_apart / tau)` and duplicate if `score >= s_min` — slightly more opaque; **prefer Option A** unless you want a single knob.

4. **Transitive duplicates:** if A~B and B~C, keep one row for {A,B,C}. Use **union-find (disjoint set)** on row indices after collecting all duplicate pairs.

5. **Representative row:** within each component, keep the row with **minimum `seendate`** (earliest), tie-break by `url` string (same as today).

6. **Performance (critical for ~10k–20k rows):** naive all-pairs is O(n²). Use **blocking**:
   - Parse ordinal **day** from `seendate` (date part only).
   - **Inverted index:** map each significant token (length ≥ L, not stopword) → list of row indices that contain it.
   - For each row `i`, **candidate set** = rows that share at least one indexed token with `i` **and** whose day is in `[day_i - W, day_i + W]` (e.g. `W=4`).
   - Only compute Jaccard for `(i, j)` with `j > i` in candidates (or symmetric once).
   - Union-find merge when threshold satisfied.

   Optional extra block: require `|A ∩ B| >= min_intersection` (e.g. 4) before calling duplicate to reduce false merges on generic words.

7. **Combined pipeline (path then jaccard):** When both are enabled, run **in fixed order**:
   1. **Path dedupe** — same as today: identical normalized path across hosts (fast, high precision).
   2. **Jaccard dedupe** — on the **dataframe after step 1** (fewer rows, less work; avoids comparing rows already collapsed by path).

   Either stage alone remains valid: `modes: ["path"]` only, or `modes: ["jaccard"]` only, or `modes: ["path", "jaccard"]` for the full pipeline.

   Logging: print row counts after each stage (e.g. `after path: N`, `after jaccard: M`). Dropped-row report (if requested) can list which pass dropped each row (`dedupe_pass: path|jaccard`) or write two optional CSV paths — simplest is **one report** with a column **`dedupe_stage`** = `path` | `jaccard`.

## Config surface

**CLI (deduplication switch):** a single optional argument **`--dedupe-mode`**. If it is **omitted**, **no deduplication** runs.

- **Value:** one string, comma-separated (no spaces required): **`path`**, **`jaccard`**, or **`path,jaccard`** (order fixed: path then jaccard when both appear). Reject invalid tokens or wrong order with a clear error.
- **No other CLI flags** are required to enable dedupe; remove legacy **`--dedupe-story-urls`** (and any duplicate aliases) when implementing this plan.

**Meta (`gdelt_enrich_url_dedupe`):** optional tuning only; **`modes`** here applies **when `--dedupe-mode` is not passed** (CLI wins if both present). If neither CLI nor meta supplies a non-empty `modes` list, **no dedupe**.

| Field | Meaning |
|-------|--------|
| `modes` | list of strings, same semantics as CLI: `["path"]`, `["jaccard"]`, or `["path", "jaccard"]` |
| `jaccard_min` | float, default ~0.88 (strong duplicate) |
| `jaccard_min_near_date` | float, default ~0.72 (when dates close) |
| `max_days_apart` | int, default 3 (for the relaxed branch) |
| `date_window_days` | int, default 4 (blocking window W) |
| `min_token_len` | int, default 3 |
| `stopwords` | optional list of tokens to ignore |

Optional **`--dedupe-report path.csv`** remains for writing dropped rows (with `dedupe_stage`); it does not enable dedupe by itself—**without `--dedupe-mode` (and without meta `modes`), still no dedupe.**

## Files to change

- [`scripts/gdelt-enrich-urls-bigquery.py`](scripts/gdelt-enrich-urls-bigquery.py): `url_path_token_set()`, blocking + pair loop, union-find; split existing logic into `dedupe_articles_path()` and `dedupe_articles_jaccard()`; orchestrator loops resolved **`modes`** in order; replace **`--dedupe-story-urls`** / `enabled` gating with **`--dedupe-mode`** + optional meta `modes`; optional **`dedupe_stage`** in `--dedupe-report`.
- [`README.md`](README.md): document **`--dedupe-mode`**, meta `modes` fallback, path then jaccard, false-positive risk, tuning; remove old `--dedupe-story-urls` text.

## Risks / docs

- **False positives:** similar headlines for different events same week — mitigate with high `jaccard_min` and `min_intersection`.
- **False negatives:** very short slugs — may need manual tuning.

## Testing

- Small fixture: subset of CSV lines 6–17; expect one row per story cluster after **`path,jaccard`** pipeline.
- `py_compile` on the script.
