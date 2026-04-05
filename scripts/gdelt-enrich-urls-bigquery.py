"""
GDELT BigQuery Enrichment — Geocoding + Themes via URL join
============================================================
Takes your data/hwc_urls.csv, uploads the URLs + dates to a
BigQuery temp table, joins against gdelt-bq.gdeltv2.gkg_partitioned
(partitioned by day → cheap), and returns GKG fields for each article.

If the CSV already contains ``gkg_v2_themes``, ``gkg_v2_locations``, and
``gkg_v2_tone`` (from ``gdelt-fetch-urls.py --source bigquery``), the script
parses those in-process and **skips** the BigQuery join unless you pass
``--force-bigquery``.

Optional ``--dedupe-mode path``, ``jaccard``, or ``path,jaccard`` deduplicates
rows before the join (see README).

Why gkg_partitioned?
  The full GKG table is 3.6TB. The partitioned version lets BigQuery
  scan only the days your articles appeared on — typically just a few
  GB for 1315 articles, well within the 1TB free monthly quota.

Setup (one-time):
  1. Install deps:
       pip install pandas google-cloud-bigquery db-dtypes tqdm
  2. Authenticate:
       gcloud auth application-default login
     OR set env var:
       export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
  3. Create a GCP project (free tier is fine) and note your PROJECT_ID.
  4. Enable the BigQuery API in your GCP project console.

  Set ``GOOGLE_CLOUD_PROJECT`` in repo root ``.env`` (see ``.env.example``) or pass ``--project``.

Usage:
    python scripts/gdelt-enrich-urls-bigquery.py --project YOUR_GCP_PROJECT_ID
    python scripts/gdelt-enrich-urls-bigquery.py   # uses GOOGLE_CLOUD_PROJECT from .env
    python scripts/gdelt-enrich-urls-bigquery.py --project myproject --input data/hwc_urls.csv

Outputs (under data/ by default):
    hwc_urls_enriched.csv        — all articles with GKG fields added
    hwc_urls_geocoded.csv        — subset with ≥1 India lat/lon resolved
    hwc_urls_high_confidence.csv — subset matching primary + secondary theme score
    hwc_urls_unmatched.csv       — URLs not found in GKG (paywall/404/pre-GDELT)
"""

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import unquote, urlparse

import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from domain_meta import get_gkg_geography, get_gkg_theme_sets, load_domain_meta  # noqa: E402
from domain_paths import (  # noqa: E402
    ensure_event_id_column,
    load_repo_env,
    meta_path_default,
    output_prefix,
    urls_csv,
    urls_enriched_csv,
    urls_geocoded_csv,
    urls_high_confidence_csv,
    urls_unmatched_csv,
)

# ── GKG V2Locations parser ─────────────────────────────────────────────────────
# Format per location block: TYPE#NAME#COUNTRYCODE#ADM1CODE#LAT#LON#FEATUREID
# Blocks separated by ";"

def parse_v2locations(loc_string: str) -> list[dict]:
    if not loc_string or pd.isna(loc_string):
        return []
    results = []
    for block in str(loc_string).split(";"):
        parts = block.strip().split("#")
        if len(parts) < 6:
            continue
        try:
            loc_type = int(parts[0]) if parts[0].isdigit() else 0
            lat = float(parts[4]) if parts[4] else None
            lon = float(parts[5]) if parts[5] else None
        except (ValueError, IndexError):
            continue
        results.append({
            "loc_type":  loc_type,
            "loc_name":  parts[1] if len(parts) > 1 else "",
            "country":   parts[2] if len(parts) > 2 else "",
            "adm1":      parts[3] if len(parts) > 3 else "",
            "lat":       lat,
            "lon":       lon,
        })
    return results


def india_locations(locs: list[dict], country_codes: tuple[str, ...]) -> list[dict]:
    return [
        l for l in locs
        if l.get("country") in country_codes
        and l.get("lat") is not None
        and l.get("lon") is not None
    ]


def best_india_location(
    locs: list[dict],
    country_codes: tuple[str, ...],
    subnational_types: set[int],
) -> dict | None:
    india = india_locations(locs, country_codes)
    if not india:
        return None
    sub = [l for l in india if l["loc_type"] in subnational_types]
    return sub[0] if sub else india[0]


def parse_themes(theme_str: str) -> set[str]:
    """V2Themes format: THEME,charoffset;THEME,charoffset;..."""
    if not theme_str or pd.isna(theme_str):
        return set()
    return {item.split(",")[0].strip()
            for item in str(theme_str).split(";")
            if item.strip()}


def score_themes(
    themes: set[str],
    primary_themes: set[str],
    secondary_themes: set[str],
) -> tuple[int, list[str]]:
    primary_hits = themes & primary_themes
    secondary_hits = themes & secondary_themes
    score = (1 if primary_hits else 0) + (2 if secondary_hits else 0)
    return score, sorted(primary_hits | secondary_hits)


def parse_tone(tone_str: str) -> float | None:
    try:
        return float(str(tone_str).split(",")[0])
    except (ValueError, TypeError, AttributeError):
        return None


# Trailing digit blob on last path segment (e.g. ANI YYYYMMDDHHMMSS on slug).
# Reverted if stripping would remove the whole segment or leave only digits.
_TRAILING_SLUG_DIGIT_BLOB = re.compile(r"\d{8,}$")

_DEFAULT_URL_STOPWORDS = frozenset({
    "www", "com", "html", "htm", "php", "aspx", "jsp", "cms", "news",
    "article", "articles", "story", "stories", "id", "page", "index",
})


def story_dedupe_key(url: str) -> str:
    """
    Normalize URL path for cross-domain wire syndication dedupe (same path, many hosts).
    Ignores scheme and host. Strips a long trailing digit run from the last segment only.
    """
    if not url or (isinstance(url, float) and pd.isna(url)):
        return ""
    u = str(url).strip()
    if not u:
        return ""
    try:
        parsed = urlparse(u)
    except Exception:
        return u.lower()
    path = parsed.path or ""
    path = unquote(path)
    segments = [p for p in path.split("/") if p]
    if not segments:
        return u.lower()
    last = segments[-1]
    stripped = _TRAILING_SLUG_DIGIT_BLOB.sub("", last)
    if stripped and not stripped.isdigit() and stripped != last:
        segments = segments[:-1] + [stripped]
    path_norm = "/" + "/".join(segments).lower().rstrip("/")
    if path_norm in ("/", ""):
        return u.lower()
    return path_norm


def dedupe_articles_path(articles: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Keep one row per story_dedupe_key; earliest seendate wins, then url for tie-break.
    Returns (kept, dropped) with dropped empty when nothing removed.
    """
    n_in = len(articles)
    df = articles.copy()
    df["_story_dedupe_key"] = df["url"].map(story_dedupe_key)
    empty_key = df["_story_dedupe_key"].eq("") | df["_story_dedupe_key"].isna()
    df.loc[empty_key, "_story_dedupe_key"] = (
        df.loc[empty_key, "url"].astype(str).str.strip().str.lower()
    )
    still_empty = df["_story_dedupe_key"].eq("")
    if still_empty.any():
        if "event_id" in df.columns:
            df.loc[still_empty, "_story_dedupe_key"] = (
                "__event__" + df.loc[still_empty, "event_id"].astype(str)
            )
        else:
            df.loc[still_empty, "_story_dedupe_key"] = (
                "__row__" + df.loc[still_empty].index.astype(str)
            )
    sort_date = pd.to_datetime(
        df["seendate"], format="%Y%m%d%H%M%S", utc=True, errors="coerce"
    )
    df = df.assign(_sort_date=sort_date)
    df = df.sort_values(
        by=["_sort_date", "url"],
        na_position="last",
        kind="mergesort",
    )
    dup = df.duplicated(subset="_story_dedupe_key", keep="first")
    n_drop = int(dup.sum())
    kept = df.loc[~dup].copy()
    dropped = pd.DataFrame()
    if n_drop:
        dropped = df.loc[dup].copy()
        first_urls = (
            kept.loc[:, ["_story_dedupe_key", "url"]]
            .rename(columns={"url": "kept_url"})
        )
        dropped = dropped.merge(first_urls, on="_story_dedupe_key", how="left")
        dropped["dedupe_stage"] = "path"
        dropped = dropped.drop(columns=["_sort_date"], errors="ignore")
    kept = kept.drop(columns=["_story_dedupe_key", "_sort_date"], errors="ignore")
    print(f"  → Path dedupe: removed {n_drop} row(s) ({n_in} → {len(kept)})")
    return kept, dropped


def url_path_token_set(
    url: str,
    min_token_len: int,
    extra_stopwords: set[str],
) -> frozenset[str]:
    """Alphanumeric tokens from URL path for Jaccard similarity."""
    if not url or (isinstance(url, float) and pd.isna(url)):
        return frozenset()
    try:
        parsed = urlparse(str(url).strip())
    except Exception:
        return frozenset()
    path = unquote(parsed.path or "").lower()
    parts = re.findall(r"[a-z0-9]+", path)
    sw = _DEFAULT_URL_STOPWORDS | extra_stopwords
    out: list[str] = []
    for p in parts:
        if len(p) < min_token_len:
            continue
        if p.isdigit():
            continue
        p2 = _TRAILING_SLUG_DIGIT_BLOB.sub("", p)
        if len(p2) >= min_token_len and not p2.isdigit() and p2 != p:
            p = p2
        if len(p) < min_token_len or p in sw:
            continue
        out.append(p)
    return frozenset(out)


def _token_jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a and not b:
        return 1.0
    u = len(a | b)
    if u == 0:
        return 0.0
    return len(a & b) / u


class _UnionFind:
    def __init__(self, n: int) -> None:
        self._p = list(range(n))

    def find(self, x: int) -> int:
        if self._p[x] != x:
            self._p[x] = self.find(self._p[x])
        return self._p[x]

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._p[rb] = ra


def dedupe_articles_jaccard(
    articles: pd.DataFrame,
    *,
    jaccard_min: float,
    jaccard_min_near_date: float,
    max_days_apart: int,
    date_window_days: int,
    min_token_len: int,
    min_intersection: int,
    stopwords: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cluster rows by URL token Jaccard + date window; keep earliest seendate per cluster.
    """
    n_in = len(articles)
    if n_in <= 1:
        return articles.copy(), pd.DataFrame()

    print(
        "  [jaccard] start: "
        f"n={n_in}, jaccard≥{jaccard_min} or "
        f"(≥{jaccard_min_near_date} & date≤{max_days_apart}d), "
        f"±{date_window_days}d window, min_intersection={min_intersection}, "
        f"min_token_len={min_token_len}"
    )

    df = articles.copy().reset_index(drop=True)
    urls = df["url"].tolist()
    token_sets: list[frozenset[str]] = []
    for u in tqdm(urls, desc="  [jaccard] tokenize URLs", unit="url", leave=False):
        token_sets.append(url_path_token_set(u, min_token_len, stopwords))
    n_empty_tokens = sum(1 for ts in token_sets if not ts)
    lens = [len(ts) for ts in token_sets]
    avg_tok = sum(lens) / len(lens) if lens else 0.0
    print(
        f"  [jaccard] token sets: empty={n_empty_tokens}, "
        f"avg_tokens={avg_tok:.1f}, max_tokens={max(lens) if lens else 0}"
    )

    dts = pd.to_datetime(
        df["seendate"], format="%Y%m%d%H%M%S", utc=True, errors="coerce"
    )
    day_ord = (
        (dts.dt.normalize() - pd.Timestamp("1970-01-01", tz="UTC"))
        .dt.days.fillna(-999999)
        .astype(int)
    )

    inverted: dict[str, list[int]] = defaultdict(list)
    for i, ts in enumerate(
        tqdm(token_sets, desc="  [jaccard] inverted index", unit="row", leave=False)
    ):
        for t in ts:
            if len(t) >= min_token_len:
                inverted[t].append(i)
    n_indexed_tokens = len(inverted)
    max_postings = max((len(v) for v in inverted.values()), default=0)
    print(
        f"  [jaccard] index: {n_indexed_tokens} token keys, "
        f"max postings/term={max_postings}"
    )

    uf = _UnionFind(len(df))
    pairs: set[tuple[int, int]] = set()
    for i in tqdm(range(len(df)), desc="  [jaccard] candidate pairs", unit="row", leave=False):
        di = int(day_ord.iloc[i])
        for t in token_sets[i]:
            for j in inverted.get(t, []):
                if j <= i:
                    continue
                dj = int(day_ord.iloc[j])
                if abs(di - dj) > date_window_days:
                    continue
                pairs.add((i, j))
    print(f"  [jaccard] candidate pairs: {len(pairs)}")

    n_skip_inter = 0
    n_skip_jacc = 0
    n_merge = 0
    for i, j in tqdm(
        pairs,
        desc="  [jaccard] evaluate & union",
        unit="pair",
        leave=False,
    ):
        inter = len(token_sets[i] & token_sets[j])
        if inter < min_intersection:
            n_skip_inter += 1
            continue
        jac = _token_jaccard(token_sets[i], token_sets[j])
        days_apart = abs(int(day_ord.iloc[i]) - int(day_ord.iloc[j]))
        if jac >= jaccard_min or (
            jac >= jaccard_min_near_date and days_apart <= max_days_apart
        ):
            if uf.find(i) != uf.find(j):
                uf.union(i, j)
                n_merge += 1
        else:
            n_skip_jacc += 1

    print(
        "  [jaccard] unions: merged_groups="
        f"{n_merge}, skip_low_intersection={n_skip_inter}, "
        f"skip_below_jaccard={n_skip_jacc}"
    )

    groups: dict[int, list[int]] = defaultdict(list)
    for i in range(len(df)):
        groups[uf.find(i)].append(i)

    n_multi = sum(1 for mem in groups.values() if len(mem) > 1)
    print(
        f"  [jaccard] clusters: {len(groups)} (multi-row: {n_multi}), "
        "picking earliest seendate per cluster..."
    )

    kept_rows: list[int] = []
    dropped_chunks: list[pd.DataFrame] = []
    for _root, members in groups.items():
        if len(members) == 1:
            kept_rows.append(members[0])
            continue
        def _member_key(m: int) -> tuple:
            dt = dts.iloc[m]
            u = str(df.iloc[m]["url"])
            if pd.isna(dt):
                return (1, u)
            return (0, dt, u)

        best = min(members, key=_member_key)
        kept_rows.append(best)
        for m in members:
            if m != best:
                row = df.iloc[m : m + 1].copy()
                row["kept_url"] = df.iloc[best]["url"]
                row["dedupe_stage"] = "jaccard"
                dropped_chunks.append(row)

    kept = df.iloc[sorted(kept_rows)].copy()
    dropped = (
        pd.concat(dropped_chunks, ignore_index=True) if dropped_chunks else pd.DataFrame()
    )
    n_drop = n_in - len(kept)
    print(f"  → Jaccard dedupe: removed {n_drop} row(s) ({n_in} → {len(kept)})")
    return kept, dropped


def parse_dedupe_mode_cli(s: str | None) -> list[str] | None:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    parts = [p.strip().lower() for p in s.split(",") if p.strip()]
    allowed = {"path", "jaccard"}
    for p in parts:
        if p not in allowed:
            sys.exit(
                f"ERROR: --dedupe-mode invalid token {p!r}; "
                "use path, jaccard, or path,jaccard"
            )
    if parts.count("path") > 1 or parts.count("jaccard") > 1:
        sys.exit("ERROR: duplicate mode in --dedupe-mode")
    if "path" in parts and "jaccard" in parts and parts != ["path", "jaccard"]:
        sys.exit("ERROR: use --dedupe-mode path,jaccard (path must come before jaccard)")
    return parts


def dedupe_modes_from_meta(meta: dict) -> list[str]:
    sec = meta.get("gdelt_enrich_url_dedupe")
    if not isinstance(sec, dict):
        return []
    m = sec.get("modes")
    if m is None:
        return []
    if isinstance(m, str):
        parts = [p.strip().lower() for p in m.split(",") if p.strip()]
    elif isinstance(m, (list, tuple)):
        parts = [str(p).strip().lower() for p in m if str(p).strip()]
    else:
        return []
    seen: set[str] = set()
    raw: list[str] = []
    for p in parts:
        if p not in ("path", "jaccard"):
            sys.exit(f"ERROR: gdelt_enrich_url_dedupe.modes invalid entry {p!r}")
        if p not in seen:
            seen.add(p)
            raw.append(p)
    out: list[str] = []
    if "path" in seen:
        out.append("path")
    if "jaccard" in seen:
        out.append("jaccard")
    return out


def jaccard_settings_from_meta(meta: dict) -> dict[str, object]:
    sec = meta.get("gdelt_enrich_url_dedupe")
    if not isinstance(sec, dict):
        sec = {}
    sw = sec.get("stopwords") or []
    stop = {str(x).lower() for x in sw} if isinstance(sw, list) else set()
    return {
        "jaccard_min": float(sec.get("jaccard_min", 0.75)),
        "jaccard_min_near_date": float(sec.get("jaccard_min_near_date", 0.72)),
        "max_days_apart": int(sec.get("max_days_apart", 3)),
        "date_window_days": int(sec.get("date_window_days", 4)),
        "min_token_len": int(sec.get("min_token_len", 3)),
        "min_intersection": int(sec.get("min_intersection", 4)),
        "stopwords": stop,
    }


def resolve_dedupe_modes(cli_mode: str | None, meta: dict) -> list[str]:
    parsed = parse_dedupe_mode_cli(cli_mode)
    if parsed is not None:
        return parsed
    return dedupe_modes_from_meta(meta)


def run_article_dedupes(
    articles: pd.DataFrame,
    modes: list[str],
    report_path: str | None,
    jaccard_kw: dict[str, object],
) -> tuple[pd.DataFrame, int]:
    df = articles
    total_drop = 0
    drop_parts: list[pd.DataFrame] = []
    for mode in modes:
        if mode == "path":
            df, dropped = dedupe_articles_path(df)
        elif mode == "jaccard":
            df, dropped = dedupe_articles_jaccard(df, **jaccard_kw)
        else:
            sys.exit(f"ERROR: unknown dedupe mode {mode!r}")
        total_drop += len(dropped)
        if not dropped.empty:
            drop_parts.append(dropped)
    if report_path and drop_parts:
        out = pd.concat(drop_parts, ignore_index=True)
        Path(report_path).parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(report_path, index=False)
        print(f"  → Wrote dedupe report: {report_path}")
    return df, total_drop


# ── BigQuery helpers ───────────────────────────────────────────────────────────

def upload_urls_as_temp_table(client: bigquery.Client, project: str,
                               articles: pd.DataFrame) -> str:
    """
    Upload the URL + date list to a temporary BigQuery dataset/table.
    Returns the fully-qualified table ID.
    """
    dataset_id = "gdelt_hwc_temp"
    table_id   = "article_urls"
    full_table = f"{project}.{dataset_id}.{table_id}"

    # Create dataset if it doesn't exist
    dataset_ref = bigquery.Dataset(f"{project}.{dataset_id}")
    dataset_ref.location = "US"
    try:
        client.create_dataset(dataset_ref, exists_ok=True)
        print(f"  ✓ Dataset ready: {project}.{dataset_id}")
    except Exception as e:
        print(f"  ⚠ Dataset creation: {e}")

    # Prepare upload df — need url and a date for partition pruning
    upload_df = articles[["url", "seendate"]].copy()
    upload_df.columns = ["DocumentIdentifier", "article_date"]
    # Normalise to calendar dates (Python date); strings break pyarrow DATE upload
    upload_df["article_date"] = pd.to_datetime(
        upload_df["article_date"], utc=True, errors="coerce"
    ).dt.date
    upload_df = upload_df.dropna(subset=["DocumentIdentifier", "article_date"])

    schema = [
        bigquery.SchemaField("DocumentIdentifier", "STRING"),
        bigquery.SchemaField("article_date",        "DATE"),
    ]
    # Same table name every run → replaces rows; no duplicate temp tables
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(upload_df, full_table,
                                           job_config=job_config)
    job.result()
    print(f"  ✓ Uploaded {len(upload_df)} URLs → {full_table}")
    return full_table


def build_query(url_table: str, start_date: str, end_date: str) -> str:
    """
    Join the uploaded URL table against gkg_partitioned.
    Uses _PARTITIONTIME to limit bytes scanned to only relevant days.
    V2Locations and V2Themes are the key fields we need.
    """
    return f"""
    SELECT
        g.DocumentIdentifier,
        g.DATE             AS gkg_date,
        g.SourceCommonName AS source_name,
        g.V2Themes,
        g.V2Locations,
        g.V2Persons,
        g.V2Tone
    FROM
        `gdelt-bq.gdeltv2.gkg_partitioned` AS g
    INNER JOIN
        `{url_table}` AS u
        ON g.DocumentIdentifier = u.DocumentIdentifier
    WHERE
        g._PARTITIONTIME >= TIMESTAMP("{start_date}")
        AND g._PARTITIONTIME <  TIMESTAMP("{end_date}")
    """


def parse_prefetched_gkg_rows(
    articles: pd.DataFrame,
    primary_themes: set[str],
    secondary_themes: set[str],
    country_codes: tuple[str, ...],
    subnational_types: set[int],
) -> pd.DataFrame:
    """
    Build the same per-URL GKG parse as the BigQuery join path, using ``gkg_v2_*``
    columns from ``gdelt-fetch-urls.py --source bigquery`` output.
    """
    rows: list[dict] = []
    for _, row in tqdm(articles.iterrows(), total=len(articles), unit="article"):
        url = str(row.get("url", "") or "").strip()
        t_raw = str(row.get("gkg_v2_themes", "") or "")
        l_raw = str(row.get("gkg_v2_locations", "") or "")
        if not t_raw.strip() and not l_raw.strip():
            rows.append({
                "DocumentIdentifier": url,
                "gkg_found": False,
                "source_name": "",
                "v2tone": None,
                "theme_score": None,
                "matched_themes": "",
                "v2themes_raw": "",
                "n_india_locations": None,
                "best_lat": None,
                "best_lon": None,
                "best_location_name": None,
                "best_adm1": None,
                "all_india_locations": "",
                "v2persons": str(row.get("gkg_v2_persons", "") or "")[:300],
            })
            continue
        locs = parse_v2locations(l_raw)
        themes = parse_themes(t_raw)
        score, matched = score_themes(themes, primary_themes, secondary_themes)
        best = best_india_location(locs, country_codes, subnational_types)
        india = india_locations(locs, country_codes)
        rows.append({
            "DocumentIdentifier": url,
            "gkg_found": True,
            "source_name": "",
            "v2tone": parse_tone(row.get("gkg_v2_tone", "")),
            "theme_score": score,
            "matched_themes": "; ".join(matched),
            "v2themes_raw": t_raw[:400],
            "n_india_locations": len(india),
            "best_lat": best["lat"] if best else None,
            "best_lon": best["lon"] if best else None,
            "best_location_name": best["loc_name"] if best else None,
            "best_adm1": best["adm1"] if best else None,
            "all_india_locations": "; ".join(
                f"{l['loc_name']} ({l['lat']:.3f},{l['lon']:.3f})"
                for l in india
            ) if india else "",
            "v2persons": str(row.get("gkg_v2_persons", "") or "")[:300],
        })
    return pd.DataFrame(rows).rename(columns={"DocumentIdentifier": "url"})


# ── Main ──────────────────────────────────────────────────────────────────────

def main(
    project: str,
    input_csv: str,
    out_enriched: str,
    out_geocoded: str,
    out_hc: str,
    out_unmatched: str,
    dry_run: bool,
    meta_path: str,
    url_table_reuse: str | None = None,
    force_bigquery: bool = False,
    dedupe_mode: str | None = None,
    dedupe_report: str | None = None,
):

    meta = load_domain_meta(meta_path)
    primary_themes, secondary_themes, hc_min = get_gkg_theme_sets(meta)
    country_codes, subnational_types = get_gkg_geography(meta)

    # ── Load articles ──────────────────────────────────────────────────────
    print(f"\nLoading: {input_csv}")
    articles = pd.read_csv(input_csv, dtype=str)
    articles = ensure_event_id_column(articles)
    print(f"  → {len(articles)} articles")

    for col in ("url", "seendate"):
        if col not in articles.columns:
            sys.exit(f"ERROR: Input CSV missing column '{col}'. "
                     "Expected output from scripts/gdelt-fetch-urls.py")

    modes = resolve_dedupe_modes(dedupe_mode, meta)
    if dedupe_report and not modes:
        sys.exit(
            "ERROR: --dedupe-report requires --dedupe-mode or "
            "non-empty gdelt_enrich_url_dedupe.modes in meta."
        )
    if modes:
        jkw = jaccard_settings_from_meta(meta)
        articles, _ = run_article_dedupes(articles, modes, dedupe_report, jkw)

    prefetch_ok = (
        not force_bigquery
        and {"gkg_v2_themes", "gkg_v2_locations", "gkg_v2_tone"}.issubset(
            articles.columns
        )
    )

    # Date range for partition pruning (BigQuery join path only)
    dates = pd.to_datetime(articles["seendate"], utc=True, errors="coerce").dropna()
    start_date = (dates.min() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date   = (dates.max() + timedelta(days=2)).strftime("%Y-%m-%d")
    print(f"  → Date range: {start_date} to {end_date}")

    if dry_run:
        if prefetch_ok:
            print(
                "\n[DRY RUN] Would skip BigQuery join — gkg_v2_themes, "
                "gkg_v2_locations, gkg_v2_tone present on input CSV."
            )
        else:
            print("\n[DRY RUN] Showing query only — not executing.")
            dummy_table = (
                url_table_reuse.strip()
                if url_table_reuse
                else f"{project}.gdelt_hwc_temp.article_urls"
            )
            print(build_query(dummy_table, start_date, end_date))
        return

    gb_billed = 0.0

    if prefetch_ok:
        print(
            "\nUsing prefetched gkg_v2_* columns from input CSV — skipping BigQuery join."
            "\n  (Use --force-bigquery to join gkg_partitioned instead.)"
        )
        print("\nParsing locations and themes from prefetched columns...")
        gkg_parsed = parse_prefetched_gkg_rows(
            articles,
            primary_themes,
            secondary_themes,
            country_codes,
            subnational_types,
        )
    else:
        # ── BigQuery client ────────────────────────────────────────────────
        print(f"\nConnecting to BigQuery project: {project}")
        client = bigquery.Client(project=project)

        # ── Upload URL list (or reuse) ───────────────────────────────────────
        if url_table_reuse:
            url_table = url_table_reuse.strip()
            print(f"\nUsing existing URL table (skipping upload): {url_table}")
        else:
            print("\nUploading URL list as temp table...")
            url_table = upload_urls_as_temp_table(client, project, articles)

        # ── Run join query ───────────────────────────────────────────────────
        query = build_query(url_table, start_date, end_date)
        print(f"\nRunning BigQuery join ({start_date} → {end_date})...")
        print("  (BigQuery will tell you bytes billed after the job completes)")

        job = client.query(query)
        gkg_df = job.result().to_dataframe()
        bytes_billed = job.total_bytes_billed or 0
        gb_billed = bytes_billed / 1e9
        print(f"  ✓ {len(gkg_df)} GKG rows returned")
        print(f"  ✓ Bytes billed: {gb_billed:.2f} GB  "
              f"({'FREE' if gb_billed < 1000 else 'OVER FREE TIER — check GCP console'})")

        if gkg_df.empty:
            print("\n⚠ No matches found. Possible reasons:")
            print("  - Articles pre-date GDELT v2 (before Feb 2015)")
            print("  - URLs differ slightly (http vs https, trailing slash)")
            print("  - Articles were behind paywalls and GDELT didn't ingest them")
            articles.to_csv(out_unmatched, index=False)
            return

        # ── Deduplicate GKG rows (same URL can appear in multiple 15-min files) ──
        gkg_df = gkg_df.drop_duplicates(subset="DocumentIdentifier", keep="first")
        print(f"  → After dedup: {len(gkg_df)} unique URLs matched")

        # ── Parse GKG fields ────────────────────────────────────────────────
        print("\nParsing locations and themes...")
        rows = []
        for _, row in tqdm(gkg_df.iterrows(), total=len(gkg_df), unit="article"):
            locs   = parse_v2locations(row.get("V2Locations", ""))
            themes = parse_themes(row.get("V2Themes", ""))
            score, matched = score_themes(themes, primary_themes, secondary_themes)
            best   = best_india_location(locs, country_codes, subnational_types)
            india  = india_locations(locs, country_codes)

            rows.append({
                "DocumentIdentifier": row["DocumentIdentifier"],
                "gkg_found":          True,
                "source_name":        row.get("source_name", ""),
                "v2tone":             parse_tone(row.get("V2Tone", "")),
                "theme_score":        score,
                "matched_themes":     "; ".join(matched),
                "v2themes_raw":       str(row.get("V2Themes", ""))[:400],
                "n_india_locations":  len(india),
                "best_lat":           best["lat"]      if best else None,
                "best_lon":           best["lon"]      if best else None,
                "best_location_name": best["loc_name"] if best else None,
                "best_adm1":          best["adm1"]     if best else None,
                "all_india_locations": "; ".join(
                    f"{l['loc_name']} ({l['lat']:.3f},{l['lon']:.3f})"
                    for l in india
                ) if india else "",
                "v2persons":          str(row.get("V2Persons", ""))[:300],
            })

        gkg_parsed = pd.DataFrame(rows).rename(
            columns={"DocumentIdentifier": "url"}
        )

    # ── Merge back onto original articles ──────────────────────────────────
    merge_left = articles
    if prefetch_ok:
        merge_left = articles.drop(
            columns=[
                c
                for c in (
                    "gkg_v2_themes",
                    "gkg_v2_locations",
                    "gkg_v2_tone",
                    "gkg_v2_persons",
                )
                if c in articles.columns
            ],
            errors="ignore",
        )
    enriched = merge_left.merge(gkg_parsed, on="url", how="left")
    enriched["gkg_found"] = enriched["gkg_found"].fillna(False)

    # ── Save outputs ───────────────────────────────────────────────────────
    enriched.to_csv(out_enriched, index=False)

    geocoded = enriched[enriched["n_india_locations"].notna() &
                        (enriched["n_india_locations"] > 0)]
    geocoded.to_csv(out_geocoded, index=False)

    high_conf = enriched[enriched["theme_score"].notna() &
                         (enriched["theme_score"] >= hc_min)]
    high_conf.to_csv(out_hc, index=False)

    unmatched = enriched[enriched["gkg_found"] == False]
    unmatched.to_csv(out_unmatched, index=False)

    # ── Summary ────────────────────────────────────────────────────────────
    total    = len(enriched)
    n_found  = enriched["gkg_found"].sum()
    n_geo    = len(geocoded)
    n_hc     = len(high_conf)
    n_miss   = len(unmatched)

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║               BigQuery GKG Enrichment Summary                ║
╠══════════════════════════════════════════════════════════════╣
║  Input articles                        : {total:>5}              ║
║  Matched in GKG                        : {int(n_found):>5}              ║
║    → With ≥1 India lat/lon             : {n_geo:>5}              ║
║    → High-confidence (primary+secondary): {n_hc:>5}              ║
║  Unmatched (paywall/404/pre-GDELT)     : {n_miss:>5}              ║
║  BigQuery billed                       : {gb_billed:>5.1f} GB           ║
╚══════════════════════════════════════════════════════════════╝

Theme score guide:
  0 = no relevant theme tags
  1 = primary theme only   (domain topic, e.g. ENV_WILDLIFE, ENV_POACHING…)
  2 = secondary theme only (harm/incident, e.g. KILL, AFFECT, WOUND…)
  3 = BOTH → high-confidence article ✓

Output files:
  {out_enriched:<45} ← all articles + GKG fields
  {out_geocoded:<45} ← has lat/lon
  {out_hc:<45} ← best HWC candidates
  {out_unmatched:<45} ← no GKG match (fetch text + Claude next)

Next steps:
  1. Map hwc_urls_geocoded.csv directly (best_lat / best_lon columns)
  2. For high-confidence matches, pass title + all_india_locations
     to Claude to pick the PRIMARY event location
  3. For unmatched articles, fetch text with trafilatura and run
     Claude extraction to get (event, species, location, date)
""")


if __name__ == "__main__":
    load_repo_env()
    _root = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(
        description="Enrich GDELT article URLs with GKG data via BigQuery"
    )
    p.add_argument(
        "--project",
        default=(os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip() or None,
        help=(
            "GCP project ID (e.g. my-gcp-project-123). "
            "Default: GOOGLE_CLOUD_PROJECT from environment or repo root .env"
        ),
    )
    p.add_argument(
        "--meta",
        default=str(meta_path_default(_root)),
        help="Domain meta (gkg_theme_sets, gkg_geography)",
    )
    p.add_argument("--input",     default=None,
                   help="Input CSV from gdelt-fetch-urls.py (default from --meta prefix)")
    p.add_argument("--enriched",  default=None)
    p.add_argument("--geocoded",  default=None)
    p.add_argument("--high-conf", default=None)
    p.add_argument("--unmatched", default=None)
    p.add_argument("--dry-run",   action="store_true",
                   help="Print the SQL query without executing it")
    p.add_argument(
        "--force-bigquery",
        action="store_true",
        help=(
            "Always join gkg_partitioned in BigQuery, even if gkg_v2_* columns exist "
            "on the input CSV (from gdelt-fetch-urls.py --source bigquery)."
        ),
    )
    p.add_argument(
        "--url-table",
        default=None,
        metavar="PROJECT.DATASET.TABLE",
        help=(
            "Skip upload; join against this table (columns: DocumentIdentifier STRING, "
            "article_date DATE). Default upload targets PROJECT.gdelt_hwc_temp.article_urls "
            "with WRITE_TRUNCATE — same table each run, rows replaced, no extra tables."
        ),
    )
    p.add_argument(
        "--dedupe-mode",
        default=None,
        metavar="MODE",
        help=(
            "Deduplicate after load, before BigQuery/prefetch: "
            "path (same normalized URL path across hosts), "
            "jaccard (similar URL path tokens + date window), "
            "or path,jaccard (both in order). Omit for no dedupe; "
            "meta gdelt_enrich_url_dedupe.modes applies when this flag is omitted."
        ),
    )
    p.add_argument(
        "--dedupe-report",
        default=None,
        metavar="CSV",
        help=(
            "Write dropped rows (columns kept_url, dedupe_stage path|jaccard). "
            "Requires a non-empty dedupe mode from --dedupe-mode or meta."
        ),
    )
    args = p.parse_args()
    if not args.project:
        p.error(
            "GCP project required: pass --project or set GOOGLE_CLOUD_PROJECT "
            "(e.g. in repo root .env — see .env.example) or export it in your shell."
        )
    _pfx = output_prefix(args.meta)
    if args.input is None:
        args.input = str(urls_csv(_root, _pfx))
    if args.enriched is None:
        args.enriched = str(urls_enriched_csv(_root, _pfx))
    if args.geocoded is None:
        args.geocoded = str(urls_geocoded_csv(_root, _pfx))
    if args.high_conf is None:
        args.high_conf = str(urls_high_confidence_csv(_root, _pfx))
    if args.unmatched is None:
        args.unmatched = str(urls_unmatched_csv(_root, _pfx))

    main(
        project     = args.project,
        input_csv   = args.input,
        out_enriched= args.enriched,
        out_geocoded= args.geocoded,
        out_hc      = args.high_conf,
        out_unmatched=args.unmatched,
        dry_run     = args.dry_run,
        meta_path   = args.meta,
        url_table_reuse=args.url_table,
        force_bigquery=args.force_bigquery,
        dedupe_mode=args.dedupe_mode,
        dedupe_report=args.dedupe_report,
    )