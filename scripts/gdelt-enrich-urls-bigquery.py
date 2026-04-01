"""
GDELT BigQuery Enrichment — Geocoding + Themes via URL join
============================================================
Takes your data/hwc_urls.csv, uploads the URLs + dates to a
BigQuery temp table, joins against gdelt-bq.gdeltv2.gkg_partitioned
(partitioned by day → cheap), and returns GKG fields for each article.

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
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

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
        g.V2Tone,
        g.SharingImage
    FROM
        `gdelt-bq.gdeltv2.gkg_partitioned` AS g
    INNER JOIN
        `{url_table}` AS u
        ON g.DocumentIdentifier = u.DocumentIdentifier
    WHERE
        g._PARTITIONTIME >= TIMESTAMP("{start_date}")
        AND g._PARTITIONTIME <  TIMESTAMP("{end_date}")
    """


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

    # Date range for partition pruning
    dates = pd.to_datetime(articles["seendate"], utc=True, errors="coerce").dropna()
    start_date = (dates.min() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date   = (dates.max() + timedelta(days=2)).strftime("%Y-%m-%d")
    print(f"  → Date range: {start_date} to {end_date}")

    if dry_run:
        print("\n[DRY RUN] Showing query only — not executing.")
        dummy_table = (
            url_table_reuse.strip()
            if url_table_reuse
            else f"{project}.gdelt_hwc_temp.article_urls"
        )
        print(build_query(dummy_table, start_date, end_date))
        return

    # ── BigQuery client ────────────────────────────────────────────────────
    print(f"\nConnecting to BigQuery project: {project}")
    client = bigquery.Client(project=project)

    # ── Upload URL list (or reuse) ─────────────────────────────────────────
    if url_table_reuse:
        url_table = url_table_reuse.strip()
        print(f"\nUsing existing URL table (skipping upload): {url_table}")
    else:
        print("\nUploading URL list as temp table...")
        url_table = upload_urls_as_temp_table(client, project, articles)

    # ── Run join query ─────────────────────────────────────────────────────
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

    # ── Parse GKG fields ───────────────────────────────────────────────────
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
            "sharing_image":      row.get("SharingImage", ""),
        })

    gkg_parsed = pd.DataFrame(rows).rename(
        columns={"DocumentIdentifier": "url"}
    )

    # ── Merge back onto original articles ──────────────────────────────────
    enriched = articles.merge(gkg_parsed, on="url", how="left")
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
        "--url-table",
        default=None,
        metavar="PROJECT.DATASET.TABLE",
        help=(
            "Skip upload; join against this table (columns: DocumentIdentifier STRING, "
            "article_date DATE). Default upload targets PROJECT.gdelt_hwc_temp.article_urls "
            "with WRITE_TRUNCATE — same table each run, rows replaced, no extra tables."
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
    )