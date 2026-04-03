"""
GDELT GKG Enrichment — Geocoding + Theme Filtering
====================================================
Takes your existing data/hwc_urls.csv (from gdelt-fetch-urls) and enriches each article
with pre-computed GDELT GKG data:
  - V2Locations  → all geocoded place mentions (lat/lon, ADM1, country)
  - V2Themes     → topic tags (e.g. ENV_WILDLIFE, KILL, AFFECT)
  - V2Persons    → named persons mentioned
  - V2Tone       → sentiment score

NO article download required. All geocoding is done by GDELT already.

Strategy
--------
GDELT publishes raw GKG files every 15 minutes at:
  http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.gkg.csv.zip

The masterfilelist.txt contains every file URL ever published.
We:
  1. Parse the dates from your URL list
  2. Find the GKG file(s) covering each article's date
  3. Download those GKG files (small ~2-5MB each, zipped)
  4. Match rows by DocumentIdentifier (= article URL)
  5. Parse V2Locations into structured lat/lon records
  6. Filter & score articles by primary/secondary GKG theme sets
  7. Output enriched CSV + a "high confidence" subset

Requirements:
    pip install pandas requests tqdm

Usage:
    python scripts/gdelt-enrich-urls.py
    python scripts/gdelt-enrich-urls.py --input data/hwc_urls.csv --max-files 100

Output (under data/ by default):
    hwc_urls_enriched.csv        — all articles with GKG fields added
    hwc_urls_geocoded.csv        — subset with ≥1 India location extracted
    hwc_urls_high_confidence.csv — subset matching primary + secondary theme score
"""

import argparse
import io
import os
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from domain_meta import (  # noqa: E402
    get_gkg_geography,
    get_gkg_theme_sets,
    load_domain_meta,
)
from domain_paths import (  # noqa: E402
    ensure_event_id_column,
    meta_path_default,
    output_prefix,
    urls_csv,
    urls_enriched_csv,
    urls_geocoded_csv,
    urls_high_confidence_csv,
)

# ── Config ────────────────────────────────────────────────────────────────────

MASTERFILE_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
GKG_BASE_URL   = "http://data.gdeltproject.org/gdeltv2/"
CACHE_DIR      = Path("gkg_cache")   # downloaded GKG files cached here

# ── Helpers ───────────────────────────────────────────────────────────────────

GKG_COLUMNS = [
    "GKGRECORDID", "DATE", "SourceCollectionIdentifier", "SourceCommonName",
    "DocumentIdentifier", "Counts", "V2Counts", "Themes", "V2Themes",
    "Locations", "V2Locations", "Persons", "V2Persons", "Organizations",
    "V2Organizations", "V2Tone", "Dates", "GCAM",
    "RelatedImages", "SocialImageEmbeds", "SocialVideoEmbeds",
    "Quotations", "AllNames", "Amounts", "TranslationInfo", "Extras"
]


def _gkg_cell_str(val: object, max_len: int | None = None) -> str:
    """Normalize a GKG dataframe cell to str; None/NaN become \"\". Optional max length."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(val)
    if max_len is not None and len(s) > max_len:
        return s[:max_len]
    return s


def parse_v2locations(loc_string: str) -> list[dict]:
    """
    Parse GDELT V2Locations field into a list of dicts.
    Format: TYPE#NAME#COUNTRYCODE#ADM1CODE#LAT#LON#FEATUREID;...
    """
    if not loc_string or pd.isna(loc_string):
        return []
    locations = []
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
        locations.append({
            "loc_type":    loc_type,
            "loc_name":    parts[1] if len(parts) > 1 else "",
            "country":     parts[2] if len(parts) > 2 else "",
            "adm1":        parts[3] if len(parts) > 3 else "",
            "lat":         lat,
            "lon":         lon,
            "feature_id":  parts[6] if len(parts) > 6 else "",
        })
    return locations


def india_locations(
    loc_list: list[dict],
    country_codes: tuple[str, ...],
) -> list[dict]:
    """Filter location list to entries matching configured country codes with valid coordinates."""
    return [
        loc for loc in loc_list
        if loc.get("country") in country_codes
        and loc.get("lat") is not None
        and loc.get("lon") is not None
    ]


def best_india_location(
    loc_list: list[dict],
    country_codes: tuple[str, ...],
    subnational_types: set[int],
) -> dict | None:
    """
    Return the most specific location available for the configured region.
    Prefer subnational (city/state) over country-level.
    """
    india_locs = india_locations(loc_list, country_codes)
    if not india_locs:
        return None
    subnational = [l for l in india_locs if l["loc_type"] in subnational_types]
    return subnational[0] if subnational else india_locs[0]


def parse_themes(theme_string: str) -> set[str]:
    """Parse semicolon-delimited theme string into a set."""
    if not theme_string or pd.isna(theme_string):
        return set()
    # V2Themes format: THEME,charoffset;THEME,charoffset;...
    themes = set()
    for item in str(theme_string).split(";"):
        theme = item.split(",")[0].strip()
        if theme:
            themes.add(theme)
    return themes


def score_article(
    themes: set[str],
    primary_themes: set[str],
    secondary_themes: set[str],
) -> tuple[int, list[str]]:
    """
    Return (score, matched_themes).
    score: 0=no match, 1=primary only, 2=secondary only, 3=both
    """
    primary_hits = themes & primary_themes
    secondary_hits = themes & secondary_themes
    score = (1 if primary_hits else 0) + (2 if secondary_hits else 0)
    return score, sorted(primary_hits | secondary_hits)


def parse_tone(tone_string: str) -> float | None:
    """Extract overall tone score (first field)."""
    if not tone_string or pd.isna(tone_string):
        return None
    try:
        return float(str(tone_string).split(",")[0])
    except ValueError:
        return None


def article_date_to_gkg_timestamps(date_str: str) -> list[str]:
    """
    Given an article date (various formats), return a list of GKG 15-min
    timestamps that might contain this article (covers the full UTC day).
    """
    try:
        dt = pd.to_datetime(date_str, utc=True).replace(tzinfo=None)
    except Exception:
        return []
    timestamps = []
    cursor = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = cursor + timedelta(days=1)
    while cursor < end_of_day:
        timestamps.append(cursor.strftime("%Y%m%d%H%M%S"))
        cursor += timedelta(minutes=15)
    return timestamps


# ── Master file index ─────────────────────────────────────────────────────────

def build_gkg_index() -> dict[str, str]:
    """
    Download GDELT masterfilelist.txt and build a dict:
      timestamp_str → full GKG zip URL
    Only includes GKG files (not events/mentions).
    """
    print("Downloading GDELT master file list (this may take ~30s)...")
    resp = requests.get(MASTERFILE_URL, timeout=60)
    resp.raise_for_status()
    index = {}
    for line in resp.text.splitlines():
        parts = line.strip().split()
        if len(parts) < 3:
            continue
        url = parts[2]
        if ".gkg.csv.zip" in url and "translation" not in url:
            # Extract timestamp from filename
            fname = url.split("/")[-1]
            ts = fname.replace(".gkg.csv.zip", "")
            index[ts] = url
    print(f"  → Indexed {len(index):,} GKG files")
    return index


# ── GKG file download + parse ─────────────────────────────────────────────────

def fetch_gkg_file(url: str, cache_dir: Path) -> pd.DataFrame | None:
    """Download a GKG zip file, parse it, return DataFrame. Uses local cache."""
    fname = url.split("/")[-1]
    cache_path = cache_dir / fname.replace(".zip", "")

    if cache_path.exists():
        try:
            return pd.read_csv(cache_path, sep="\t", header=None,
                               names=GKG_COLUMNS, dtype=str, on_bad_lines="skip")
        except Exception:
            cache_path.unlink(missing_ok=True)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                content = f.read()
        # Cache unzipped
        cache_path.write_bytes(content)
        return pd.read_csv(io.BytesIO(content), sep="\t", header=None,
                           names=GKG_COLUMNS, dtype=str, on_bad_lines="skip")
    except Exception as e:
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main(
    input_csv: str,
    output_enriched: str,
    output_geocoded: str,
    output_hc: str,
    max_files: int,
    sleep: float,
    meta_path: str,
):

    meta = load_domain_meta(meta_path)
    primary_themes, secondary_themes, hc_min = get_gkg_theme_sets(meta)
    country_codes, subnational_types = get_gkg_geography(meta)

    CACHE_DIR.mkdir(exist_ok=True)

    # Load your article list
    print(f"\nLoading articles from: {input_csv}")
    articles = pd.read_csv(input_csv, dtype=str)
    articles = ensure_event_id_column(articles)
    print(f"  → {len(articles)} articles loaded")

    required = {"url", "seendate"}
    missing = required - set(articles.columns)
    if missing:
        raise ValueError(f"Input CSV missing columns: {missing}. "
                         f"Expected output of scripts/gdelt-fetch-urls.py")

    # Build mapping: url → row index
    url_to_idx = {row["url"]: i for i, row in articles.iterrows()
                  if pd.notna(row.get("url"))}

    # Build GKG master index
    gkg_index = build_gkg_index()

    # For each article date, find which GKG timestamps to search
    # Group articles by the GKG timestamps they might appear in
    ts_to_urls: dict[str, list[str]] = defaultdict(list)
    for url, idx in url_to_idx.items():
        date_str = articles.at[idx, "seendate"]
        for ts in article_date_to_gkg_timestamps(date_str):
            if ts in gkg_index:
                ts_to_urls[ts].append(url)

    # Sort timestamps, limit if requested
    all_timestamps = sorted(ts_to_urls.keys(), reverse=True)
    if max_files > 0:
        all_timestamps = all_timestamps[:max_files]
        print(f"  → Processing {len(all_timestamps)} GKG files (capped at --max-files {max_files})")
    else:
        print(f"  → Processing up to {len(all_timestamps)} GKG files")

    # Prepare result columns
    new_cols = ["gkg_found", "v2themes", "v2locations_raw", "v2persons",
                "v2tone", "theme_score", "matched_themes",
                "n_india_locations", "best_lat", "best_lon",
                "best_location_name", "best_adm1", "all_india_locations"]
    for col in new_cols:
        articles[col] = None

    found_count = 0
    files_processed = 0

    print(f"\nSearching GKG files for your {len(url_to_idx)} article URLs...")
    pbar = tqdm(all_timestamps, unit="file")

    for ts in pbar:
        # Only fetch if any of this file's candidate URLs are still unmatched
        candidate_urls = [u for u in ts_to_urls[ts]
                          if articles.loc[url_to_idx[u], "gkg_found"] != "yes"]
        if not candidate_urls:
            continue

        gkg_url = gkg_index[ts]
        df = fetch_gkg_file(gkg_url, CACHE_DIR)
        files_processed += 1

        if df is None or df.empty:
            time.sleep(sleep)
            continue

        # Match rows where DocumentIdentifier is in our URL list
        matched = df[df["DocumentIdentifier"].isin(candidate_urls)]

        for _, row in matched.iterrows():
            url = row["DocumentIdentifier"]
            if url not in url_to_idx:
                continue
            idx = url_to_idx[url]

            # Parse locations (cells may be float NaN in pandas even when column exists)
            locs = parse_v2locations(_gkg_cell_str(row.get("V2Locations")))
            india_locs = india_locations(locs, country_codes)
            best = best_india_location(locs, country_codes, subnational_types)

            # Parse themes
            themes = parse_themes(_gkg_cell_str(row.get("V2Themes")))
            score, matched_themes = score_article(themes, primary_themes, secondary_themes)

            # Parse tone
            tone = parse_tone(_gkg_cell_str(row.get("V2Tone")))

            # Write back
            articles.at[idx, "gkg_found"]           = "yes"
            articles.at[idx, "v2themes"]             = "; ".join(sorted(themes)[:30])
            articles.at[idx, "v2locations_raw"]      = _gkg_cell_str(row.get("V2Locations"), 500)
            articles.at[idx, "v2persons"]            = _gkg_cell_str(row.get("V2Persons"), 300)
            articles.at[idx, "v2tone"]               = tone
            articles.at[idx, "theme_score"]          = score
            articles.at[idx, "matched_themes"]       = "; ".join(matched_themes)
            articles.at[idx, "n_india_locations"]    = len(india_locs)
            articles.at[idx, "best_lat"]             = best["lat"] if best else None
            articles.at[idx, "best_lon"]             = best["lon"] if best else None
            articles.at[idx, "best_location_name"]   = best["loc_name"] if best else None
            articles.at[idx, "best_adm1"]            = best["adm1"] if best else None
            articles.at[idx, "all_india_locations"]  = str([
                f"{l['loc_name']}({l['lat']:.3f},{l['lon']:.3f})"
                for l in india_locs
            ]) if india_locs else ""

            found_count += 1

        pbar.set_postfix({"matched": found_count, "files": files_processed})
        time.sleep(sleep)

    # ── Save outputs ──────────────────────────────────────────────────────────
    articles.to_csv(output_enriched, index=False)
    print(f"\n✓ Enriched CSV saved → {output_enriched}  ({len(articles)} rows)")

    geocoded = articles[articles["n_india_locations"].notna() &
                        (articles["n_india_locations"].astype(float) > 0)]
    geocoded.to_csv(output_geocoded, index=False)
    print(f"✓ Geocoded subset   → {output_geocoded}  ({len(geocoded)} rows)")

    high_conf = articles[articles["theme_score"].notna() &
                         (articles["theme_score"].astype(float) >= hc_min)]
    high_conf.to_csv(output_hc, index=False)
    print(f"✓ High-confidence   → {output_hc}  ({len(high_conf)} rows)")

    # ── Summary ───────────────────────────────────────────────────────────────
    total = len(articles)
    n_found   = (articles["gkg_found"] == "yes").sum()
    n_geo     = len(geocoded)
    n_hc      = len(high_conf)
    n_no_loc  = n_found - n_geo

    print(f"""
╔══════════════════════════════════════════════════════════╗
║              GKG Enrichment Summary                      ║
╠══════════════════════════════════════════════════════════╣
║  Total articles in input          : {total:>6}             ║
║  Matched in GKG files             : {n_found:>6}             ║
║  → With ≥1 India location (lat/lon): {n_geo:>6}             ║
║  → No India location found        : {n_no_loc:>6}             ║
║  High-confidence (primary+secondary): {n_hc:>6}            ║
║  GKG files downloaded/scanned     : {files_processed:>6}             ║
╚══════════════════════════════════════════════════════════╝

Theme score guide:
  0 = No primary or secondary theme match
  1 = Primary theme only (domain topic, e.g. ENV_WILDLIFE, ENV_POACHING)
  2 = Secondary theme only (harm/incident, e.g. KILL, AFFECT, WOUND)
  3 = Both primary + secondary themes → HIGH CONFIDENCE HWC article

Next steps:
  1. Review hwc_urls_high_confidence.csv — these are your best candidates
  2. For articles with best_lat/best_lon: ready to map directly
  3. For geocoded but ambiguous: pass title + all_india_locations to Claude
     to pick the primary event location
  4. For unmatched articles (gkg_found != 'yes'): these fell outside the
     GKG file window — use BigQuery or fetch article text + geocode with Claude
""")


if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Enrich GDELT article list with GKG geocoding + themes")
    parser.add_argument("--input",     default=None,
                        help="Input CSV from gdelt-fetch-urls.py (default: {prefix}_urls.csv from --meta)")
    parser.add_argument("--enriched",  default=None)
    parser.add_argument("--geocoded",  default=None)
    parser.add_argument("--high-conf", default=None)
    parser.add_argument(
        "--meta",
        default=str(meta_path_default(_root)),
        help="Domain meta (gkg_theme_sets, gkg_geography)",
    )
    parser.add_argument("--max-files", type=int, default=200,
                        help="Max GKG files to download (0=unlimited). Each covers 15 minutes.")
    parser.add_argument("--sleep",     type=float, default=0.5,
                        help="Seconds to sleep between file downloads")
    args = parser.parse_args()
    _pfx = output_prefix(args.meta)
    if args.input is None:
        args.input = str(urls_csv(_root, _pfx))
    if args.enriched is None:
        args.enriched = str(urls_enriched_csv(_root, _pfx))
    if args.geocoded is None:
        args.geocoded = str(urls_geocoded_csv(_root, _pfx))
    if args.high_conf is None:
        args.high_conf = str(urls_high_confidence_csv(_root, _pfx))

    main(
        input_csv=args.input,
        output_enriched=args.enriched,
        output_geocoded=args.geocoded,
        output_hc=args.high_conf,
        max_files=args.max_files,
        sleep=args.sleep,
        meta_path=args.meta,
    )