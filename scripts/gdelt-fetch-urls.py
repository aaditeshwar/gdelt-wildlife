"""
GDELT Feasibility Check — India Human-Wildlife Conflict Articles
================================================================
Queries the GDELT DOC 2.0 API across multiple HWC keywords,
sweeping the past 5 years in 3-month windows (the max reliable range
per window for the DOC API). Deduplicates results and saves a CSV.

Requirements:
    pip install gdeltdoc pandas

Usage:
    python scripts/gdelt-fetch-urls.py

Output (under data/):
    hwc_urls.csv          — deduplicated article metadata
    hwc_urls_summary.txt  — coverage stats and notes

Domain keywords, country, and query tuning come from the meta JSON (gdelt_doc_fetch),
default: meta/hwc_india_conflict_meta.json
"""

import argparse
import datetime
import sys
import time
import uuid
from pathlib import Path

import pandas as pd
from gdeltdoc import GdeltDoc, Filters

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from domain_meta import get_gdelt_doc_fetch, load_domain_meta  # noqa: E402
from domain_paths import meta_path_default, output_prefix, urls_csv, urls_summary_txt  # noqa: E402

_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_META = meta_path_default(_ROOT)

# ── Helpers ───────────────────────────────────────────────────────────────────

def date_windows(years_back: int, window_months: int):
    """Generate (start, end) date string pairs going back `years_back` years."""
    end = datetime.date.today()
    windows = []
    while True:
        # Step back one window
        month = end.month - window_months
        year = end.year
        while month <= 0:
            month += 12
            year -= 1
        start = end.replace(year=year, month=month)
        if start < datetime.date.today() - datetime.timedelta(days=365 * years_back):
            start = datetime.date.today() - datetime.timedelta(days=365 * years_back)
            windows.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
            break
        windows.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
        end = start - datetime.timedelta(days=1)
    return windows


def query_gdelt(
    keyword: str,
    start: str,
    end: str,
    gd: GdeltDoc,
    *,
    country: str,
    language: str,
    max_records: int,
) -> pd.DataFrame:
    """Run a single GDELT article search. Returns empty DataFrame on failure."""
    try:
        f = Filters(
            keyword=keyword,
            start_date=start,
            end_date=end,
            num_records=max_records,
            country=country,
            language=language,
        )
        df = gd.article_search(f)
        if df is not None and not df.empty:
            df["query_keyword"] = keyword
            df["query_start"] = start
            df["query_end"] = end
            return df
        return pd.DataFrame()
    except Exception as e:
        print(f"    ⚠ Error querying '{keyword}' [{start}→{end}]: {e}")
        return pd.DataFrame()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Fetch GDELT DOC URLs using domain meta")
    ap.add_argument(
        "--meta",
        default=str(_DEFAULT_META),
        help="Domain meta JSON (must define gdelt_doc_fetch)",
    )
    args = ap.parse_args()

    meta = load_domain_meta(args.meta)
    prefix = output_prefix(args.meta)
    output_csv = urls_csv(_ROOT, prefix)
    output_summary = urls_summary_txt(_ROOT, prefix)
    cfg = get_gdelt_doc_fetch(meta)
    keywords = list(cfg["keywords"])
    country = str(cfg["country"])
    language = str(cfg["language"])
    max_records = int(cfg.get("max_records", 250))
    sleep_q = float(cfg.get("sleep_between_queries_seconds", 3))
    years_back = int(cfg.get("years_back", 5))
    window_months = int(cfg.get("window_months", 3))
    banner = str(cfg.get("summary_banner", "GDELT DOC fetch"))
    summary_title = str(cfg.get("summary_title", "GDELT summary"))

    gd = GdeltDoc()
    windows = date_windows(years_back, window_months)

    print(banner)
    print(f"{'='*60}")
    print(f"Keywords : {len(keywords)}")
    print(f"Windows  : {len(windows)} × {window_months}-month chunks over {years_back} years")
    print(f"Total    : up to {len(keywords) * len(windows)} API calls")
    print(f"NOTE: Only the most recent ~3 months are guaranteed by GDELT DOC API.")
    print(f"      Older windows will be attempted but may return sparse results.\n")

    all_frames = []
    window_stats = []   # (start, end, n_articles_raw, n_keywords_with_results)

    for i, (start, end) in enumerate(windows):
        print(f"\n[Window {i+1}/{len(windows)}]  {start} → {end}")
        window_dfs = []
        keywords_with_results = 0

        for kw in keywords:
            print(f"  → querying: '{kw}' ... ", end="", flush=True)
            df = query_gdelt(
                kw,
                start,
                end,
                gd,
                country=country,
                language=language,
                max_records=max_records,
            )
            n = len(df)
            print(f"{n} articles")
            if n > 0:
                window_dfs.append(df)
                keywords_with_results += 1
            time.sleep(sleep_q)

        if window_dfs:
            window_df = pd.concat(window_dfs, ignore_index=True)
        else:
            window_df = pd.DataFrame()

        raw_count = len(window_df)
        window_stats.append({
            "window_start": start,
            "window_end": end,
            "raw_articles": raw_count,
            "keywords_with_results": keywords_with_results,
        })
        if not window_df.empty:
            all_frames.append(window_df)

    # ── Combine & deduplicate ─────────────────────────────────────────────────
    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        total_raw = len(combined)

        # Deduplicate on URL; keep first occurrence (earliest keyword match)
        combined_dedup = combined.drop_duplicates(subset="url", keep="first")
        total_dedup = len(combined_dedup)

        # Sort by date descending
        combined_dedup = combined_dedup.sort_values("seendate", ascending=False)

        # Stable row IDs for the rest of the pipeline
        combined_dedup.insert(0, "event_id", [str(uuid.uuid4()) for _ in range(len(combined_dedup))])

        # Save CSV
        cols = ["event_id", "url", "title", "seendate", "domain", "language",
                "sourcecountry", "query_keyword", "query_start", "query_end",
                "url_mobile", "socialimage"]
        cols = [c for c in cols if c in combined_dedup.columns]
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        combined_dedup[cols].to_csv(output_csv, index=False)
        print(f"\n✓ Saved {total_dedup} deduplicated articles → {output_csv}")
    else:
        total_raw = 0
        total_dedup = 0
        combined_dedup = pd.DataFrame()
        print("\n⚠ No articles found across all queries.")

    # ── Summary report ────────────────────────────────────────────────────────
    stats_df = pd.DataFrame(window_stats)

    summary_lines = [
        summary_title,
        "=" * 60,
        f"Date range attempted : {windows[-1][0]} to {windows[0][1]}",
        f"Keywords searched    : {len(keywords)}",
        f"Time windows         : {len(windows)}",
        f"Raw articles (with duplicates): {total_raw}",
        f"Deduplicated articles         : {total_dedup}",
        "",
        "Results by window:",
        stats_df.to_string(index=False),
        "",
        "Top domains (if any results):",
    ]

    if not combined_dedup.empty and "domain" in combined_dedup.columns:
        top_domains = combined_dedup["domain"].value_counts().head(20)
        summary_lines.append(top_domains.to_string())
        summary_lines += [
            "",
            "Articles per keyword (before dedup):",
        ]
        if "query_keyword" in combined_dedup.columns:
            kw_counts = combined_dedup["query_keyword"].value_counts()
            summary_lines.append(kw_counts.to_string())

    summary_lines += [
        "",
        "⚠  IMPORTANT NOTES:",
        "  1. GDELT DOC API reliably covers only the last ~3 months.",
        "     Older windows above are best-effort and may be sparse.",
        "  2. For a complete 5-year dataset, use Google BigQuery:",
        "     SELECT * FROM `gdelt-bq.gdeltv2.gkg`",
        "     WHERE DATE(DATE) BETWEEN '2021-01-01' AND '2026-03-23'",
        "     AND V2Themes LIKE '%WILDLIFE%'",
        "     AND Locations LIKE '%India%'",
        "  3. Each keyword query is capped at 250 articles by the API.",
        "     High-volume topics may be undersampled — use BigQuery for completeness.",
        "  4. Next step: fetch article text with trafilatura/newspaper3k,",
        "     then pass to Claude API for (event, geolocation) extraction.",
    ]

    summary_text = "\n".join(summary_lines)
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    with open(output_summary, "w") as f:
        f.write(summary_text)

    print("\n" + summary_text)
    print(f"\n✓ Summary saved → {output_summary}")


if __name__ == "__main__":
    main()