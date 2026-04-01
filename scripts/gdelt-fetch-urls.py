"""
GDELT URL discovery — DOC API or BigQuery GKG
==============================================
**Recommended pipeline (keyword-driven articles):** use the default
``--source doc`` so ``gdelt_doc_fetch.keywords`` are sent to the GDELT DOC API.
Then run ``gdelt-enrich-urls-bigquery.py`` (or local enrich) on that CSV — those
scripts **enrich** URLs with GKG fields; they are not substitutes for keyword
discovery.

**``--source bigquery``** queries ``gdelt-bq.gdeltv2.gkg_partitioned``. Default:
``gkg_theme_sets`` + ``gkg_geography`` (V2Themes + V2Locations LIKE). Optional
``bigquery_gkg_fetch.mode: "url_keywords"`` uses **URL slug** ``DocumentIdentifier LIKE``
patterns (plus India location and optional domain exclusions), closer to keyword
intent when the DOC API is rate-limited. It still does **not** use
``gdelt_doc_fetch.keywords`` as SQL fragments — configure patterns under
``bigquery_gkg_fetch.url_keyword_patterns``.

Default: DOC API across keywords and date windows,
deduplicates by URL, writes CSV + summary.

Requirements:
    pip install gdeltdoc pandas
    # for --source bigquery:
    pip install google-cloud-bigquery db-dtypes

Usage:
    python scripts/gdelt-fetch-urls.py
    python scripts/gdelt-fetch-urls.py --dry-run
    python scripts/gdelt-fetch-urls.py --source bigquery --project YOUR_GCP_PROJECT

DOC API: rate limits and transient errors are retried with exponential backoff.
Each keyword×date-window call is appended to ``data/{prefix}_fetch_log.jsonl`` as JSON
(one line per final outcome). To re-run only queries that failed last time::

    python scripts/gdelt-fetch-urls.py --retry-failed
"""

from __future__ import annotations

import argparse
import calendar
import datetime
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from gdeltdoc import GdeltDoc, Filters

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from domain_meta import (  # noqa: E402
    get_bigquery_gkg_fetch_spec,
    get_gdelt_doc_fetch,
    get_gdelt_doc_fetch_date_range,
    get_gkg_geography,
    get_gkg_theme_codes_for_bigquery_fetch,
    load_domain_meta,
)
from domain_paths import (  # noqa: E402
    load_repo_env,
    meta_path_default,
    output_prefix,
    urls_csv,
    urls_fetch_log_jsonl,
    urls_summary_txt,
)

_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_META = meta_path_default(_ROOT)

# --dry-run: cap total deduplicated articles and limit API load / BQ rows
DRY_RUN_MAX_ARTICLES = 50

# ── Helpers ───────────────────────────────────────────────────────────────────


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def append_fetch_log(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_queries_to_retry_from_log(path: Path) -> list[tuple[str, str, str]]:
    """
    Return (keyword, window_start, window_end) for queries whose *last* log line
    has status ``failed``. Later success lines for the same key override failure.
    """
    if not path.is_file():
        return []
    last: dict[tuple[str, str, str], dict[str, Any]] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            kw = rec.get("keyword")
            ws = rec.get("window_start")
            we = rec.get("window_end")
            if kw is None or ws is None or we is None:
                continue
            key = (str(kw), str(ws), str(we))
            last[key] = rec
    return [k for k, r in last.items() if r.get("status") == "failed"]


def merge_existing_urls_csv(existing_path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    """Append new rows; dedupe by ``url`` (first wins — existing rows keep ``event_id``)."""
    if new_df.empty:
        if existing_path.is_file():
            return pd.read_csv(existing_path)
        return pd.DataFrame()
    new_part = new_df.copy()
    if "event_id" not in new_part.columns:
        new_part.insert(0, "event_id", [str(uuid.uuid4()) for _ in range(len(new_part))])
    if not existing_path.is_file():
        return new_part
    existing = pd.read_csv(existing_path)
    combined = pd.concat([existing, new_part], ignore_index=True)
    return combined.drop_duplicates(subset="url", keep="first")


def _retryable_doc_error(exc: BaseException) -> bool:
    try:
        from gdeltdoc.errors import RateLimitError, ServerError

        if isinstance(exc, (RateLimitError, ServerError)):
            return True
    except ImportError:
        pass
    try:
        import requests

        if isinstance(exc, requests.exceptions.RequestException):
            return True
    except ImportError:
        pass
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None) if resp is not None else None
    if code in (429, 502, 503, 504):
        return True
    return False


def _subtract_calendar_months(d: datetime.date, months: int) -> datetime.date:
    """
    Step back ``months`` calendar months from ``d``, matching the old
    ``end.month - window_months`` rollover logic. The day is clamped so the
    result is always valid (e.g. Dec 31 − 3 months → Sep 30, not Sep 31).
    """
    y, m = d.year, d.month
    m -= months
    while m <= 0:
        m += 12
        y -= 1
    last = calendar.monthrange(y, m)[1]
    return datetime.date(y, m, min(d.day, last))


def date_windows(
    period_start: datetime.date,
    period_end: datetime.date,
    window_months: int,
) -> list[tuple[str, str]]:
    """
    Chunk the inclusive range ``[period_start, period_end]`` into windows of at most
    ``window_months`` calendar months each, **newest first** (same iteration order as
    before: latest chunk first, then stepping backward).
    """
    if period_start > period_end:
        raise ValueError("period_start must be on or before period_end")
    windows: list[tuple[str, str]] = []
    end = period_end
    while end >= period_start:
        start = _subtract_calendar_months(end, window_months)
        if start < period_start:
            start = period_start
        if start > end:
            break
        windows.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
        if start <= period_start:
            break
        end = start - datetime.timedelta(days=1)
    return windows


def query_gdelt_doc_with_retry(
    keyword: str,
    start: str,
    end: str,
    gd: GdeltDoc,
    *,
    country: str,
    language: str,
    max_records: int,
    fetch_log_path: Path,
    max_retries: int,
    backoff_base: float,
    backoff_max: float,
) -> pd.DataFrame:
    """
    GDELT DOC article search with backoff/retry on rate limits and transient errors.
    Appends one JSON line to ``fetch_log_path`` per call (final success or failure).
    """
    attempt = 0
    last_err: BaseException | None = None
    while attempt < max_retries:
        attempt += 1
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
                df = df.copy()
                df["query_keyword"] = keyword
                df["query_start"] = start
                df["query_end"] = end
            else:
                df = pd.DataFrame()
            append_fetch_log(
                fetch_log_path,
                {
                    "ts": utc_now_iso(),
                    "keyword": keyword,
                    "window_start": start,
                    "window_end": end,
                    "status": "success",
                    "n_rows": len(df),
                    "attempts": attempt,
                    "error": None,
                },
            )
            return df
        except Exception as e:
            last_err = e
            if _retryable_doc_error(e) and attempt < max_retries:
                delay = min(backoff_max, backoff_base * (2 ** (attempt - 1)))
                ename = type(e).__name__
                print(
                    f"    ⚠ {ename} (attempt {attempt}/{max_retries}), "
                    f"sleeping {delay:.1f}s …",
                    flush=True,
                )
                time.sleep(delay)
                continue
            break

    msg = ""
    if last_err is not None:
        msg = str(last_err).strip() or repr(last_err)
        print(
            f"    ⚠ Error querying '{keyword}' [{start}→{end}]: "
            f"{type(last_err).__name__}: {msg}"
        )
    append_fetch_log(
        fetch_log_path,
        {
            "ts": utc_now_iso(),
            "keyword": keyword,
            "window_start": start,
            "window_end": end,
            "status": "failed",
            "n_rows": 0,
            "attempts": attempt,
            "error": f"{type(last_err).__name__}: {msg}" if last_err else None,
        },
    )
    return pd.DataFrame()


def _sql_escape(s: str) -> str:
    return str(s).replace("\\", "\\\\").replace("'", "''")


def _normalize_seendate(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if hasattr(val, "strftime"):
        try:
            return val.strftime("%Y%m%d%H%M%S")
        except (ValueError, OSError):
            pass
    s = str(val).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def build_gkg_partitioned_fetch_sql(
    *,
    d_start: str,
    d_end: str,
    theme_terms: list[str],
    location_terms: list[str],
    row_limit: int,
) -> str:
    """
    Build SQL against public ``gdelt-bq.gdeltv2.gkg_partitioned``.
    Filters: partition date range, OR of V2Themes LIKE, OR of V2Locations LIKE.
    """
    if not theme_terms:
        raise ValueError("BigQuery fetch requires at least one theme from gkg_theme_sets")
    if not location_terms:
        raise ValueError("BigQuery fetch requires gkg_geography.location_country_codes")

    theme_ors = " OR ".join(
        f"g.V2Themes LIKE '%{_sql_escape(t)}%'" for t in theme_terms
    )
    loc_ors = " OR ".join(
        f"g.V2Locations LIKE '%{_sql_escape(loc)}%'" for loc in location_terms
    )

    return f"""
SELECT
  g.DocumentIdentifier,
  g.DATE AS gkg_date,
  g.SourceCommonName,
  g.SharingImage
FROM `gdelt-bq.gdeltv2.gkg_partitioned` AS g
WHERE DATE(g._PARTITIONTIME, 'UTC') BETWEEN DATE('{_sql_escape(d_start)}')
                                       AND DATE('{_sql_escape(d_end)}')
  AND ({theme_ors})
  AND ({loc_ors})
ORDER BY g.DATE DESC
LIMIT {int(row_limit)}
"""


def build_gkg_partitioned_fetch_sql_url_keywords(
    *,
    partition_time_start: str,
    partition_time_end: str,
    url_patterns: list[str],
    v2_locations_like: str,
    document_identifier_not_like: list[str],
    row_limit: int,
) -> str:
    """
    BigQuery discovery via URL slug proxies: OR of ``DocumentIdentifier LIKE``,
    hard location on ``V2Locations``, optional ``NOT LIKE`` noise filters.
    Uses ``_PARTITIONTIME`` half-open range ``[start, end)`` like ad-hoc GDELT SQL.
    """
    if not url_patterns:
        raise ValueError("url_keyword_patterns must be non-empty")
    url_ors = " OR ".join(
        f"g.DocumentIdentifier LIKE '{_sql_escape(p)}'" for p in url_patterns
    )
    not_clauses = ""
    for p in document_identifier_not_like:
        not_clauses += f"\n  AND g.DocumentIdentifier NOT LIKE '{_sql_escape(p)}'"
    pts = _sql_escape(partition_time_start)
    pte = _sql_escape(partition_time_end)
    loc = _sql_escape(v2_locations_like)
    return f"""
SELECT DISTINCT
  g.DocumentIdentifier AS url,
  g.DATE AS gkg_date,
  g.SourceCommonName,
  g.SharingImage,
  g.V2Themes,
  g.V2Locations,
  g.V2Tone
FROM `gdelt-bq.gdeltv2.gkg_partitioned` AS g
WHERE
  g._PARTITIONTIME >= TIMESTAMP('{pts}')
  AND g._PARTITIONTIME < TIMESTAMP('{pte}')
  AND ({url_ors})
  AND g.V2Locations LIKE '{loc}'{not_clauses}
ORDER BY g.DATE DESC
LIMIT {int(row_limit)}
"""


def fetch_urls_bigquery(
    *,
    meta: dict,
    cfg: dict,
    windows: list[tuple[str, str]],
    project: str,
    dry_run: bool,
    max_rows_cfg: int,
) -> tuple[
    pd.DataFrame,
    int,
    int,
    list[dict],
    float,
    int,
    str,
    list[tuple[str, str]],
]:
    """
    Query GKG partitioned table. Returns (combined_dedup, total_raw, total_dedup,
    window_stats, gb_billed, match_count, bq_mode, windows_for_summary).

    ``match_count`` is theme OR-term count (themes mode) or URL LIKE pattern count
    (url_keywords mode). ``bq_mode`` is ``themes`` or ``url_keywords``.
    """
    from google.cloud import bigquery

    bq_spec = get_bigquery_gkg_fetch_spec(meta, windows)
    row_limit = min(DRY_RUN_MAX_ARTICLES, max_rows_cfg) if dry_run else max_rows_cfg

    if bq_spec["mode"] == "url_keywords":
        pts = bq_spec["partition_time_start"]
        pte = bq_spec["partition_time_end"]
        pats = bq_spec["url_keyword_patterns"]
        sql = build_gkg_partitioned_fetch_sql_url_keywords(
            partition_time_start=pts,
            partition_time_end=pte,
            url_patterns=pats,
            v2_locations_like=bq_spec["v2_locations_like"],
            document_identifier_not_like=bq_spec["document_identifier_not_like"],
            row_limit=row_limit,
        )
        range_start = pts[:10] if len(pts) >= 10 else pts
        range_end = pte[:10] if len(pte) >= 10 else pte
        windows_for_summary = [(range_start, range_end)]
        match_count = len(pats)
        print("\nBigQuery URL discovery (gkg_partitioned) — URL keyword mode")
        print(f"  Project : {project}")
        print(f"  Partition: TIMESTAMP('{pts}') ≤ _PARTITIONTIME < TIMESTAMP('{pte}')")
        print(f"  URL LIKE patterns: {match_count} (bigquery_gkg_fetch.url_keyword_patterns)")
        print(f"  V2Locations LIKE: {bq_spec['v2_locations_like']!r}")
        print(f"  NOT LIKE exclusions: {len(bq_spec['document_identifier_not_like'])}")
        print(f"  LIMIT   : {row_limit}")
        if dry_run:
            print(f"  (dry run — capped at {DRY_RUN_MAX_ARTICLES} rows)\n")
    else:
        themes = get_gkg_theme_codes_for_bigquery_fetch(meta)
        country_codes, _sub = get_gkg_geography(meta)
        loc_terms = sorted({*country_codes, "India", "#IN#", "#IND#"})
        range_start = windows[-1][0]
        range_end = windows[0][1]
        windows_for_summary = windows
        match_count = len(themes)
        sql = build_gkg_partitioned_fetch_sql(
            d_start=range_start,
            d_end=range_end,
            theme_terms=themes,
            location_terms=loc_terms,
            row_limit=row_limit,
        )
        print("\nBigQuery URL discovery (gkg_partitioned) — theme mode")
        print(f"  Project : {project}")
        print(f"  Date    : {range_start} → {range_end} (partition filter)")
        print(f"  Themes  : {match_count} OR terms from gkg_theme_sets")
        print(f"  Loc     : {len(loc_terms)} OR patterns from gkg_geography + India")
        print(f"  LIMIT   : {row_limit}")
        if dry_run:
            print(f"  (dry run — capped at {DRY_RUN_MAX_ARTICLES} rows)\n")

    client = bigquery.Client(project=project)
    job = client.query(sql)
    df = job.result().to_dataframe()
    bytes_billed = job.total_bytes_billed or 0
    gb_billed = bytes_billed / 1e9

    print(f"  ✓ Rows returned: {len(df)}")
    print(f"  ✓ Bytes billed: {gb_billed:.2f} GB")

    bq_mode = str(bq_spec["mode"])

    if df.empty:
        empty = pd.DataFrame()
        return (
            empty,
            0,
            0,
            [],
            gb_billed,
            match_count,
            bq_mode,
            windows_for_summary,
        )

    total_raw = len(df)
    if "DocumentIdentifier" in df.columns:
        df = df.rename(columns={"DocumentIdentifier": "url"})
    df = df.drop_duplicates(subset=["url"], keep="first")

    language = str(cfg.get("language", "English"))
    q_start, q_end = windows_for_summary[-1][0], windows_for_summary[0][1]

    rows = []
    has_gkg_cols = "V2Themes" in df.columns
    for _, r in df.iterrows():
        url = str(r.get("url") or "").strip()
        if not url:
            continue
        dom = str(r.get("SourceCommonName") or "").strip()
        if not dom:
            dom = urlparse(url).netloc or ""
        row = {
            "url": url,
            "title": "",
            "seendate": _normalize_seendate(r.get("gkg_date")),
            "domain": dom,
            "language": language,
            "sourcecountry": str(cfg.get("country", "IN")),
            "query_keyword": (
                "bigquery_gkg_url_keywords"
                if bq_mode == "url_keywords"
                else "bigquery_gkg"
            ),
            "query_start": q_start,
            "query_end": q_end,
            "url_mobile": "",
            "socialimage": str(r.get("SharingImage") or ""),
        }
        if has_gkg_cols:
            row["gkg_v2_themes"] = str(r.get("V2Themes") or "")
            row["gkg_v2_locations"] = str(r.get("V2Locations") or "")
            row["gkg_v2_tone"] = str(r.get("V2Tone") or "")
        rows.append(row)

    out = pd.DataFrame(rows)
    total_dedup = len(out)
    window_stats = [{
        "window_start": q_start,
        "window_end": q_end,
        "raw_articles": total_raw,
        "keywords_with_results": total_dedup,
    }]
    return (
        out,
        total_raw,
        total_dedup,
        window_stats,
        gb_billed,
        match_count,
        bq_mode,
        windows_for_summary,
    )


def write_csv_and_summary(
    *,
    combined_dedup: pd.DataFrame,
    total_raw: int,
    total_dedup: int,
    window_stats: list[dict],
    windows: list[tuple[str, str]],
    keywords: list[str],
    summary_title: str,
    output_csv: Path,
    output_summary: Path,
    dry_run: bool,
    source: str,
    gb_billed: float | None,
    theme_count: int | None = None,
    bigquery_mode: str | None = None,
):
    """Write {prefix}_urls.csv and summary txt; print summary."""
    out_df = combined_dedup.copy() if not combined_dedup.empty else combined_dedup
    if not out_df.empty:
        if "seendate" in out_df.columns:
            out_df = out_df.sort_values("seendate", ascending=False)
        if "event_id" not in out_df.columns:
            out_df.insert(
                0, "event_id",
                [str(uuid.uuid4()) for _ in range(len(out_df))],
            )
        else:
            ev = out_df["event_id"].astype(str).str.strip()
            missing = ev.eq("") | ev.eq("nan") | out_df["event_id"].isna()
            if missing.any():
                nmiss = int(missing.sum())
                out_df.loc[missing, "event_id"] = [
                    str(uuid.uuid4()) for _ in range(nmiss)
                ]
            cols = list(out_df.columns)
            if cols[0] != "event_id":
                out_df = out_df[["event_id"] + [c for c in cols if c != "event_id"]]
        cols = [
            "event_id", "url", "title", "seendate", "domain", "language",
            "sourcecountry", "query_keyword", "query_start", "query_end",
            "url_mobile", "socialimage",
        ]
        cols = [c for c in cols if c in out_df.columns]
        for _gkg in ("gkg_v2_themes", "gkg_v2_locations", "gkg_v2_tone"):
            if _gkg in out_df.columns and _gkg not in cols:
                cols.append(_gkg)
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        out_df[cols].to_csv(output_csv, index=False, encoding="utf-8")
        print(f"\n✓ Saved {total_dedup} deduplicated articles → {output_csv}")
    else:
        print("\n⚠ No articles found across all queries.")

    stats_df = pd.DataFrame(window_stats) if window_stats else pd.DataFrame()

    summary_lines = [summary_title]
    if dry_run:
        summary_lines.append(
            f"(dry run — at most {DRY_RUN_MAX_ARTICLES} deduplicated articles)"
        )
    summary_lines += [
        "=" * 60,
        f"Source               : {source}",
        f"Date range attempted : {windows[-1][0]} to {windows[0][1]}",
    ]
    if source == "doc":
        summary_lines += [
            f"Keywords searched    : {len(keywords)}",
            f"Time windows         : {len(windows)}",
        ]
    else:
        tc = theme_count if theme_count is not None else 0
        if bigquery_mode == "url_keywords":
            summary_lines += [
                f"URL LIKE patterns     : {tc} (bigquery_gkg_fetch)",
                "Partition / match     : _PARTITIONTIME + URL + V2Locations (see meta)",
            ]
        else:
            summary_lines += [
                f"GKG theme OR terms   : {tc} (union of both theme groups in meta)",
                "Partition range      : single query over full span above",
            ]

    summary_lines += [
        f"Raw articles (with duplicates): {total_raw}",
        f"Deduplicated articles         : {total_dedup}",
        "",
        "Results by window:",
        stats_df.to_string(index=False) if not stats_df.empty else "(none)",
        "",
        "Top domains (if any results):",
    ]

    if not out_df.empty and "domain" in out_df.columns:
        top_domains = out_df["domain"].value_counts().head(20)
        summary_lines.append(top_domains.to_string())
        summary_lines += ["", "Articles per query_keyword (before dedup):"]
        if "query_keyword" in out_df.columns:
            summary_lines.append(out_df["query_keyword"].value_counts().to_string())

    if source == "doc":
        summary_lines += [
            "",
            "⚠  IMPORTANT NOTES:",
            "  1. GDELT DOC API reliably covers only the last ~3 months.",
            "     Older windows above are best-effort and may be sparse.",
            "  2. For a longer historical span, widen fetch_start_date / fetch_end_date in meta,",
            "     or use --source bigquery or ad-hoc SQL.",
            "  3. Each keyword query is capped by max_records in meta.",
            "  4. Next step: GKG enrichment (local files or BigQuery join).",
        ]
    else:
        if bigquery_mode == "url_keywords":
            summary_lines += [
                "",
                "⚠  IMPORTANT NOTES:",
                "  1. Discovery used bigquery_gkg_fetch (URL slug LIKE + V2Locations + exclusions).",
                "     It does not send gdelt_doc_fetch.keywords to BigQuery — patterns live in meta.",
                f"  2. BigQuery bytes billed (this run): ~{gb_billed or 0:.2f} GB.",
                "  3. Titles are empty — downstream full-text fetch supplies text.",
                "  4. gkg_v2_* columns are GKG previews; enrichment still scores themes per URL.",
            ]
        else:
            summary_lines += [
                "",
                "⚠  IMPORTANT NOTES:",
                "  1. This run did NOT use gdelt_doc_fetch.keywords — only GKG themes + geography.",
                "     For keyword-driven discovery, re-run with --source doc (default), then enrich.",
                f"  2. BigQuery bytes billed (this run): ~{gb_billed or 0:.2f} GB.",
                "  3. Titles are empty — downstream full-text fetch supplies text.",
                "  4. Next: prefer doc fetch + gdelt-enrich-urls-bigquery.py for the usual pipeline.",
            ]

    summary_text = "\n".join(summary_lines)
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    with open(output_summary, "w", encoding="utf-8", newline="\n") as f:
        f.write(summary_text)

    print("\n" + summary_text)
    print(f"\n✓ Summary saved → {output_summary}")


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    ap = argparse.ArgumentParser(
        description="Fetch GDELT article URLs via DOC API or BigQuery GKG",
    )
    ap.add_argument(
        "--meta",
        default=str(_DEFAULT_META),
        help="Domain meta JSON (gdelt_doc_fetch; bigquery also uses gkg_theme_sets + gkg_geography)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            f"DOC: one window, ≤{DRY_RUN_MAX_ARTICLES} articles. "
            f"BigQuery: same row cap."
        ),
    )
    ap.add_argument(
        "--source",
        choices=("doc", "bigquery"),
        default="doc",
        help=(
            "doc (default): GDELT DOC API — uses gdelt_doc_fetch.keywords. "
            "bigquery: scan gkg_partitioned by theme/geography only; "
            "IGNORES keywords (use doc first, then gdelt-enrich-urls-bigquery.py)."
        ),
    )
    ap.add_argument(
        "--project",
        default=None,
        help="GCP project ID for --source bigquery (default: GOOGLE_CLOUD_PROJECT / .env)",
    )
    ap.add_argument(
        "--fetch-log",
        default=None,
        help=(
            "DOC API only: append JSONL log here (default: data/{prefix}_fetch_log.jsonl)"
        ),
    )
    ap.add_argument(
        "--retry-failed",
        action="store_true",
        help=(
            "DOC API only: re-run only (keyword × window) pairs whose last log line "
            "is failed; merge new URLs into existing data/{prefix}_urls.csv"
        ),
    )
    ap.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="DOC API: max attempts per query (rate limits / transient errors)",
    )
    ap.add_argument(
        "--backoff-base",
        type=float,
        default=5.0,
        help="DOC API: initial backoff seconds (exponential, capped by --backoff-max)",
    )
    ap.add_argument(
        "--backoff-max",
        type=float,
        default=300.0,
        help="DOC API: max seconds between retries",
    )
    args = ap.parse_args()

    load_repo_env()
    meta = load_domain_meta(args.meta)
    prefix = output_prefix(args.meta)
    output_csv = urls_csv(_ROOT, prefix)
    output_summary = urls_summary_txt(_ROOT, prefix)
    fetch_log_path = (
        Path(args.fetch_log).expanduser()
        if args.fetch_log
        else urls_fetch_log_jsonl(_ROOT, prefix)
    )
    cfg = get_gdelt_doc_fetch(meta)
    keywords = list(cfg["keywords"])
    country = str(cfg["country"])
    language = str(cfg["language"])
    max_records = int(cfg.get("max_records", 250))
    sleep_q = float(cfg.get("sleep_between_queries_seconds", 3))
    period_start, period_end = get_gdelt_doc_fetch_date_range(cfg)
    window_months = int(cfg.get("window_months", 3))
    banner = str(cfg.get("summary_banner", "GDELT DOC fetch"))
    summary_title = str(cfg.get("summary_title", "GDELT summary"))
    bq_max_rows = int(cfg.get("bigquery_max_rows", 50_000))

    windows = date_windows(period_start, period_end, window_months)
    if args.dry_run:
        windows = windows[:1]

    gb_billed: float | None = None

    if args.retry_failed and args.source != "doc":
        ap.error("--retry-failed requires --source doc (default)")

    if args.source == "bigquery":
        project = (args.project or os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip() or None
        if not project:
            ap.error(
                "GCP project required for --source bigquery: pass --project or set "
                "GOOGLE_CLOUD_PROJECT (e.g. repo root .env)."
            )
        bq_spec = get_bigquery_gkg_fetch_spec(meta, windows)
        if bq_spec.get("mode") == "url_keywords":
            print(
                "\n*** --source bigquery (URL keyword mode) ***\n"
                "  Using meta `bigquery_gkg_fetch`: DocumentIdentifier LIKE patterns + "
                "V2Locations + exclusions.\n"
                "  This does not use gdelt_doc_fetch.keywords — edit url_keyword_patterns "
                "in meta to tune.\n",
                flush=True,
            )
        else:
            print(
                "\n*** WARNING: --source bigquery (theme mode) ***\n"
                "  gdelt_doc_fetch.keywords are NOT used. "
                "URLs are discovered by GKG theme + location filters only,\n"
                "  so many rows may be unrelated to your keyword list.\n"
                "  For keyword-driven discovery: omit --source (use DOC API), "
                "then run gdelt-enrich-urls-bigquery.py on the CSV.\n"
                "  Or set bigquery_gkg_fetch.mode to \"url_keywords\" for URL-slug matching.\n",
                flush=True,
            )
        print(banner)
        print(f"{'='*60}")
        print("MODE: BigQuery gkg_partitioned (not DOC API)\n")
        (
            combined_dedup,
            total_raw,
            total_dedup,
            window_stats,
            gb_billed,
            n_match,
            bq_mode,
            windows_for_summary,
        ) = fetch_urls_bigquery(
            meta=meta,
            cfg=cfg,
            windows=windows,
            project=project,
            dry_run=args.dry_run,
            max_rows_cfg=bq_max_rows,
        )
        write_csv_and_summary(
            combined_dedup=combined_dedup,
            total_raw=total_raw,
            total_dedup=total_dedup,
            window_stats=window_stats,
            windows=windows_for_summary,
            keywords=keywords,
            summary_title=summary_title,
            output_csv=output_csv,
            output_summary=output_summary,
            dry_run=args.dry_run,
            source="bigquery",
            gb_billed=gb_billed,
            theme_count=n_match,
            bigquery_mode=bq_mode,
        )
        return

    # ── DOC API path ──────────────────────────────────────────────────────────
    gd = GdeltDoc()
    max_retries = max(1, int(args.max_retries))
    backoff_base = float(args.backoff_base)
    backoff_max = float(args.backoff_max)

    if args.retry_failed:
        tasks = load_queries_to_retry_from_log(fetch_log_path)
        if not tasks:
            print(
                f"No failed queries to retry: empty log or last line per "
                f"keyword×window is not failed.\n→ {fetch_log_path}",
                flush=True,
            )
            return
        windows_summary = [
            (min(t[1] for t in tasks), max(t[2] for t in tasks)),
        ]
        keywords_summary = sorted({t[0] for t in tasks})
        print(
            f"\n*** RETRY MODE ***\n"
            f"  Log file : {fetch_log_path}\n"
            f"  Queries  : {len(tasks)} (failed keyword×window pairs)\n"
            f"  Merge into: {output_csv}\n",
            flush=True,
        )
    else:
        tasks = None
        windows_summary = windows
        keywords_summary = keywords

    if args.dry_run and not args.retry_failed:
        print(
            f"DRY RUN: single most recent window only; stop at "
            f"{DRY_RUN_MAX_ARTICLES} deduplicated URLs.\n"
        )

    print(banner)
    print(f"{'='*60}")
    print(f"MODE: GDELT DOC API" + (" — retry failed only" if args.retry_failed else ""))
    print(f"Fetch log: {fetch_log_path}")
    if args.retry_failed:
        print(f"Keywords (this run): {len(keywords_summary)}")
        print(f"API calls          : {len(tasks)}")
    else:
        print(f"Keywords : {len(keywords)}")
        print(
            f"Windows  : {len(windows)} × {window_months}-month chunks "
            f"from {period_start} to {period_end}"
        )
        print(f"Total    : up to {len(keywords) * len(windows)} API calls")
    print(f"NOTE: Only the most recent ~3 months are guaranteed by GDELT DOC API.")
    print(f"      Older windows will be attempted but may return sparse results.\n")

    all_frames = []
    window_stats = []

    if args.retry_failed:
        assert tasks is not None
        per_query_cap = (
            min(max_records, DRY_RUN_MAX_ARTICLES) if args.dry_run else max_records
        )
        for ti, (kw, start, end) in enumerate(tasks):
            print(
                f"\n[Retry {ti + 1}/{len(tasks)}]  '{kw}'  {start} → {end}",
                flush=True,
            )
            print(f"  → querying ... ", end="", flush=True)
            df = query_gdelt_doc_with_retry(
                kw,
                start,
                end,
                gd,
                country=country,
                language=language,
                max_records=per_query_cap,
                fetch_log_path=fetch_log_path,
                max_retries=max_retries,
                backoff_base=backoff_base,
                backoff_max=backoff_max,
            )
            n = len(df)
            print(f"{n} articles", flush=True)
            if n > 0:
                all_frames.append(df)
            time.sleep(sleep_q)
        window_stats = []
    else:
        for i, (start, end) in enumerate(windows):
            print(f"\n[Window {i+1}/{len(windows)}]  {start} → {end}")
            window_dfs = []
            keywords_with_results = 0

            per_query_cap = (
                min(max_records, DRY_RUN_MAX_ARTICLES) if args.dry_run else max_records
            )
            for kw in keywords:
                print(f"  → querying: '{kw}' ... ", end="", flush=True)
                df = query_gdelt_doc_with_retry(
                    kw,
                    start,
                    end,
                    gd,
                    country=country,
                    language=language,
                    max_records=per_query_cap,
                    fetch_log_path=fetch_log_path,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                    backoff_max=backoff_max,
                )
                n = len(df)
                print(f"{n} articles")
                if n > 0:
                    window_dfs.append(df)
                    keywords_with_results += 1
                if args.dry_run and window_dfs:
                    wcomb = pd.concat(window_dfs, ignore_index=True)
                    wdedup = wcomb.drop_duplicates(subset="url", keep="first")
                    if len(wdedup) >= DRY_RUN_MAX_ARTICLES:
                        break
                time.sleep(sleep_q)

            if window_dfs:
                window_df = pd.concat(window_dfs, ignore_index=True)
                if args.dry_run:
                    window_df = window_df.drop_duplicates(
                        subset="url", keep="first"
                    ).head(DRY_RUN_MAX_ARTICLES)
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

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        total_raw = len(combined)
        combined_dedup = combined.drop_duplicates(subset="url", keep="first")
        total_dedup = len(combined_dedup)
        combined_dedup = combined_dedup.copy()
    else:
        total_raw = 0
        total_dedup = 0
        combined_dedup = pd.DataFrame()

    if args.retry_failed:
        combined_dedup = merge_existing_urls_csv(output_csv, combined_dedup)
        total_dedup = len(combined_dedup)
        # total_raw remains count from this run's API responses only (see summary)

    stitle = summary_title
    if args.retry_failed:
        stitle = f"{summary_title} (retry failed only)"

    write_csv_and_summary(
        combined_dedup=combined_dedup,
        total_raw=total_raw,
        total_dedup=total_dedup,
        window_stats=window_stats,
        windows=windows_summary,
        keywords=keywords_summary,
        summary_title=stitle,
        output_csv=output_csv,
        output_summary=output_summary,
        dry_run=args.dry_run,
        source="doc",
        gb_billed=None,
        theme_count=None,
    )


if __name__ == "__main__":
    main()
