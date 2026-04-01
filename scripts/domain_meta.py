"""
Load domain configuration from meta/*.json for GDELT pipeline scripts.

Prompts may be stored as either:
  - llm_extraction.system_prompt / extraction_prompt (single string, use \\n for newlines in JSON), or
  - llm_extraction.system_prompt_lines / extraction_prompt_lines (array of lines; preferred for readability).

extraction_prompt must remain a str.format template with placeholders:
  {pub_date}, {url}, {gdelt_locations}, {article_text}
Literal braces elsewhere must be doubled ({{ }}) so .format() leaves single braces in output.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any


def load_domain_meta(path: str | Path) -> dict[str, Any]:
    p = Path(path).resolve()
    if not p.is_file():
        raise FileNotFoundError(f"Domain meta file not found: {p}")
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _joined_or_key(section: dict[str, Any], key: str, lines_key: str) -> str:
    if section.get(key) is not None:
        return str(section[key])
    lines = section.get(lines_key)
    if isinstance(lines, list):
        return "\n".join(str(x) for x in lines)
    raise KeyError(f"Define {key} or {lines_key}")


def get_llm_prompts(meta: dict[str, Any]) -> tuple[str, str]:
    llm = meta.get("llm_extraction")
    if not isinstance(llm, dict):
        raise KeyError("meta must include object llm_extraction")
    system = _joined_or_key(llm, "system_prompt", "system_prompt_lines")
    extraction = _joined_or_key(llm, "extraction_prompt", "extraction_prompt_lines")
    for name in llm.get("extraction_prompt_placeholders") or (
        "pub_date",
        "url",
        "gdelt_locations",
        "article_text",
    ):
        token = "{" + str(name) + "}"
        if token not in extraction:
            raise ValueError(
                f"llm_extraction extraction prompt must contain placeholder {token!r}"
            )
    return system, extraction


def get_gkg_theme_sets(meta: dict[str, Any]) -> tuple[set[str], set[str], int]:
    """
    Two theme groups for GKG scoring (enrich scripts, BigQuery fetch).

    **Canonical** meta keys: ``primary_themes`` + ``secondary_themes`` (arrays of GKG
    V2Themes codes). Scoring: 1 = primary only, 2 = secondary only, 3 = both (when
    ``high_confidence_theme_score_min`` is 3).

    **Legacy** (still accepted): ``wildlife_themes`` + ``conflict_themes``, or
    ``primary_themes`` + ``harm_themes`` (older crop-damage files).
    """
    gs = meta.get("gkg_theme_sets")
    if not isinstance(gs, dict):
        raise KeyError("meta must include object gkg_theme_sets")
    hc = int(gs.get("high_confidence_theme_score_min", 3))
    prim = gs.get("primary_themes")
    sec = gs.get("secondary_themes")
    if isinstance(prim, list) and isinstance(sec, list):
        return set(str(x) for x in prim), set(str(x) for x in sec), hc
    w = gs.get("wildlife_themes")
    c = gs.get("conflict_themes")
    if isinstance(w, list) and isinstance(c, list):
        return set(str(x) for x in w), set(str(x) for x in c), hc
    harm = gs.get("harm_themes")
    if isinstance(prim, list) and isinstance(harm, list):
        return set(str(x) for x in prim), set(str(x) for x in harm), hc
    raise KeyError(
        "gkg_theme_sets must define primary_themes and secondary_themes as arrays "
        "(GKG V2Themes codes). Legacy: wildlife_themes+conflict_themes, or "
        "primary_themes+harm_themes."
    )


def get_gkg_theme_codes_for_bigquery_fetch(meta: dict[str, Any]) -> list[str]:
    """
    Sorted union of both theme groups for ``gkg_partitioned`` V2Themes LIKE filters
    (``gdelt-fetch-urls.py --source bigquery``). Uses the same meta layout as
    :func:`get_gkg_theme_sets`.
    """
    a, b, _hc = get_gkg_theme_sets(meta)
    return sorted(a | b)


def get_gkg_geography(meta: dict[str, Any]) -> tuple[tuple[str, ...], set[int]]:
    geo = meta.get("gkg_geography")
    if not isinstance(geo, dict):
        raise KeyError("meta must include object gkg_geography")
    codes = geo.get("location_country_codes")
    sub = geo.get("subnational_loc_types")
    if not isinstance(codes, list):
        raise KeyError("gkg_geography.location_country_codes must be an array")
    if not isinstance(sub, list):
        raise KeyError("gkg_geography.subnational_loc_types must be an array")
    return tuple(str(x) for x in codes), {int(x) for x in sub}


def get_gdelt_doc_fetch(meta: dict[str, Any]) -> dict[str, Any]:
    g = meta.get("gdelt_doc_fetch")
    if not isinstance(g, dict):
        raise KeyError("meta must include object gdelt_doc_fetch")
    required = ("keywords", "country", "language")
    for k in required:
        if k not in g:
            raise KeyError(f"gdelt_doc_fetch.{k} is required")
    return g


def get_gdelt_doc_fetch_date_range(cfg: dict[str, Any]) -> tuple[datetime.date, datetime.date]:
    """
    Inclusive ``[start, end]`` for DOC/BigQuery windowing in ``gdelt-fetch-urls.py``.

    - ``fetch_start_date`` — required, ``YYYY-MM-DD``.
    - ``fetch_end_date`` — optional; if omitted or empty, **end** is **today** (UTC date
      in the local timezone of the process — same as ``datetime.date.today()``).
    """
    if "fetch_start_date" not in cfg:
        raise KeyError(
            "gdelt_doc_fetch.fetch_start_date is required (YYYY-MM-DD); "
            "replace deprecated years_back."
        )
    try:
        start = datetime.date.fromisoformat(str(cfg["fetch_start_date"]).strip())
    except ValueError as e:
        raise ValueError(
            f"gdelt_doc_fetch.fetch_start_date must be YYYY-MM-DD, "
            f"got {cfg['fetch_start_date']!r}"
        ) from e
    raw_end = cfg.get("fetch_end_date")
    if raw_end is None or str(raw_end).strip() == "":
        end = datetime.date.today()
    else:
        try:
            end = datetime.date.fromisoformat(str(raw_end).strip())
        except ValueError as e:
            raise ValueError(
                f"gdelt_doc_fetch.fetch_end_date must be YYYY-MM-DD, got {raw_end!r}"
            ) from e
    if start > end:
        raise ValueError(
            "gdelt_doc_fetch.fetch_start_date must be on or before fetch_end_date "
            "(when fetch_end_date is omitted, end is today)"
        )
    return start, end


def get_bigquery_gkg_fetch_spec(
    meta: dict[str, Any],
    windows: list[tuple[str, str]],
) -> dict[str, Any]:
    """
    Optional ``bigquery_gkg_fetch`` in domain meta for ``gdelt-fetch-urls.py --source bigquery``.

    - ``mode`` ``themes`` (default): V2Themes OR + geography (existing behaviour).
    - ``mode`` ``url_keywords``: OR of ``DocumentIdentifier LIKE`` patterns (URL slug proxy),
      plus ``V2Locations LIKE``, optional ``document_identifier_not_like``, partition bounds.
    """
    raw = meta.get("bigquery_gkg_fetch")
    if not isinstance(raw, dict):
        return {"mode": "themes"}
    mode = str(raw.get("mode", "themes")).strip().lower()
    if mode == "themes":
        return {"mode": "themes"}
    if mode != "url_keywords":
        raise ValueError("bigquery_gkg_fetch.mode must be 'themes' or 'url_keywords'")
    pats = raw.get("url_keyword_patterns")
    if not isinstance(pats, list) or not pats:
        raise ValueError(
            "bigquery_gkg_fetch.url_keyword_patterns (non-empty list) is required "
            "when mode is url_keywords"
        )
    pts = raw.get("partition_time_start")
    pte = raw.get("partition_time_end")
    if not pts or not pte:
        rs = windows[-1][0]
        re = windows[0][1]
        pts = f"{rs}T00:00:00Z"
        pte = f"{re}T23:59:59.999999Z"
    loc = str(raw.get("v2_locations_like", "%#IN#%"))
    not_like = raw.get("document_identifier_not_like")
    if not_like is None:
        not_like = []
    if not isinstance(not_like, list):
        raise ValueError("bigquery_gkg_fetch.document_identifier_not_like must be a list")
    return {
        "mode": "url_keywords",
        "url_keyword_patterns": [str(x) for x in pats],
        "partition_time_start": str(pts),
        "partition_time_end": str(pte),
        "v2_locations_like": loc,
        "document_identifier_not_like": [str(x) for x in not_like],
    }
