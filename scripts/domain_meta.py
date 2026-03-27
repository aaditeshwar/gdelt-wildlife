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
    gs = meta.get("gkg_theme_sets")
    if not isinstance(gs, dict):
        raise KeyError("meta must include object gkg_theme_sets")
    w = gs.get("wildlife_themes")
    c = gs.get("conflict_themes")
    if not isinstance(w, list) or not isinstance(c, list):
        raise KeyError("gkg_theme_sets.wildlife_themes and conflict_themes must be arrays")
    hc = int(gs.get("high_confidence_theme_score_min", 3))
    return set(str(x) for x in w), set(str(x) for x in c), hc


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
