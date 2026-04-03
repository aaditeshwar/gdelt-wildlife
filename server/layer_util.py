"""Resolve meta files → layer descriptors and GeoJSON paths."""

from __future__ import annotations

import json
import re
from pathlib import Path

from fastapi import HTTPException

_LAYER_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")


def output_prefix(meta_path: Path | str) -> str:
    stem = Path(meta_path).stem
    if "_" in stem:
        return stem.split("_")[0].lower()
    return stem[:3].lower() if len(stem) >= 3 else stem.lower()


def validate_layer_id(layer_id: str) -> str:
    if not layer_id or not _LAYER_ID_RE.match(layer_id):
        raise HTTPException(status_code=400, detail="invalid layer_id")
    return layer_id


def meta_path_for_layer(repo: Path, layer_id: str) -> Path:
    validate_layer_id(layer_id)
    p = (repo / "meta" / f"{layer_id}.json").resolve()
    root = repo.resolve()
    if not str(p).startswith(str(root)) or not p.is_file():
        raise HTTPException(status_code=404, detail="layer not found")
    return p


def load_meta(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def list_layer_descriptors(meta_dir: Path, outputs_dir: Path) -> list[dict]:
    repo = meta_dir.parent
    out: list[dict] = []
    for p in sorted(meta_dir.glob("*.json")):
        if p.name == "event_domain_template.json":
            continue
        stem = p.stem
        prefix = output_prefix(p)
        try:
            meta = load_meta(p)
        except OSError:
            continue
        domain = meta.get("domain") or {}
        title = domain.get("title") or stem
        gj = outputs_dir / f"{prefix}_points.geojson"
        qml = outputs_dir / f"{prefix}_india_points.qml"
        out.append(
            {
                "id": stem,
                "label": title,
                "meta_path": str(p.relative_to(repo)),
                "prefix": prefix,
                "geojson_path": str(gj.relative_to(repo)) if gj.is_file() else None,
                "has_geojson": gj.is_file(),
                "has_qml": qml.is_file(),
            }
        )
    return out


def geojson_file_for_layer(repo: Path, layer_id: str) -> Path:
    mp = meta_path_for_layer(repo, layer_id)
    prefix = output_prefix(mp)
    gj = (repo / "outputs" / f"{prefix}_points.geojson").resolve()
    root = repo.resolve()
    if not str(gj).startswith(str(root)):
        raise HTTPException(status_code=403, detail="invalid path")
    if not gj.is_file():
        raise HTTPException(status_code=404, detail="GeoJSON not found for layer")
    return gj


def qml_file_for_layer(repo: Path, layer_id: str) -> Path:
    """QGIS categorized style: outputs/{prefix}_india_points.qml (see domain_paths.points_qml)."""
    mp = meta_path_for_layer(repo, layer_id)
    prefix = output_prefix(mp)
    qml = (repo / "outputs" / f"{prefix}_india_points.qml").resolve()
    root = repo.resolve()
    if not str(qml).startswith(str(root)):
        raise HTTPException(status_code=403, detail="invalid path")
    if not qml.is_file():
        raise HTTPException(status_code=404, detail="QML style not found for layer")
    return qml


def style_payload_for_layer(repo: Path, layer_id: str) -> dict:
    mp = meta_path_for_layer(repo, layer_id)
    meta = load_meta(mp)
    ms = meta.get("map_style") or {}
    return {
        "colors_hex": ms.get("colors_hex") or {},
        "category_field": ms.get("category_field", "map_category"),
        "merge_groups": ms.get("merge_groups") or [],
        "singleton_event_types": ms.get("singleton_event_types") or [],
        "fallback_category": ms.get("fallback_category", "other"),
    }


def meta_summary_for_layer(repo: Path, layer_id: str) -> dict:
    """Public dashboard copy: domain blurb + discovery keywords + GKG themes + species examples."""
    mp = meta_path_for_layer(repo, layer_id)
    meta = load_meta(mp)
    dom = meta.get("domain") or {}
    doc = meta.get("gdelt_doc_fetch") or {}
    themes = meta.get("gkg_theme_sets") or {}
    tax = meta.get("taxonomy") or {}
    species_ex = (tax.get("species") or {}).get("examples") or []
    kw = doc.get("keywords") or []
    if isinstance(kw, list):
        gdelt_keywords = [str(x) for x in kw[:40]]
    else:
        gdelt_keywords = []
    prim = themes.get("primary_themes") or []
    sec = themes.get("secondary_themes") or []
    if not isinstance(prim, list):
        prim = []
    if not isinstance(sec, list):
        sec = []
    return {
        "domain": {
            "id": str(dom.get("id", "")),
            "title": str(dom.get("title", "")),
            "description": str(dom.get("description", "")),
        },
        "methodology": {
            "gdelt_keywords": gdelt_keywords,
            "gkg_primary_themes": [str(x) for x in prim[:50]],
            "gkg_secondary_themes": [str(x) for x in sec[:50]],
            "species_examples": [str(x) for x in species_ex[:30]],
            "fetch_start_date": doc.get("fetch_start_date"),
            "fetch_end_date": doc.get("fetch_end_date"),
        },
    }
