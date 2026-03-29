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
        out.append(
            {
                "id": stem,
                "label": title,
                "meta_path": str(p.relative_to(repo)),
                "prefix": prefix,
                "geojson_path": str(gj.relative_to(repo)) if gj.is_file() else None,
                "has_geojson": gj.is_file(),
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
