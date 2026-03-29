"""Load / update GeoJSON features by point id (event_id)."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import HTTPException


def read_fc(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_fc(path: Path, fc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), encoding="utf-8")


def find_feature_index(fc: dict, point_id: str) -> int | None:
    pid = str(point_id)
    for i, feat in enumerate(fc.get("features") or []):
        if feat.get("type") != "Feature":
            continue
        props = feat.get("properties") or {}
        fid = feat.get("id")
        if fid is not None and str(fid) == pid:
            return i
        if str(props.get("event_id", "")) == pid:
            return i
        if str(props.get("url", "")) == pid:
            return i
    return None


def merge_properties(geojson_path: Path, point_id: str, suggested: dict) -> dict:
    """Merge suggested into feature properties; return previous properties snapshot."""
    fc = read_fc(geojson_path)
    idx = find_feature_index(fc, point_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="feature not found")
    feat = fc["features"][idx]
    props = feat.setdefault("properties", {})
    before = dict(props)
    for k, v in suggested.items():
        props[k] = v
    if "event_id" in suggested:
        feat["id"] = suggested["event_id"]
    write_fc(geojson_path, fc)
    return before
