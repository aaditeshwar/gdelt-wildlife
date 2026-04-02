"""
CSV → GeoJSON (HWC points) + optional QGIS QML style
===================================================
Reads one or more extraction CSVs (concatenated in order), keeps rows where
is_hwc_event is true, valid final_lon/final_lat, and writes a Point GeoJSON.

Category field ``map_category`` follows meta/hwc_india_conflict_meta.json
(merged event types for styling).

Usage:
    python scripts/convert_csv_to_geojson.py
    python scripts/convert_csv_to_geojson.py --input data/hwc_final_report_updated.csv --output outputs/hwc_points.geojson
    python scripts/convert_csv_to_geojson.py --input run1/a.csv run2/b.csv --output outputs/hwc_points.geojson
    python scripts/convert_csv_to_geojson.py --meta meta/hwc_india_conflict_meta.json --write-qml outputs/hwc_india_points.qml
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import pandas as pd

from domain_paths import (  # noqa: E402
    ensure_event_id_column,
    output_prefix,
    points_qml,
)


def _truthy_hwc(val) -> bool:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return False
    s = str(val).strip().lower()
    if s in ("", "nan", "none", "false", "0", "no"):
        return False
    return s in ("true", "1", "yes") or val is True


def _finite_lon_lat(lon, lat) -> bool:
    try:
        lo = float(lon)
        la = float(lat)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(lo) or not math.isfinite(la):
        return False
    return -180.0 <= lo <= 180.0 and -90.0 <= la <= 90.0


def _norm_event_type(raw) -> str:
    if raw is None:
        return ""
    try:
        if pd.isna(raw):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(raw).strip().lower()
    if s in ("", "nan", "none"):
        return ""
    return s


def compute_map_category(event_type, meta: dict) -> str:
    ms = meta.get("map_style", {})
    et = _norm_event_type(event_type)
    if not et:
        return "unknown"
    for group in ms.get("merge_groups", []):
        merged = {str(x).strip().lower() for x in group.get("event_types", [])}
        if et in merged:
            return group["id"]
    singles = {str(x).strip().lower() for x in ms.get("singleton_event_types", [])}
    if et in singles:
        return et
    return ms.get("fallback_category", "other")


def hex_to_qgis_rgba(hex_color: str) -> str:
    h = hex_color.strip().lstrip("#")
    if len(h) != 6:
        return "128,128,128,255"
    r = int(h[0:2], 16)
    g = int(h[2:4], 16)
    b = int(h[4:6], 16)
    return f"{r},{g},{b},255"


def write_qml_categorized(path: Path, meta: dict, field_name: str) -> None:
    """Write a QGIS 3 categorized point style for ``field_name`` (default map_category)."""
    colors = meta.get("map_style", {}).get("colors_hex", {})
    labels = {}
    for group in meta.get("map_style", {}).get("merge_groups", []):
        labels[group["id"]] = group.get("label", group["id"])
    for s in meta.get("map_style", {}).get("singleton_event_types", []):
        labels[s] = s.replace("_", " ").title()
    labels.setdefault("other", "Other")
    labels.setdefault("unknown", "Unknown")

    keys = list(colors.keys())
    categories_xml = []
    symbols_xml = []
    for i, key in enumerate(keys):
        rgba = hex_to_qgis_rgba(colors[key])
        lab = labels.get(key, key)
        categories_xml.append(
            f'      <category label="{_xml_escape(lab)}" symbol="{i}" value="{_xml_escape(key)}" render="true"/>'
        )
        symbols_xml.append(
            f'''    <symbol name="{i}" type="marker" alpha="1" clip_to_extent="1" force_rhr="0" is_animated="0" frame_rate="10">
      <layer pass="0" class="SimpleMarker" enabled="1" locked="0">
        <prop k="angle" v="0"/>
        <prop k="cap_style" v="square"/>
        <prop k="color" v="{rgba}"/>
        <prop k="horizontal_anchor_point" v="1"/>
        <prop k="join_style" v="bevel"/>
        <prop k="name" v="circle"/>
        <prop k="offset" v="0,0"/>
        <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
        <prop k="offset_unit" v="MM"/>
        <prop k="outline_color" v="35,35,35,255"/>
        <prop k="outline_style" v="solid"/>
        <prop k="outline_width" v="0.4"/>
        <prop k="outline_width_map_unit_scale" v="3x:0,0,0,0,0,0"/>
        <prop k="outline_width_unit" v="MM"/>
        <prop k="scale_method" v="diameter"/>
        <prop k="size" v="2.5"/>
        <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
        <prop k="size_unit" v="MM"/>
        <prop k="vertical_anchor_point" v="1"/>
      </layer>
    </symbol>'''
        )

    cat_block = "\n".join(categories_xml)
    sym_block = "\n".join(symbols_xml)

    body = f'''<?xml version="1.0" encoding="UTF-8"?>
<qgis version="3.28" styleCategories="AllStyleCategories">
  <renderer-v2 type="categorizedSymbol" forceraster="0" symbollevels="0" enableorderby="0" attr="{_xml_escape(field_name)}">
    <categories>
{cat_block}
    </categories>
    <symbols>
{sym_block}
    </symbols>
    <source-symbol>
      <symbol name="0" type="marker" alpha="1" clip_to_extent="1" force_rhr="0" is_animated="0" frame_rate="10">
        <layer pass="0" class="SimpleMarker" enabled="1" locked="0">
          <prop k="color" v="200,200,200,255"/>
          <prop k="name" v="circle"/>
          <prop k="size" v="2.5"/>
        </layer>
      </symbol>
    </source-symbol>
    <rotation/>
    <sizescale/>
  </renderer-v2>
</qgis>
'''
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def _xml_escape(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(description="Convert extraction CSV to GeoJSON points + optional QML")
    p.add_argument(
        "--input",
        nargs="*",
        default=None,
        metavar="CSV",
        help=(
            "One or more input CSVs, merged in order (default: data/hwc_final_report_updated.csv). "
            "Prefer this over shell-concatenating files, which can break on embedded newlines."
        ),
    )
    p.add_argument(
        "--meta",
        default=str(root / "meta" / "hwc_india_conflict_meta.json"),
        help="Domain meta JSON (categories, colors)",
    )
    p.add_argument(
        "--output",
        default=str(root / "outputs" / "hwc_points.geojson"),
        help="Output GeoJSON path",
    )
    p.add_argument(
        "--write-qml",
        default=None,
        metavar="PATH",
        help="Write QGIS categorized style (default: outputs/hwc_india_points.qml if flag used without path)",
        nargs="?",
        const="__default__",
    )
    args = p.parse_args()

    meta_path = Path(args.meta)
    if not meta_path.is_file():
        sys.exit(f"ERROR: meta file not found: {meta_path}")

    pfx = output_prefix(meta_path)

    if args.input is None or len(args.input) == 0:
        csv_paths = [root / "data" / "hwc_final_report_updated.csv"]
    else:
        csv_paths = [Path(p).expanduser() for p in args.input]

    for inp in csv_paths:
        if not inp.is_file():
            sys.exit(f"ERROR: input CSV not found: {inp}")

    frames = [pd.read_csv(p, dtype=object) for p in csv_paths]
    df = pd.concat(frames, ignore_index=True)

    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)

    field_cat = meta.get("map_style", {}).get("category_field", "map_category")
    df = ensure_event_id_column(df)
    filter_spec = meta.get("data_binding", {}).get("final_report_csv", {}).get("filter_hwc_events", {})
    col_flag = filter_spec.get("column", "is_hwc_event")
    if col_flag not in df.columns:
        sys.exit(f"ERROR: column '{col_flag}' not in CSV")

    geo = meta.get("data_binding", {}).get("final_report_csv", {}).get("geometry", {})
    lon_c = geo.get("longitude_column", "final_lon")
    lat_c = geo.get("latitude_column", "final_lat")
    for c in (lon_c, lat_c):
        if c not in df.columns:
            sys.exit(f"ERROR: geometry column '{c}' not in CSV")

    props_cols = list(
        meta.get("data_binding", {})
        .get("final_report_csv", {})
        .get("properties_suggested", [])
    )
    props_cols = [c for c in props_cols if c in df.columns]
    if "event_type" not in props_cols and "event_type" in df.columns:
        props_cols.append("event_type")
    if "event_id" not in props_cols and "event_id" in df.columns:
        props_cols.insert(0, "event_id")

    features = []
    for _, row in df.iterrows():
        if not _truthy_hwc(row.get(col_flag)):
            continue
        if not _finite_lon_lat(row.get(lon_c), row.get(lat_c)):
            continue
        eid = str(row.get("event_id", "") or "").strip()
        lon = float(row[lon_c])
        lat = float(row[lat_c])
        et = row.get("event_type", "")
        mcat = compute_map_category(et, meta)
        props: dict = {field_cat: mcat, "event_type_raw": str(et) if et is not None else ""}
        for c in props_cols:
            v = row.get(c)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                props[c] = None
            else:
                props[c] = str(v) if not isinstance(v, (int, float, bool)) else v
        if "event_id" not in props or props.get("event_id") in (None, ""):
            props["event_id"] = eid
        feat: dict = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        }
        if eid:
            feat["id"] = eid
        features.append(feat)

    fc = {
        "type": "FeatureCollection",
        "name": Path(args.output).stem,
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": features,
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)

    n_in = len(df)
    src = ", ".join(str(p) for p in csv_paths)
    print(f"Loaded {n_in} rows from {len(csv_paths)} file(s): {src}")
    print(f"Wrote {len(features)} points -> {out_path}")

    qml_arg = args.write_qml
    if qml_arg is not None:
        qml_path = points_qml(root, pfx) if qml_arg == "__default__" else Path(qml_arg)
        write_qml_categorized(qml_path, meta, field_cat)
        print(f"Wrote QGIS style -> {qml_path} (field: {field_cat})")


if __name__ == "__main__":
    main()
