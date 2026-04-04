"""
CSV → GeoJSON (domain points) + optional QGIS QML style
======================================================
Reads one or more extraction CSVs (merged in order; multiple files are de-duplicated
by ``url`` with first occurrence kept), keeps rows matching the domain filter in
``--meta`` (``data_binding.final_report_csv.filter_hwc_events``), valid lon/lat
columns from meta, and writes a Point GeoJSON.

``map_category`` is computed from ``map_style`` merge_groups using the column named by
``map_style.category_source_column`` (default ``event_type``; e.g. ``damage_cause`` for
crop damage meta).

Empty ``title`` and missing ``event_date`` / ``pub_date`` in the GeoJSON output can be
filled from the article URL and ``data/{prefix}_article_text/{event_id}.txt`` when present
(no CSV writes). Use ``--no-infer-metadata`` to disable.

Default input/output paths derive from the meta filename prefix (first ``_`` segment):
``data/{prefix}_final_report_updated.csv``, ``outputs/{prefix}_points.geojson``.

Usage:
    python scripts/convert_csv_to_geojson.py
    python scripts/convert_csv_to_geojson.py --meta meta/cropdamage_india_meta.json
    python scripts/convert_csv_to_geojson.py --input data/hwc_final_report_updated.csv --output-geojson outputs/hwc_points.geojson
    python scripts/convert_csv_to_geojson.py --input run1/a.csv run2/b.csv --output-csv data/merged.csv --output-geojson outputs/hwc_points.geojson
    python scripts/convert_csv_to_geojson.py --meta meta/hwc_india_conflict_meta.json --write-qml
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from domain_paths import (  # noqa: E402
    article_text_dir,
    ensure_event_id_column,
    final_report_updated_csv,
    meta_path_default,
    output_prefix,
    points_geojson,
    points_qml,
)


def _truthy_filter(val, true_values: list[str] | None) -> bool:
    """Row inclusion for the domain flag column; ``true_values`` from meta when set."""
    if val is True:
        return True
    if val is False:
        return False
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return False
    if true_values:
        return str(val).strip() in true_values
    s = str(val).strip().lower()
    if s in ("", "nan", "none", "false", "0", "no"):
        return False
    return s in ("true", "1", "yes")


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


def dedupe_by_url(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Keep first row per ``url``; returns (deduped_df, n_dropped)."""
    if "url" not in df.columns:
        sys.exit("ERROR: column 'url' required for de-duplication across input files")
    n_before = len(df)
    out = df.drop_duplicates(subset=["url"], keep="first", ignore_index=True)
    return out, n_before - len(out)


# --- Offline metadata inference (URL + cached article text; GeoJSON only) ---

_BOILERPLATE_LINE = re.compile(
    r"^(English Edition|Download News18 APP|Watch LIVE TV|Sign in|ADVERTISEMENT|"
    r"Download|Home|Latest|India|World|Elections|Explainers|Cricket|Cities|Movies|"
    r"Business|Lifestyle|Viral|More|Ask News18|Follow Us|TRENDING:|Rapid Read|On Google|"
    r"RECOMMENDED STORIES)\s*$",
    re.I,
)


def _is_blank_prop(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return True
    return False


def capitalize_words(s: str) -> str:
    """First character of each whitespace-delimited word uppercased; ASCII rest lowercased."""
    out: list[str] = []
    for w in s.split():
        if not w:
            continue
        tail = w[1:]
        if all(ord(c) < 128 for c in w):
            out.append(w[:1].upper() + tail.lower())
        else:
            out.append(w[:1].upper() + tail)
    return " ".join(out)


def _parse_fuzzy_date_fragment(s: str) -> date | None:
    s = s.strip()
    if not s:
        return None
    s = re.sub(r",\s*\d{1,2}:\d{2}(\s*[A-Z]{2,4})?$", "", s)
    s = s.strip().rstrip(",")
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(s[:80], fmt).date()
        except ValueError:
            continue
    m = re.search(
        r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})",
        s,
    )
    if m:
        frag = f"{m.group(1)} {m.group(2)}, {m.group(3)}"
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(frag, fmt).date()
            except ValueError:
                continue
    return None


def parse_date_from_url(url: str) -> date | None:
    if not url:
        return None
    u = url.strip()
    low = u.lower()

    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", u)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    m = re.search(r"/(\d{4})/([a-z]{3})/(\d{1,2})/", low)
    if m:
        mon = m.group(2)
        d0 = int(m.group(3))
        y = int(m.group(1))
        try:
            dt = datetime.strptime(f"{mon} {d0} {y}", "%b %d %Y")
            return dt.date()
        except ValueError:
            pass

    m = re.search(r"/(\d{8})(?:\.[a-z]+)?(?:/|$)", u, re.I)
    if m:
        s8 = m.group(1)
        try:
            y, mo, d = int(s8[0:4]), int(s8[4:6]), int(s8[6:8])
            return date(y, mo, d)
        except ValueError:
            pass

    if "business-standard.com" in low:
        m = re.search(r"1(\d{2})(\d{2})(\d{2})\d+", u)
        if m:
            yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            y = 2000 + yy if yy < 100 else yy
            try:
                return date(y, mm, dd)
            except ValueError:
                pass

    return None


def infer_date_from_text(text: str, url: str = "") -> date | None:
    if not text:
        return None
    head = text[:15000]
    for pat in (
        r"Published:\s*([^\n]+)",
        r"Last Updated:\s*\n?\s*([^\n]+)",
        r"Updated:\s*([^\n]+)",
    ):
        m = re.search(pat, head, re.I | re.MULTILINE)
        if m:
            d = _parse_fuzzy_date_fragment(m.group(1))
            if d:
                return d
    # Dateline "City, Month DD :" — year from URL when possible
    url_d = parse_date_from_url(url)
    m = re.search(
        r",\s*((?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s*:",
        head[:8000],
        re.I,
    )
    if m and url_d:
        d = _parse_fuzzy_date_fragment(f"{m.group(1)}, {url_d.year}")
        if d:
            return d
    return None


def title_from_url(url: str) -> str | None:
    if not url:
        return None
    try:
        path = urlparse(url).path or ""
        seg = path.strip("/").split("/")[-1] if path else ""
        if not seg:
            return None
        seg = seg.split("?")[0]
        seg = re.sub(r"\.[a-z0-9]{1,5}$", "", seg, flags=re.I)
        # Business Standard / similar: strip trailing story id like -120010201342_1
        seg = re.sub(r"-\d{9,}(?:_\d+)?$", "", seg, flags=re.I)
        seg = re.sub(r"_\d+_1$", "", seg, flags=re.I)
        if not seg or seg.isdigit():
            return None
        parts = []
        for x in seg.split("-"):
            if not x:
                continue
            if re.fullmatch(r"Article\d+", x, re.I):
                continue
            if x.isdigit() and len(x) >= 8:
                continue
            x = re.sub(r"\d{12,}$", "", x)
            if len(x) < 2 and x.isdigit():
                continue
            parts.append(x)
        if not parts:
            return None
        while len(parts) > 1 and (
            parts[-1].isdigit() or (len(parts[-1]) <= 4 and parts[-1].isupper())
        ):
            parts.pop()
        if not parts:
            return None
        return " ".join(parts)
    except Exception:
        return None


def _looks_like_headline(s: str) -> bool:
    if re.match(r"^(Dr|Mr|Mrs|Ms|Prof)\s", s, re.I):
        return False
    if " at Ashoka" in s or " pointed out" in s or " said that" in s:
        return False
    if s.count(",") > 4:
        return False
    if len(s) > 220:
        return False
    return True


def title_from_text(text: str) -> str | None:
    lines = [ln.strip() for ln in text.splitlines()[:80]]
    if lines:
        first = lines[0]
        if (
            25 <= len(first) <= 130
            and _looks_like_headline(first)
            and not first.startswith("📌")
        ):
            return first
    candidates: list[tuple[int, int, str]] = []
    for i, s in enumerate(lines):
        if len(s) < 25:
            continue
        if s.startswith(("- ", "• ", "📌", "* ")):
            continue
        if _BOILERPLATE_LINE.match(s):
            continue
        if "http://" in s or "https://" in s:
            continue
        if len(s) > 320:
            continue
        if len(s) < 120 and s.isupper() and " " not in s:
            continue
        if not _looks_like_headline(s):
            continue
        candidates.append((i, len(s), s))
    if not candidates:
        return None
    # Prefer first substantive line with a colon (typical headline) after nav boilerplate
    with_colon = [
        c
        for c in candidates
        if c[0] >= 8 and 35 <= c[1] <= 200 and ":" in c[2][:120]
    ]
    if with_colon:
        return min(with_colon, key=lambda x: x[0])[2]
    post = [c for c in candidates if c[0] >= 12 and c[1] >= 45]
    pick = max(post, key=lambda x: x[1]) if post else max(candidates, key=lambda x: x[1])
    return pick[2]


def enrich_geojson_properties(
    props: dict,
    url: str,
    cache_text: str | None,
    *,
    enabled: bool,
) -> None:
    """
    Fill empty ``title``, normalize ``pub_date`` from GDELT seendate, fill empty
    ``pub_date`` / ``event_date`` from URL or cached article text. Mutates ``props``.
    """
    if not enabled:
        return

    # Normalize GDELT 14-digit seendate to YYYY-MM-DD for display
    pd = props.get("pub_date")
    if not _is_blank_prop(pd):
        pds = str(pd).strip()
        if re.fullmatch(r"\d{14}", pds):
            props["pub_date"] = f"{pds[0:4]}-{pds[4:6]}-{pds[6:8]}"

    url_d = parse_date_from_url(url)
    text_d = infer_date_from_text(cache_text, url) if cache_text else None

    if _is_blank_prop(props.get("title")):
        raw = title_from_url(url)
        if not raw and cache_text:
            raw = title_from_text(cache_text)
        if raw:
            props["title"] = capitalize_words(raw)

    if _is_blank_prop(props.get("pub_date")):
        d = url_d or text_d
        if d:
            props["pub_date"] = d.isoformat()

    if _is_blank_prop(props.get("event_date")):
        d = url_d or text_d
        if d:
            props["event_date"] = d.isoformat()


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(description="Convert extraction CSV to GeoJSON points + optional QML")
    p.add_argument(
        "--input",
        nargs="*",
        default=None,
        metavar="CSV",
        help=(
            "One or more input CSVs, merged in order. "
            "Default: data/{prefix}_final_report_updated.csv where prefix comes from --meta "
            "(e.g. hwc, cropdamage, avianmortality)."
        ),
    )
    p.add_argument(
        "--meta",
        default=None,
        metavar="PATH",
        help=(
            "Domain meta JSON (filter column, geometry columns, properties, map_style). "
            f"Default: {meta_path_default(root)}"
        ),
    )
    p.add_argument(
        "--output-geojson",
        dest="output_geojson",
        default=None,
        help="Output GeoJSON path (default: outputs/{prefix}_points.geojson from --meta)",
    )
    p.add_argument(
        "--output-csv",
        dest="output_csv",
        default=None,
        metavar="PATH",
        help=(
            "Required with multiple --input files: write URL-deduplicated merged CSV "
            "(first occurrence wins, input order preserved)."
        ),
    )
    p.add_argument(
        "--write-qml",
        default=None,
        metavar="PATH",
        help="Write QGIS categorized style (default: outputs/{prefix}_india_points.qml from --meta if no path)",
        nargs="?",
        const="__default__",
    )
    p.add_argument(
        "--no-infer-metadata",
        action="store_true",
        help="Skip inferring title and dates from URL / cached article text (GeoJSON only)",
    )
    args = p.parse_args()

    meta_path = Path(args.meta).expanduser() if args.meta else meta_path_default(root)
    if not meta_path.is_file():
        sys.exit(f"ERROR: meta file not found: {meta_path}")

    pfx = output_prefix(meta_path)

    if args.input is None or len(args.input) == 0:
        csv_paths = [final_report_updated_csv(root, pfx)]
    else:
        csv_paths = [Path(p).expanduser() for p in args.input]

    output_geojson = args.output_geojson
    if output_geojson is None:
        output_geojson = str(points_geojson(root, pfx))

    for inp in csv_paths:
        if not inp.is_file():
            sys.exit(f"ERROR: input CSV not found: {inp}")

    multi_input = len(csv_paths) > 1
    if multi_input and not args.output_csv:
        p.error("--output-csv is required when multiple input CSVs are given")
    if not multi_input and args.output_csv:
        p.error("--output-csv is only used when multiple input CSVs are given")

    frames = [pd.read_csv(p, dtype=object) for p in csv_paths]
    df = pd.concat(frames, ignore_index=True)
    if multi_input:
        df, n_dup = dedupe_by_url(df)
        if n_dup:
            print(f"De-duplicated by url: dropped {n_dup} duplicate row(s) (keep first)")

    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)

    print(f"Meta: {meta_path} (prefix={pfx})", flush=True)

    field_cat = meta.get("map_style", {}).get("category_field", "map_category")
    category_src = meta.get("map_style", {}).get("category_source_column", "event_type")
    raw_prop_key = (
        "event_type_raw" if category_src == "event_type" else f"{category_src}_raw"
    )
    df = ensure_event_id_column(df)
    if multi_input:
        out_csv = Path(args.output_csv).expanduser()
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False, encoding="utf-8")
        print(f"Wrote merged de-duplicated CSV -> {out_csv}")
    filter_spec = meta.get("data_binding", {}).get("final_report_csv", {}).get("filter_hwc_events", {})
    col_flag = filter_spec.get("column", "is_hwc_event")
    true_vals = filter_spec.get("true_values")
    if true_vals is not None and not isinstance(true_vals, list):
        true_vals = None
    if col_flag not in df.columns:
        sys.exit(f"ERROR: column '{col_flag}' not in CSV")

    geo = meta.get("data_binding", {}).get("final_report_csv", {}).get("geometry", {})
    lon_c = geo.get("longitude_column", "final_lon")
    lat_c = geo.get("latitude_column", "final_lat")
    for c in (lon_c, lat_c):
        if c not in df.columns:
            sys.exit(f"ERROR: geometry column '{c}' not in CSV")
    if category_src not in df.columns:
        sys.exit(
            f"ERROR: map_style.category_source_column '{category_src}' not in CSV "
            "(needed for map_category / QML)"
        )

    props_cols = list(
        meta.get("data_binding", {})
        .get("final_report_csv", {})
        .get("properties_suggested", [])
    )
    props_cols = [c for c in props_cols if c in df.columns]
    if category_src not in props_cols and category_src in df.columns:
        props_cols.append(category_src)
    if "event_id" not in props_cols and "event_id" in df.columns:
        props_cols.insert(0, "event_id")

    infer_meta = not args.no_infer_metadata
    article_text_root = article_text_dir(root, pfx)

    features = []
    for _, row in df.iterrows():
        if not _truthy_filter(row.get(col_flag), true_vals):
            continue
        if not _finite_lon_lat(row.get(lon_c), row.get(lat_c)):
            continue
        eid = str(row.get("event_id", "") or "").strip()
        lon = float(row[lon_c])
        lat = float(row[lat_c])
        et = row.get(category_src, "")
        mcat = compute_map_category(et, meta)
        raw_s = str(et) if et is not None else ""
        props: dict = {field_cat: mcat, raw_prop_key: raw_s}
        for c in props_cols:
            v = row.get(c)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                props[c] = None
            else:
                props[c] = str(v) if not isinstance(v, (int, float, bool)) else v
        if "event_id" not in props or props.get("event_id") in (None, ""):
            props["event_id"] = eid
        url_s = str(row.get("url", "") or "")
        cache_text: str | None = None
        if infer_meta and eid:
            _ctp = article_text_root / f"{eid}.txt"
            if _ctp.is_file():
                try:
                    cache_text = _ctp.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    cache_text = None
        enrich_geojson_properties(props, url_s, cache_text, enabled=infer_meta)
        feat: dict = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        }
        if eid:
            feat["id"] = eid
        features.append(feat)

    fc_name = meta.get("domain", {}).get("id") or Path(output_geojson).stem
    fc = {
        "type": "FeatureCollection",
        "name": fc_name,
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": features,
    }

    out_path = Path(output_geojson).expanduser()
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
