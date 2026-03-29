"""
One-off migration: add deterministic event_id (uuid5 per url) to existing pipeline CSVs
and patch GeoJSON features (properties + Feature id).

Same namespace as domain_paths.deterministic_event_id / ensure_event_id_column.

Usage (repo root):
  python scripts/migrate_add_event_ids.py --dry-run
  python scripts/migrate_add_event_ids.py --backup
  python scripts/migrate_add_event_ids.py --csv data/hwc_urls.csv data/hwc_final_report.csv
  python scripts/migrate_add_event_ids.py --geojson outputs/hwc_points.geojson
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from domain_paths import (  # noqa: E402
    data_dir,
    deterministic_event_id,
    ensure_event_id_column,
    outputs_dir,
    repo_root,
)


def _default_csv_candidates(root: Path) -> list[Path]:
    data = data_dir(root)
    patterns = (
        "*_urls.csv",
        "*_urls_enriched.csv",
        "*_urls_geocoded.csv",
        "*_urls_high_confidence.csv",
        "*_urls_unmatched.csv",
        "*_final_report.csv",
        "*_final_report_updated.csv",
    )
    out: list[Path] = []
    for pat in patterns:
        out.extend(sorted(data.glob(pat)))
    # de-dupe, stable
    seen: set[Path] = set()
    uniq: list[Path] = []
    for p in out:
        rp = p.resolve()
        if rp not in seen:
            seen.add(rp)
            uniq.append(p)
    return uniq


def migrate_csv(path: Path, *, dry_run: bool, backup: bool) -> bool:
    if not path.is_file():
        print(f"  skip (missing): {path}")
        return False
    df = pd.read_csv(path, dtype=object)
    col_before = (
        [str(x) for x in df["event_id"].tolist()]
        if "event_id" in df.columns
        else None
    )
    out = ensure_event_id_column(df)
    col_after = [str(x) for x in out["event_id"].tolist()]
    if col_before is not None and col_before == col_after:
        print(f"  unchanged: {path.name}")
        return False
    if dry_run:
        print(f"  [dry-run] would write: {path} ({len(out)} rows)")
        return True
    if backup:
        bak = path.with_suffix(path.suffix + ".bak")
        shutil.copy2(path, bak)
        print(f"  backup -> {bak.name}")
    out.to_csv(path, index=False)
    print(f"  wrote: {path}")
    return True


def migrate_geojson(path: Path, *, dry_run: bool, backup: bool) -> bool:
    if not path.is_file():
        print(f"  skip (missing): {path}")
        return False
    raw = path.read_text(encoding="utf-8")
    fc = json.loads(raw)
    if fc.get("type") != "FeatureCollection":
        print(f"  skip (not FeatureCollection): {path}")
        return False
    feats = fc.get("features") or []
    changed = False
    for feat in feats:
        if feat.get("type") != "Feature":
            continue
        props = feat.setdefault("properties", {})
        url = props.get("url") or ""
        eid = deterministic_event_id(str(url) if url is not None else "")
        old = props.get("event_id")
        if old != eid:
            changed = True
        props["event_id"] = eid
        feat["id"] = eid
    if not changed:
        print(f"  unchanged: {path.name}")
        return False
    if dry_run:
        print(f"  [dry-run] would write: {path}")
        return True
    if backup:
        bak = path.with_suffix(path.suffix + ".bak")
        shutil.copy2(path, bak)
        print(f"  backup -> {bak.name}")
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  wrote: {path}")
    return True


def main() -> None:
    root = repo_root()
    p = argparse.ArgumentParser(description="Add deterministic event_id to CSVs / GeoJSON")
    p.add_argument(
        "--root",
        type=Path,
        default=root,
        help="Repository root (default: auto)",
    )
    p.add_argument(
        "--csv",
        nargs="*",
        default=None,
        help="Explicit CSV paths under data/. If omitted, migrates default pipeline globs.",
    )
    p.add_argument(
        "--geojson",
        nargs="*",
        default=None,
        help="GeoJSON files to patch (e.g. outputs/hwc_points.geojson). If omitted, patches outputs/*_points.geojson",
    )
    p.add_argument("--dry-run", action="store_true", help="Print actions only")
    p.add_argument("--backup", action="store_true", help="Copy .bak before overwrite")
    args = p.parse_args()
    root = Path(args.root).resolve()

    csvs = (
        [Path(c) for c in args.csv]
        if args.csv is not None and len(args.csv) > 0
        else _default_csv_candidates(root)
    )
    geojsons = (
        [Path(g) for g in args.geojson]
        if args.geojson is not None and len(args.geojson) > 0
        else sorted(outputs_dir(root).glob("*_points.geojson"))
    )

    ts = datetime.now(timezone.utc).isoformat()
    print(f"migrate_add_event_ids @ {ts}")
    print(f"root: {root}")
    print("CSVs:")
    any_csv = False
    for c in csvs:
        any_csv |= migrate_csv(c.resolve(), dry_run=args.dry_run, backup=args.backup)
    if not csvs:
        print("  (none)")
    print("GeoJSON:")
    any_gj = False
    for g in geojsons:
        any_gj |= migrate_geojson(g.resolve(), dry_run=args.dry_run, backup=args.backup)
    if not geojsons:
        print("  (none)")
    if args.dry_run and (any_csv or any_gj):
        print("Dry run complete — remove --dry-run to apply.")
    elif not any_csv and not any_gj:
        print("Nothing to change.")


if __name__ == "__main__":
    main()
