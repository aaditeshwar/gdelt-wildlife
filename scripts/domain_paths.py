"""
Path helpers: output filename prefix from meta JSON stem (first underscore segment).
Also deterministic event_id for rows missing UUIDs (migration / legacy CSVs).
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pandas as pd


def load_repo_env() -> None:
    """Load ``.env`` from the repository root (does not override existing OS env)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(repo_root() / ".env", override=False)

# Namespace for uuid5(url) when event_id is backfilled (must match migrate_add_event_ids.py)
EVENT_ID_NAMESPACE = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def output_prefix(meta_path: Path | str) -> str:
    """
    First underscore-delimited segment of the meta filename stem, lowercased.
    e.g. hwc_india_conflict_meta -> hwc
    If no underscore, first 3 characters of stem (or full stem if shorter).
    """
    stem = Path(meta_path).stem
    if "_" in stem:
        return stem.split("_")[0].lower()
    return stem[:3].lower() if len(stem) >= 3 else stem.lower()


def data_dir(root: Path | None = None) -> Path:
    r = root or repo_root()
    return r / "data"


def outputs_dir(root: Path | None = None) -> Path:
    r = root or repo_root()
    return r / "outputs"


def meta_path_default(root: Path | None = None) -> Path:
    r = root or repo_root()
    return r / "meta" / "hwc_india_conflict_meta.json"


# --- Standard pipeline filenames under data/ and outputs/ ---

def urls_csv(root: Path, prefix: str) -> Path:
    return data_dir(root) / f"{prefix}_urls.csv"


def urls_summary_txt(root: Path, prefix: str) -> Path:
    return data_dir(root) / f"{prefix}_urls_summary.txt"


def urls_enriched_csv(root: Path, prefix: str) -> Path:
    return data_dir(root) / f"{prefix}_urls_enriched.csv"


def urls_geocoded_csv(root: Path, prefix: str) -> Path:
    return data_dir(root) / f"{prefix}_urls_geocoded.csv"


def urls_high_confidence_csv(root: Path, prefix: str) -> Path:
    return data_dir(root) / f"{prefix}_urls_high_confidence.csv"


def urls_unmatched_csv(root: Path, prefix: str) -> Path:
    return data_dir(root) / f"{prefix}_urls_unmatched.csv"


def final_report_csv(root: Path, prefix: str) -> Path:
    return data_dir(root) / f"{prefix}_final_report.csv"


def final_report_updated_csv(root: Path, prefix: str) -> Path:
    return data_dir(root) / f"{prefix}_final_report_updated.csv"


def final_report_txt(root: Path, prefix: str) -> Path:
    return outputs_dir(root) / f"{prefix}_final_report.txt"


def points_geojson(root: Path, prefix: str) -> Path:
    return outputs_dir(root) / f"{prefix}_points.geojson"


def points_qml(root: Path, prefix: str) -> Path:
    """Default QML path (domain-specific naming: {prefix}_india_points.qml for HWC)."""
    return outputs_dir(root) / f"{prefix}_india_points.qml"


def deterministic_event_id(url: str) -> str:
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, url or ""))


def ensure_event_id_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure event_id column exists; fill missing with uuid5(url)."""
    out = df.copy()
    if "event_id" not in out.columns:
        out["event_id"] = ""
    for i in out.index:
        v = out.at[i, "event_id"]
        u = out.at[i, "url"] if "url" in out.columns else ""
        if (
            v is None
            or (isinstance(v, float) and pd.isna(v))
            or str(v).strip() == ""
            or str(v).strip().lower() == "nan"
        ):
            out.at[i, "event_id"] = deterministic_event_id(str(u) if u is not None else "")
    return out


def prefix_from_report_csv(path: Path) -> str:
    """Infer pipeline prefix from data/{prefix}_final_report*.csv stem."""
    s = path.stem
    for suf in ("_final_report_updated", "_final_report"):
        if s.endswith(suf):
            return s[: -len(suf)]
    parts = s.split("_")
    return parts[0] if parts else "hwc"


def urls_geocoded_for_prefix(root: Path, prefix: str) -> Path:
    return urls_geocoded_csv(root, prefix)
