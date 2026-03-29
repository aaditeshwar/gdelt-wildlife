"""Runtime config (see README).

Loads ``server/.env`` via python-dotenv before reading ``os.environ``. Variables
already set in the process environment take precedence over ``.env``
(``override=False``). The ``Settings`` instance is built once at import time;
restart Uvicorn after editing ``.env``.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_SERVER_DIR = Path(__file__).resolve().parent
load_dotenv(_SERVER_DIR / ".env", override=False)


def _listen_port() -> int:
    """HTTP port for Uvicorn: ``PORT`` first (common on PaaS), then ``UVICORN_PORT``, else 8000."""
    for key in ("PORT", "UVICORN_PORT"):
        raw = os.environ.get(key)
        if raw is not None and str(raw).strip() != "":
            try:
                return int(raw)
            except ValueError:
                pass
    return 8000


class Settings:
    def __init__(self) -> None:
        self.repo_root = Path(
            os.environ.get("REPO_ROOT", Path(__file__).resolve().parent.parent)
        ).resolve()
        self.session_secret = os.environ.get("SESSION_SECRET", "dev-change-me")
        self.git_auto_commit = os.environ.get("GIT_AUTO_COMMIT", "0").strip() in (
            "1",
            "true",
            "yes",
        )
        self.host = os.environ.get("HOST", "127.0.0.1").strip() or "127.0.0.1"
        self.port = _listen_port()
        self.moderators_path = self.repo_root / "data" / "moderators.json"
        self.edits_pending_dir = self.repo_root / "data" / "edits" / "pending"
        self.edit_log_path = self.repo_root / "data" / "edit_log.jsonl"
        self.meta_dir = self.repo_root / "meta"
        self.outputs_dir = self.repo_root / "outputs"
        self.frontend_dist = self.repo_root / "frontend" / "dist"


settings = Settings()
