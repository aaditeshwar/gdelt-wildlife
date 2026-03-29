"""Optional git add/commit after GeoJSON edits."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


def try_auto_commit(repo: Path, message: str) -> tuple[bool, str]:
    if not (repo / ".git").is_dir():
        return False, "not a git repository"
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    try:
        subprocess.run(
            ["git", "-C", str(repo), "add", "-A"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        subprocess.run(
            ["git", "-C", str(repo), "commit", "-m", message],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        return True, "committed"
    except subprocess.CalledProcessError as e:
        err = (e.stderr or e.stdout or str(e))[:500]
        return False, err
