"""Moderator list + bcrypt verification."""

from __future__ import annotations

import json
from pathlib import Path

import bcrypt


def load_users(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return list(data.get("users") or [])


def verify_password(plain: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(
            plain.encode("utf-8"),
            password_hash.encode("utf-8"),
        )
    except (ValueError, TypeError):
        return False


def find_user(users: list[dict], username: str) -> dict | None:
    for u in users:
        if (u.get("username") or "") == username:
            return u
    return None
