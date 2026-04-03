"""FastAPI: layers, GeoJSON, styles, suggested edits, moderator apply."""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.sessions import SessionMiddleware

from server.auth_util import find_user, load_users, verify_password
from server.geojson_edit import merge_properties
from server.git_auto import try_auto_commit
from server.layer_util import (
    geojson_file_for_layer,
    list_layer_descriptors,
    meta_summary_for_layer,
    qml_file_for_layer,
    style_payload_for_layer,
)
from server.settings import settings

SESSION_USER_KEY = "moderator_username"

app = FastAPI(title="GDELT wildlife map API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    same_site="lax",
    https_only=False,
)

_login_buckets: dict[str, list[float]] = {}
_LOGIN_WINDOW_SEC = 60.0
_LOGIN_MAX = 5


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _rate_limit_login(ip: str) -> None:
    now = time.time()
    bucket = _login_buckets.setdefault(ip, [])
    bucket[:] = [t for t in bucket if now - t < _LOGIN_WINDOW_SEC]
    if len(bucket) >= _LOGIN_MAX:
        raise HTTPException(status_code=429, detail="too many login attempts")
    bucket.append(now)


def require_moderator(request: Request) -> str:
    u = request.session.get(SESSION_USER_KEY)
    if not u:
        raise HTTPException(status_code=401, detail="not authenticated")
    return str(u)


class LoginBody(BaseModel):
    username: str
    password: str


class EditCreateBody(BaseModel):
    point_id: str = Field(..., min_length=1)
    layer_id: str = Field(..., min_length=1)
    suggested_properties: dict = Field(default_factory=dict)
    note: str | None = None


@app.get("/api/meta/layers")
def api_layers():
    return list_layer_descriptors(settings.meta_dir, settings.outputs_dir)


@app.get("/api/layers/{layer_id}/geojson")
def api_layer_geojson(layer_id: str):
    path = geojson_file_for_layer(settings.repo_root, layer_id)
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/api/layers/{layer_id}/style")
def api_layer_style(layer_id: str):
    return style_payload_for_layer(settings.repo_root, layer_id)


@app.get("/api/layers/{layer_id}/qml")
def api_layer_qml(layer_id: str):
    path = qml_file_for_layer(settings.repo_root, layer_id)
    return FileResponse(path, media_type="application/xml", filename=path.name)


@app.get("/api/layers/{layer_id}/meta-summary")
def api_layer_meta_summary(layer_id: str):
    return meta_summary_for_layer(settings.repo_root, layer_id)


@app.post("/api/edits")
def api_create_edit(body: EditCreateBody):
    geojson_file_for_layer(settings.repo_root, body.layer_id)
    edit_id = str(uuid.uuid4())
    settings.edits_pending_dir.mkdir(parents=True, exist_ok=True)
    rec = {
        "edit_id": edit_id,
        "point_id": body.point_id,
        "layer_id": body.layer_id,
        "suggested_properties": body.suggested_properties,
        "note": body.note,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    out = settings.edits_pending_dir / f"{edit_id}.json"
    out.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"edit_id": edit_id, "status": "pending"}


@app.post("/api/auth/login")
def api_login(request: Request, body: LoginBody):
    _rate_limit_login(_client_ip(request))
    users = load_users(settings.moderators_path)
    u = find_user(users, body.username)
    if not u or not verify_password(body.password, u.get("password_hash") or ""):
        raise HTTPException(status_code=401, detail="invalid credentials")
    request.session[SESSION_USER_KEY] = body.username
    return {"ok": True, "username": body.username}


@app.post("/api/auth/logout")
def api_logout(request: Request):
    request.session.clear()
    return {"ok": True}


@app.get("/api/auth/me")
def api_me(request: Request):
    u = request.session.get(SESSION_USER_KEY)
    if not u:
        return {"authenticated": False}
    return {"authenticated": True, "username": u}


@app.get("/api/moderation/edits")
def api_list_edits(request: Request):
    require_moderator(request)
    pending = settings.edits_pending_dir
    if not pending.is_dir():
        return []
    out: list[dict] = []
    for p in sorted(pending.glob("*.json")):
        try:
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            continue
    return out


@app.post("/api/moderation/edits/{edit_id}/apply")
def api_apply_edit(request: Request, edit_id: str):
    require_moderator(request)
    path = settings.edits_pending_dir / f"{edit_id}.json"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="edit not found")
    rec = json.loads(path.read_text(encoding="utf-8"))
    layer_id = rec.get("layer_id") or ""
    point_id = rec.get("point_id") or ""
    suggested = rec.get("suggested_properties") or {}
    if not layer_id or not point_id:
        raise HTTPException(status_code=400, detail="invalid edit record")

    gj_path = geojson_file_for_layer(settings.repo_root, layer_id)
    before = merge_properties(gj_path, point_id, suggested)

    log_line = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": "apply",
        "edit_id": edit_id,
        "point_id": point_id,
        "layer_id": layer_id,
        "moderator": request.session.get(SESSION_USER_KEY),
        "previous_properties": before,
        "applied_properties": suggested,
    }
    settings.edit_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(settings.edit_log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_line, ensure_ascii=False) + "\n")

    path.unlink(missing_ok=True)

    if settings.git_auto_commit:
        ok, msg = try_auto_commit(
            settings.repo_root,
            f"Apply edit {edit_id} for point {point_id} on {layer_id}",
        )
        log_line["git"] = {"ok": ok, "message": msg}

    return {"ok": True, "edit_id": edit_id, "log": log_line}


@app.delete("/api/moderation/edits/{edit_id}")
def api_delete_edit(request: Request, edit_id: str):
    require_moderator(request)
    path = settings.edits_pending_dir / f"{edit_id}.json"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="edit not found")
    path.unlink()
    return {"ok": True}


# Static SPA (after `npm run build` in frontend/)
_dist = settings.frontend_dist
if _dist.is_dir() and any(_dist.iterdir()):
    app.mount("/", StaticFiles(directory=str(_dist), html=True), name="spa")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server.main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
    )
