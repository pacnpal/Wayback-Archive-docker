"""Scheduled recurring archives."""
from __future__ import annotations
import json
from pathlib import Path

from datetime import datetime, timezone

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from croniter import croniter

from .. import jobs
from ..scheduler import compute_next


def _simple_to_cron(mode: str, minute: str, time: str,
                    dows: list[str], dom: str) -> str | None:
    try:
        m_n = max(1, min(59, int(minute or 15)))
        d_n = max(1, min(31, int(dom or 1)))
    except ValueError:
        m_n, d_n = 15, 1
    t = (time or "03:00").split(":")
    try:
        hh, mm = int(t[0]), int(t[1] if len(t) > 1 else 0)
    except ValueError:
        hh, mm = 3, 0
    hh, mm = max(0, min(23, hh)), max(0, min(59, mm))
    if mode == "every-n":
        return f"*/{m_n} * * * *"
    if mode == "hourly":
        return f"{m_n} * * * *"
    if mode == "daily":
        return f"{mm} {hh} * * *"
    if mode == "weekly":
        days = sorted({int(d) for d in (dows or []) if d.isdigit()}) or [1]
        return f"{mm} {hh} * * {','.join(str(d) for d in days)}"
    if mode == "monthly":
        return f"{mm} {hh} {d_n} * *"
    return None

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


SCHED_SORT_COLS = {
    "id": "id", "url": "target_url", "cron": "cron_expr",
    "enabled": "enabled", "next": "next_run_at", "last": "last_run_at",
}


@router.get("/schedules", response_class=HTMLResponse)
async def list_schedules(request: Request, sort: str = "", dir: str = ""):
    explicit = bool(sort or dir)
    if not sort or not dir:
        raw = request.cookies.get("sort_schedules") or ""
        cs, _, cd = raw.partition(":")
        sort = sort or cs or "id"
        dir = dir or cd or "desc"
    col = SCHED_SORT_COLS.get(sort, "id")
    if dir not in ("asc", "desc"):
        dir = "desc"
    direction = "ASC" if dir == "asc" else "DESC"
    with jobs.connect() as c:
        rows = c.execute(
            f"SELECT * FROM schedules ORDER BY {col} {direction}, id DESC"
        ).fetchall()
    server_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    resp = templates.TemplateResponse("schedules.html", {
        "request": request, "schedules": rows, "sort": sort, "dir": dir,
        "server_time_utc": server_time_utc,
    })
    if explicit:
        resp.set_cookie("sort_schedules", f"{sort}:{dir}",
                        max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@router.post("/schedules")
async def create(request: Request,
                 target_url: str = Form(...),
                 cron_expr: str = Form("")):
    cron_expr = (cron_expr or "").strip()
    if not cron_expr or not croniter.is_valid(cron_expr):
        # Fall back to Simple-form fields (no-JS path).
        form = await request.form()
        rebuilt = _simple_to_cron(
            (form.get("mode") or "daily").strip(),
            form.get("minute") or "",
            form.get("time") or "",
            form.getlist("dow"),
            form.get("dom") or "",
        )
        # Advanced-tab raw field
        if (not rebuilt or not croniter.is_valid(rebuilt)) and form.get("cron_expr_raw"):
            cron_expr_raw = (form.get("cron_expr_raw") or "").strip()
            if croniter.is_valid(cron_expr_raw):
                rebuilt = cron_expr_raw
        if rebuilt and croniter.is_valid(rebuilt):
            cron_expr = rebuilt
    if not croniter.is_valid(cron_expr):
        raise HTTPException(400, "invalid cron expression")
    t = target_url.strip()
    if "://" not in t:
        t = "http://" + t
    nxt = compute_next(cron_expr)
    with jobs.connect() as c:
        c.execute(
            """INSERT INTO schedules (target_url, cron_expr, flags_json, enabled, next_run_at, created_at)
               VALUES (?, ?, '{}', 1, ?, ?)""",
            (t, cron_expr.strip(), nxt, jobs.now_iso()),
        )
    resp = RedirectResponse("/schedules", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.post("/schedules/{sid}/toggle")
async def toggle(sid: int):
    with jobs.connect() as c:
        c.execute("UPDATE schedules SET enabled = 1 - enabled WHERE id=?", (sid,))
    resp = RedirectResponse("/schedules", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.post("/schedules/{sid}/delete")
async def delete(sid: int):
    with jobs.connect() as c:
        c.execute("DELETE FROM schedules WHERE id=?", (sid,))
    resp = RedirectResponse("/schedules", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.post("/schedules/{sid}/run-now")
async def run_now(sid: int):
    with jobs.connect() as c:
        s = c.execute("SELECT * FROM schedules WHERE id=?", (sid,)).fetchone()
    if not s:
        raise HTTPException(404)
    jid = jobs.enqueue(s["target_url"], None, json.loads(s["flags_json"]), schedule_id=sid)
    with jobs.connect() as c:
        c.execute("UPDATE schedules SET last_run_at=?, last_job_id=? WHERE id=?",
                  (jobs.now_iso(), jid, sid))
    resp = RedirectResponse("/schedules", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp
