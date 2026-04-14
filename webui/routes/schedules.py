"""Scheduled recurring archives."""
from __future__ import annotations
import json
from pathlib import Path

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from croniter import croniter

from .. import jobs
from ..scheduler import compute_next

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/schedules", response_class=HTMLResponse)
async def list_schedules(request: Request):
    with jobs.connect() as c:
        rows = c.execute("SELECT * FROM schedules ORDER BY id DESC").fetchall()
    return templates.TemplateResponse("schedules.html", {"request": request, "schedules": rows})


@router.post("/schedules")
async def create(target_url: str = Form(...), cron_expr: str = Form(...)):
    if not croniter.is_valid(cron_expr):
        raise HTTPException(400, "invalid cron expression")
    nxt = compute_next(cron_expr)
    with jobs.connect() as c:
        c.execute(
            """INSERT INTO schedules (target_url, cron_expr, flags_json, enabled, next_run_at, created_at)
               VALUES (?, ?, '{}', 1, ?, ?)""",
            (target_url.strip(), cron_expr.strip(), nxt, jobs.now_iso()),
        )
    return RedirectResponse("/schedules", status_code=303)


@router.post("/schedules/{sid}/toggle")
async def toggle(sid: int):
    with jobs.connect() as c:
        c.execute("UPDATE schedules SET enabled = 1 - enabled WHERE id=?", (sid,))
    return RedirectResponse("/schedules", status_code=303)


@router.post("/schedules/{sid}/delete")
async def delete(sid: int):
    with jobs.connect() as c:
        c.execute("DELETE FROM schedules WHERE id=?", (sid,))
    return RedirectResponse("/schedules", status_code=303)


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
    return RedirectResponse("/schedules", status_code=303)
