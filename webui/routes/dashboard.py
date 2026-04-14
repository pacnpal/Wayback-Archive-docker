"""Dashboard + jobs routes."""
from __future__ import annotations
from pathlib import Path

from fastapi import APIRouter, Form, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

from .. import jobs, wayback

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

FLAG_DEFAULTS = [
    ("OPTIMIZE_HTML", True), ("OPTIMIZE_IMAGES", False),
    ("MINIFY_JS", False), ("MINIFY_CSS", False),
    ("REMOVE_TRACKERS", True), ("REMOVE_ADS", True),
    ("REMOVE_CLICKABLE_CONTACTS", True), ("REMOVE_EXTERNAL_IFRAMES", False),
    ("REMOVE_EXTERNAL_LINKS_KEEP_ANCHORS", True),
    ("REMOVE_EXTERNAL_LINKS_REMOVE_ANCHORS", False),
    ("MAKE_INTERNAL_LINKS_RELATIVE", True),
    ("MAKE_NON_WWW", True), ("MAKE_WWW", False),
    ("KEEP_REDIRECTIONS", False),
]


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request, "flag_defaults": FLAG_DEFAULTS})


@router.get("/jobs/list", response_class=HTMLResponse)
async def jobs_list(request: Request):
    return templates.TemplateResponse("_jobs_list.html", {"request": request, "jobs": jobs.list_jobs()})


def _collect_flags(form: dict) -> dict:
    out = {}
    for key, default in FLAG_DEFAULTS:
        out[key] = "true" if form.get(key) else "false"
    mf = form.get("MAX_FILES")
    if mf and str(mf).strip().isdigit():
        out["MAX_FILES"] = str(mf).strip()
    return out


@router.post("/jobs")
async def create_job(request: Request):
    form = dict(await request.form())
    target = form.get("target_url", "").strip()
    if not target:
        raise HTTPException(400, "target_url required")
    ts = (form.get("timestamp") or "").strip() or None
    flags = _collect_flags(form)
    jobs.enqueue(target, ts, flags)
    return RedirectResponse("/", status_code=303)


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_detail(request: Request, job_id: int):
    job = jobs.get_job(job_id)
    if not job:
        raise HTTPException(404)
    return templates.TemplateResponse("job_detail.html", {"request": request, "job": job})


@router.get("/jobs/{job_id}/log", response_class=PlainTextResponse)
async def job_log(job_id: int):
    job = jobs.get_job(job_id)
    if not job:
        raise HTTPException(404)
    p = Path(job["log_path"])
    if not p.exists():
        return ""
    data = p.read_bytes()[-20000:]
    return data.decode("utf-8", errors="replace")


@router.post("/jobs/{job_id}/cancel")
async def cancel(job_id: int):
    jobs.cancel_job(job_id)
    return RedirectResponse("/", status_code=303)


@router.get("/api/snapshots", response_class=HTMLResponse)
async def api_snapshots(request: Request, target_url: str = ""):
    target_url = target_url.strip()
    if not target_url:
        return templates.TemplateResponse("_snapshots.html", {"request": request, "snaps": [], "error": "Enter a URL first"})
    try:
        snaps = wayback.list_snapshots(target_url)
    except Exception as e:
        return templates.TemplateResponse("_snapshots.html", {"request": request, "snaps": [], "error": f"CDX error: {e}"})
    return templates.TemplateResponse("_snapshots.html", {"request": request, "snaps": snaps, "error": None})
