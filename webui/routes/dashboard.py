"""Dashboard + jobs routes."""
from __future__ import annotations
from pathlib import Path

from fastapi import APIRouter, Form, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

from .. import jobs, wayback

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# (flag, label, help, default)
FLAG_GROUPS = [
    ("Optimization", [
        ("OPTIMIZE_HTML", "Optimize HTML", "Clean + prettify archived HTML", True),
        ("OPTIMIZE_IMAGES", "Optimize images", "Recompress JPEG/PNG to save space", False),
        ("MINIFY_JS", "Minify JS", "Shrink JavaScript files", False),
        ("MINIFY_CSS", "Minify CSS", "Shrink CSS files", False),
    ]),
    ("Content removal", [
        ("REMOVE_TRACKERS", "Remove trackers", "Strip analytics/tracking scripts", True),
        ("REMOVE_ADS", "Remove ads", "Strip common ad markup", True),
        ("REMOVE_CLICKABLE_CONTACTS", "Disable tel/mailto links", "Keep text, strip click handler", True),
        ("REMOVE_EXTERNAL_IFRAMES", "Remove external iframes", "Drop iframes pointing off-site", False),
    ]),
    ("External links", [
        ("REMOVE_EXTERNAL_LINKS_KEEP_ANCHORS", "Neutralize external links (keep anchors)", "Make off-site links non-clickable but keep <a> text", True),
        ("REMOVE_EXTERNAL_LINKS_REMOVE_ANCHORS", "Remove external links entirely", "Strip the <a> tags outright", False),
    ]),
    ("Link rewriting", [
        ("MAKE_INTERNAL_LINKS_RELATIVE", "Make internal links relative", "Rewrite absolute → relative paths", True),
        ("MAKE_NON_WWW", "Force non-www canonical", "Treat www. and bare host as one", True),
        ("MAKE_WWW", "Force www canonical", "Opposite of above", False),
        ("KEEP_REDIRECTIONS", "Keep HTTP redirections", "Don't flatten 301/302 chains", False),
    ]),
]

# Mutually-exclusive groups: checking one disables the others.
EXCLUSIVE_PAIRS = [
    ("REMOVE_EXTERNAL_LINKS_KEEP_ANCHORS", "REMOVE_EXTERNAL_LINKS_REMOVE_ANCHORS"),
    ("MAKE_NON_WWW", "MAKE_WWW"),
]


def _flatten_flags():
    return [(f, default) for _, items in FLAG_GROUPS for (f, _, _, default) in items]


FLAG_DEFAULTS = _flatten_flags()


def _exclusive_map() -> dict[str, str]:
    m: dict[str, list[str]] = {}
    for a, b in EXCLUSIVE_PAIRS:
        m.setdefault(a, []).append(b)
        m.setdefault(b, []).append(a)
    return {k: ",".join(v) for k, v in m.items()}


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "flag_groups": FLAG_GROUPS,
        "exclusive_map": _exclusive_map(),
    })


@router.get("/jobs/list", response_class=HTMLResponse)
async def jobs_list(request: Request):
    return templates.TemplateResponse("_jobs_list.html", {"request": request, "jobs": jobs.list_jobs()})


def _collect_flags(form: dict) -> dict:
    out = {}
    for key, default in FLAG_DEFAULTS:
        out[key] = "true" if form.get(key) else "false"
    # Enforce mutual exclusion server-side (first of pair wins if both submitted)
    for a, b in EXCLUSIVE_PAIRS:
        if out.get(a) == "true" and out.get(b) == "true":
            out[b] = "false"
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
    if "://" not in target:
        target = "https://" + target
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
