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
        ("OPTIMIZE_HTML", "Optimize HTML", "Clean + prettify archived HTML (off by default — changes the bytes)", False),
        ("OPTIMIZE_IMAGES", "Optimize images", "Recompress JPEG/PNG to save space", False),
        ("MINIFY_JS", "Minify JS", "Shrink JavaScript files", False),
        ("MINIFY_CSS", "Minify CSS", "Shrink CSS files", False),
    ]),
    ("Shim options", [
        ("USE_PLAYWRIGHT", "Render HTML via headless Chromium",
         "Requires the wayback-archive:playwright image variant. Slower but captures SPA/JS-rendered content.",
         False),
    ]),
    ("Content removal", [
        ("REMOVE_TRACKERS", "Remove trackers", "Strip analytics/tracking scripts", True),
        ("REMOVE_ADS", "Remove ads", "Strip common ad markup", True),
        ("REMOVE_CLICKABLE_CONTACTS", "Disable tel/mailto links", "Keep text, strip click handler", True),
        ("REMOVE_EXTERNAL_IFRAMES", "Remove external iframes", "Drop iframes pointing off-site", False),
    ]),
    ("Link rewriting", [
        ("MAKE_INTERNAL_LINKS_RELATIVE", "Make internal links relative", "Rewrite absolute → relative paths", True),
        ("KEEP_REDIRECTIONS", "Keep HTTP redirections", "Don't flatten 301/302 chains", False),
    ]),
]

# Radio groups: (group_title, help, param_name_for_form, options=[(flag_to_set_true, label, help, is_default)])
# The special flag value "" means "neither" (all listed flags stay false).
RADIO_GROUPS = [
    ("External links", "What to do with off-site <a> tags", "external_links", [
        ("REMOVE_EXTERNAL_LINKS_KEEP_ANCHORS", "Neutralize, keep anchor text", "Strip href but leave <a> text in place", True),
        ("REMOVE_EXTERNAL_LINKS_REMOVE_ANCHORS", "Remove entirely", "Drop the <a> tag and its text", False),
        ("", "Leave as-is", "Preserve the original external link", False),
    ]),
    ("Host canonicalization", "Unify www. and non-www. forms", "www_mode", [
        ("MAKE_NON_WWW", "Force non-www", "Rewrite www.example.com → example.com", False),
        ("MAKE_WWW", "Force www", "Rewrite example.com → www.example.com", False),
        ("", "Leave as-is", "Keep whichever form was archived (default — preserves byte faithfulness)", True),
    ]),
]

RADIO_FLAGS = {flag for _, _, _, opts in RADIO_GROUPS for flag, _, _, _ in opts if flag}

# Integer-valued flags rendered as <input type=number> instead of a checkbox.
# Schema: (key, label, help, min, max, default_blank)
NUMBER_FLAGS = [
    ("FETCH_WORKERS", "Parallel asset prefetch",
     "How many background threads speculatively fetch the next few URLs. "
     "1 (default) keeps the current sequential behavior; higher values amortize "
     "network RTT at the cost of Wayback rate-limit pressure.",
     1, 16, ""),
]


def _flatten_flags():
    out = [(f, default) for _, items in FLAG_GROUPS for (f, _, _, default) in items]
    for _, _, _, opts in RADIO_GROUPS:
        for flag, _, _, is_default in opts:
            if flag:
                out.append((flag, is_default))
    return out


FLAG_DEFAULTS = _flatten_flags()


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "flag_groups": FLAG_GROUPS,
        "radio_groups": RADIO_GROUPS,
        "number_flags": NUMBER_FLAGS,
        "max_concurrent": jobs.get_max_concurrent(),
    })


def _sort_from_cookie(request: Request, name: str, default: tuple[str, str]) -> tuple[str, str]:
    raw = request.cookies.get(f"sort_{name}")
    if not raw or ":" not in raw:
        return default
    col, _, d = raw.partition(":")
    return col, d


def _split_csv(v: str) -> list[str]:
    return [x for x in (p.strip() for p in (v or "").split(",")) if x]


def _parse_filter_cookie(raw: str) -> dict:
    out = {}
    for kv in (raw or "").split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            out[k.strip()] = v.strip()
    return out


@router.get("/jobs/list", response_class=HTMLResponse)
async def jobs_list(request: Request, page: int = 1, per_page: int = 0,
                    status: str = "", sort: str = "", dir: str = "",
                    statuses: str = "", types: str = ""):
    page = max(1, page)
    explicit_sort = bool(sort or dir)
    if not sort or not dir:
        csort, cdir = _sort_from_cookie(request, "jobs", ("id", "desc"))
        sort = sort or csort
        dir = dir or cdir
    if sort not in jobs.JOB_SORT_COLS:
        sort = "id"
    if dir not in ("asc", "desc"):
        dir = "desc"

    # Starlette query params — accept both ?statuses=a,b and ?statuses=a&statuses=b
    qs_statuses: list[str] = []
    for v in request.query_params.getlist("statuses"):
        qs_statuses.extend(_split_csv(v))
    qs_types: list[str] = []
    for v in request.query_params.getlist("types"):
        qs_types.extend(_split_csv(v))
    # `_filter=1` is emitted by the toolbar form whenever any filter input
    # changes. Its presence means "trust the submitted values" — including
    # empty ones (user ticked all off). Otherwise fall back to cookie.
    submitted_filter = request.query_params.get("_filter") == "1"
    explicit_filter = submitted_filter or bool(
        request.query_params.getlist("statuses") or
        request.query_params.getlist("types") or
        request.query_params.get("per_page")
    )

    cookie = _parse_filter_cookie(request.cookies.get("filter_jobs") or "")
    if not submitted_filter:
        if not qs_statuses:
            qs_statuses = _split_csv(cookie.get("statuses", ""))
        if not qs_types:
            qs_types = _split_csv(cookie.get("types", ""))
    if per_page <= 0:
        try:
            per_page = int(cookie.get("per_page") or 25)
        except ValueError:
            per_page = 25
    per_page = max(5, min(per_page, 100000))

    legacy_status = status or None
    if legacy_status and not qs_statuses:
        qs_statuses = [legacy_status]

    total = jobs.count_jobs(statuses=qs_statuses, types=qs_types)
    pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, pages)
    rows = jobs.list_jobs(limit=per_page, offset=(page - 1) * per_page,
                          sort=sort, dir=dir,
                          statuses=qs_statuses, types=qs_types)
    import json as _json
    from .. import job_progress
    progress = {}
    for r in rows:
        if r["status"] != "running":
            continue
        try:
            mf = _json.loads(r["flags_json"] or "{}").get("MAX_FILES")
            mf = int(mf) if mf and str(mf).isdigit() else None
        except Exception:
            mf = None
        p = job_progress.read_progress(r["log_path"], mf)
        if p is not None:
            progress[r["id"]] = p
    resp = templates.TemplateResponse("_jobs_list.html", {
        "request": request, "jobs": rows, "page": page, "pages": pages,
        "per_page": per_page, "total": total,
        "selected_statuses": qs_statuses, "selected_types": qs_types,
        "sort": sort, "dir": dir, "progress": progress,
    })
    if explicit_sort:
        resp.set_cookie("sort_jobs", f"{sort}:{dir}", max_age=60 * 60 * 24 * 365,
                        samesite="lax")
    if explicit_filter:
        val = f"statuses={','.join(qs_statuses)};types={','.join(qs_types)};per_page={per_page}"
        resp.set_cookie("filter_jobs", val, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@router.post("/settings/max-concurrent")
async def set_max_concurrent(request: Request):
    form = await request.form()
    try:
        n = max(1, min(20, int(form.get("max_concurrent") or 3)))
    except ValueError:
        n = 3
    jobs.set_setting("max_concurrent", str(n))
    return RedirectResponse("/", status_code=303)


@router.post("/jobs/bulk-action")
async def jobs_bulk_action(request: Request):
    form = await request.form()
    action = form.get("action", "")
    ids = [int(v) for v in form.getlist("job_id") if str(v).isdigit()]
    if action == "delete":
        jobs.delete_many(ids)
    else:
        jobs.cancel_many(ids)
    resp = RedirectResponse("/", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


def _default_flags() -> dict:
    out = {key: ("true" if default else "false") for key, default in FLAG_DEFAULTS}
    return out


def _collect_flags(form: dict, *, submitted_form: bool = True) -> dict:
    """Build flag dict from form. If submitted_form=False, return defaults only."""
    if not submitted_form:
        return _default_flags()
    out = {}
    for key, default in FLAG_DEFAULTS:
        if key in RADIO_FLAGS:
            out[key] = "false"
        else:
            out[key] = "true" if form.get(key) else "false"
    for _, _, param, _ in RADIO_GROUPS:
        chosen = form.get(param) or ""
        if chosen and chosen in RADIO_FLAGS:
            out[chosen] = "true"
    mf = form.get("MAX_FILES")
    if mf and str(mf).strip().isdigit():
        out["MAX_FILES"] = str(mf).strip()
    for key, _, _, lo, hi, _ in NUMBER_FLAGS:
        v = (form.get(key) or "").strip()
        if v and v.isdigit():
            n = max(lo, min(hi, int(v)))
            out[key] = str(n)
    return out


GRANULARITY = {"year": 4, "month": 6, "day": 8, "every": 14}


@router.post("/jobs/bulk")
async def create_bulk(request: Request):
    form = dict(await request.form())
    target = form.get("target_url", "").strip()
    if not target:
        raise HTTPException(400, "target_url required")
    if "://" not in target:
        target = "https://" + target
    gran = form.get("granularity", "year")
    digits = GRANULARITY.get(gran, 4)
    fy = form.get("from_year")
    ty = form.get("to_year")
    try:
        cap = int(form.get("max_count") or 50)
    except ValueError:
        cap = 50
    cap = max(1, min(cap, 500))
    try:
        snaps = wayback.list_snapshots(
            target,
            from_year=int(fy) if fy and fy.isdigit() else None,
            to_year=int(ty) if ty and ty.isdigit() else None,
            limit=cap,
            collapse_digits=digits,
        )
    except Exception as e:
        raise HTTPException(502, f"CDX error: {e}")
    flags = _default_flags()
    count = 0
    for s in snaps[:cap]:
        try:
            jobs.enqueue(target, s["timestamp"], flags)
            count += 1
        except Exception:
            continue
    return RedirectResponse(f"/?bulk={count}", status_code=303)


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
    try:
        jobs.enqueue(target, ts, flags)
    except Exception as e:
        raise HTTPException(502, f"could not enqueue: {e}")
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
