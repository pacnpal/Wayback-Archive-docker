"""Per-site (host) overview: indexed local snapshots + opt-in remote CDX."""
from __future__ import annotations
from pathlib import Path
from collections import defaultdict

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .. import jobs, wayback, sites_index, link_rewrite, asset_audit

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_KEYS = {"ts", "size", "files"}


def _local_hosts() -> list[tuple[str, int, str]]:
    root = jobs.OUTPUT_ROOT
    if not root.exists():
        return []
    out = []
    for h in sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")):
        snaps = sorted(
            (s.name for s in h.iterdir()
             if s.is_dir() and sites_index.is_snapshot_ts(s.name)),
            reverse=True,
        )
        if snaps:
            out.append((h.name, len(snaps), snaps[0]))
    return out


HOSTS_SORT_KEYS = {"host", "count", "newest"}


@router.get("/sites", response_class=HTMLResponse)
async def sites_index_route(request: Request, sort: str = "", dir: str = ""):
    explicit = bool(sort or dir)
    if not sort or not dir:
        raw = request.cookies.get("sort_sites") or ""
        c, _, d = raw.partition(":")
        sort = sort or c or "host"
        dir = dir or d or "asc"
    if sort not in HOSTS_SORT_KEYS:
        sort = "host"
    if dir not in ("asc", "desc"):
        dir = "asc"
    reverse = (dir == "desc")
    hosts = _local_hosts()
    key_map = {
        "host": lambda t: t[0],
        "count": lambda t: t[1],
        "newest": lambda t: t[2],
    }
    hosts.sort(key=key_map[sort], reverse=reverse)
    resp = templates.TemplateResponse("sites_index.html", {
        "request": request, "hosts": hosts, "sort": sort, "dir": dir,
    })
    if explicit:
        resp.set_cookie("sort_sites", f"{sort}:{dir}",
                        max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@router.get("/sites/{host}", response_class=HTMLResponse)
async def site_detail(request: Request, host: str,
                      sort: str = "", dir: str = "",
                      page: int = 1, per_page: int = 50,
                      remote: int = 0, from_year: str = "", to_year: str = ""):
    explicit = bool(sort or dir)
    if not sort or not dir:
        raw = request.cookies.get("sort_site_detail") or ""
        c, _, d = raw.partition(":")
        sort = sort or c or "ts"
        dir = dir or d or "desc"
    idx = sites_index.get_index(host)
    if sort not in SORT_KEYS:
        sort = "ts"
    if dir not in ("asc", "desc"):
        dir = "desc"
    reverse = (dir == "desc")
    key_map = {
        "ts": lambda kv: kv[0],
        "size": lambda kv: kv[1].get("size_bytes", 0),
        "files": lambda kv: kv[1].get("file_count", 0),
    }
    rows = sorted(idx.items(), key=key_map[sort], reverse=reverse)
    total = len(rows)
    per_page = max(1, min(per_page, 100000))
    pages = max(1, (total + per_page - 1) // per_page) if total else 1
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    rows_page = rows[start:start + per_page]

    remote_snaps: list[dict] = []
    remote_error: str | None = None
    by_day: dict[str, list[dict]] = defaultdict(list)
    days_sorted: list[str] = []
    if remote:
        target_url = f"https://{host}"
        try:
            remote_snaps = wayback.list_snapshots(
                target_url,
                from_year=int(from_year) if from_year.isdigit() else None,
                to_year=int(to_year) if to_year.isdigit() else None,
                limit=10000,
                collapse_digits=14,
            )
        except wayback.WaybackUnreachable as e:
            remote_error = str(e)
        except Exception as e:
            remote_error = f"CDX error: {e}"
        for s in remote_snaps:
            ts = s["timestamp"]
            by_day[f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"].append(s)
        days_sorted = sorted(by_day.keys(), reverse=True)

    audit_map = {}
    for ts, _meta in rows_page:
        try:
            a = asset_audit.get_audit(jobs.OUTPUT_ROOT / host / ts)
            total = a["total_refs"]
            missing_n = len(a["missing"])
            pct = 100 if total == 0 else int((total - missing_n) * 100 / total)
            audit_map[ts] = {"total": total, "missing": missing_n, "percent": pct}
        except Exception:
            audit_map[ts] = None

    resp = templates.TemplateResponse("site_detail.html", {
        "request": request,
        "host": host,
        "rows": rows_page,
        "total": total,
        "sort": sort, "dir": dir,
        "page": page, "pages": pages, "per_page": per_page,
        "remote": int(remote),
        "remote_snaps": remote_snaps,
        "remote_error": remote_error,
        "by_day": by_day,
        "days_sorted": days_sorted,
        "from_year": from_year,
        "to_year": to_year,
        "audit_map": audit_map,
    })
    if explicit:
        resp.set_cookie("sort_site_detail", f"{sort}:{dir}",
                        max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@router.post("/sites/{host}/rewrite-links")
async def rewrite_links(host: str, ts: str = ""):
    """Rewrite absolute-path URLs inside archived HTML/CSS to relative paths,
    so pages render correctly when served from /sites/{host}/view. Applies to
    a single snapshot if `ts` is given, otherwise every snapshot of the host."""
    host_dir = jobs.OUTPUT_ROOT / host
    if not host_dir.is_dir():
        return RedirectResponse(f"/sites/{host}", status_code=303)
    targets = [host_dir / ts] if ts else [p for p in host_dir.iterdir() if p.is_dir()]
    totals = {"snapshots": 0, "files_scanned": 0, "files_changed": 0, "refs_rewritten": 0}
    for snap in targets:
        if not snap.is_dir():
            continue
        r = link_rewrite.rewrite_snapshot(snap)
        totals["snapshots"] += 1
        totals["files_scanned"] += r["files_scanned"]
        totals["files_changed"] += r["files_changed"]
        totals["refs_rewritten"] += r["refs_rewritten"]
    from .. import log as _log
    _log.get("rewrite").info(
        "rewrite host=%s snapshots=%d files_changed=%d refs=%d",
        host, totals["snapshots"], totals["files_changed"], totals["refs_rewritten"],
    )
    qs = "&".join(f"{k}={v}" for k, v in totals.items())
    return RedirectResponse(f"/sites/{host}?rewrite_done=1&{qs}", status_code=303)


@router.post("/sites/{host}/audit")
async def audit_snapshots(host: str, ts: str = ""):
    host_dir = jobs.OUTPUT_ROOT / host
    if not host_dir.is_dir():
        return RedirectResponse(f"/sites/{host}", status_code=303)
    targets = [ts] if ts else [
        p.name for p in host_dir.iterdir()
        if p.is_dir() and sites_index.is_snapshot_ts(p.name)
    ]
    for t in targets:
        asset_audit.get_audit(host_dir / t, force=True)
    return RedirectResponse(f"/sites/{host}", status_code=303)


@router.get("/sites/{host}/audit/{ts}", response_class=HTMLResponse)
async def audit_details(request: Request, host: str, ts: str):
    path = jobs.OUTPUT_ROOT / host / ts
    data = asset_audit.get_audit(path)
    return templates.TemplateResponse("audit_details.html", {
        "request": request, "host": host, "ts": ts,
        "total": data["total_refs"], "present": data["present"],
        "missing": data["missing"],
    })


@router.post("/sites/{host}/repair")
async def repair_snapshot(host: str, ts: str = Form(...)):
    path = jobs.OUTPUT_ROOT / host / ts
    data = asset_audit.get_audit(path)
    rel_paths = [m["rel"] for m in data["missing"]]
    if rel_paths:
        jobs.enqueue_repair(host, ts, rel_paths)
    return RedirectResponse(f"/sites/{host}", status_code=303)


@router.post("/sites/{host}/archive")
async def archive_one(host: str, request: Request):
    form = await request.form()
    ts = (form.get("timestamp") or "").strip() or None
    from .dashboard import _default_flags
    jobs.enqueue(f"https://{host}", ts, _default_flags())
    return RedirectResponse(f"/sites/{host}", status_code=303)
