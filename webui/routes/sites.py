"""Per-site (host) overview: indexed local snapshots + opt-in remote CDX."""
from __future__ import annotations
from pathlib import Path
from collections import defaultdict

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from urllib.parse import quote as _urlquote

from .. import jobs, wayback, sites_index, link_rewrite, asset_audit, cleanup_orphans, search as _search
from ..safe_path import safe_output_child
from ._validators import valid_host, valid_ts, valid_ts_optional


def _qhost(host: str) -> str:
    """URL-encode a validated host for use in redirect paths. `host` is
    already regex-clean so this is a no-op in practice, but the `quote` call
    is what CodeQL recognizes as a url-redirection sanitizer."""
    return _urlquote(host, safe="")

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_KEYS = {"ts", "size", "files", "downloaded"}


def _local_hosts() -> list[tuple[str, int, str]]:
    root = jobs.OUTPUT_ROOT
    if not root.exists():
        return []
    out = []
    for h in sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith((".", "_"))):
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
async def sites_index_route(request: Request, sort: str = "", dir: str = "",
                            page: int = 1, per_page: int = 0):
    explicit_sort = bool(sort or dir)
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

    explicit_filter = bool(request.query_params.get("per_page"))
    cookie_pp = None
    raw_f = request.cookies.get("filter_sites") or ""
    for kv in raw_f.split(";"):
        if kv.startswith("per_page="):
            try: cookie_pp = int(kv.split("=", 1)[1])
            except ValueError: pass
    if per_page <= 0:
        per_page = cookie_pp or 50
    per_page = max(1, min(per_page, 100000))

    hosts = _local_hosts()
    key_map = {
        "host": lambda t: t[0],
        "count": lambda t: t[1],
        "newest": lambda t: t[2],
    }
    hosts.sort(key=key_map[sort], reverse=reverse)
    total = len(hosts)
    pages = max(1, (total + per_page - 1) // per_page) if total else 1
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    slice_ = hosts[start:start + per_page]
    resp = templates.TemplateResponse(request, "sites_index.html", {
        "request": request, "hosts": slice_, "sort": sort, "dir": dir,
        "page": page, "pages": pages, "per_page": per_page, "total": total,
    })
    if explicit_sort:
        resp.set_cookie("sort_sites", f"{sort}:{dir}",
                        max_age=60 * 60 * 24 * 365, samesite="lax")
    if explicit_filter:
        resp.set_cookie("filter_sites", f"per_page={per_page}",
                        max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@router.get("/sites/{host}", response_class=HTMLResponse)
async def site_detail(request: Request, host: str,
                      sort: str = "", dir: str = "",
                      page: int = 1, per_page: int = 50,
                      remote: int = 0, from_year: str = "", to_year: str = ""):
    host = valid_host(host)
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
        "downloaded": lambda kv: kv[1].get("mtime") or "",
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
            a = asset_audit.get_audit(safe_output_child(host, ts))
            refs_total = a["total_refs"]
            missing_n = len(a["missing"])
            pct = 100 if refs_total == 0 else int((refs_total - missing_n) * 100 / refs_total)
            audit_map[ts] = {"total": refs_total, "missing": missing_n, "percent": pct}
        except Exception:
            audit_map[ts] = None

    resp = templates.TemplateResponse(request, "site_detail.html", {
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
    host = valid_host(host)
    ts = valid_ts_optional(ts)
    host_dir = safe_output_child(host)
    if not host_dir.is_dir():
        resp = RedirectResponse(f"/sites/{_qhost(host)}", status_code=303)
        resp.headers["HX-Trigger"] = "jobs-changed"
        return resp
    if ts:
        targets = [host_dir / ts]
    else:
        targets = [p for p in host_dir.iterdir()
                   if p.is_dir() and not p.name.startswith((".", "_"))]
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
    resp = RedirectResponse(f"/sites/{_qhost(host)}?rewrite_done=1&{qs}", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.post("/sites/{host}/audit")
async def audit_snapshots(host: str, ts: str = ""):
    host = valid_host(host)
    ts = valid_ts_optional(ts)
    host_dir = safe_output_child(host)
    if not host_dir.is_dir():
        resp = RedirectResponse(f"/sites/{_qhost(host)}", status_code=303)
        resp.headers["HX-Trigger"] = "jobs-changed"
        return resp
    targets = [ts] if ts else [
        p.name for p in host_dir.iterdir()
        if p.is_dir() and sites_index.is_snapshot_ts(p.name)
    ]
    for t in targets:
        asset_audit.get_audit(host_dir / t, force=True)
    resp = RedirectResponse(f"/sites/{_qhost(host)}", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.get("/sites/{host}/audit/{ts}", response_class=HTMLResponse)
async def audit_details(request: Request, host: str, ts: str):
    host = valid_host(host)
    ts = valid_ts(ts)
    path = safe_output_child(host, ts)
    data = asset_audit.get_audit(path)
    return templates.TemplateResponse(request, "audit_details.html", {
        "request": request, "host": host, "ts": ts,
        "total": data["total_refs"], "present": data["present"],
        "missing": data["missing"],
    })


@router.post("/sites/{host}/recover-missing")
async def recover_missing(host: str):
    """Queue a repair job per snapshot with every currently-missing ref."""
    host = valid_host(host)
    host_dir = safe_output_child(host)
    if not host_dir.is_dir():
        resp = RedirectResponse(f"/sites/{_qhost(host)}", status_code=303)
        resp.headers["HX-Trigger"] = "jobs-changed"
        return resp
    queued = 0
    total_paths = 0
    for snap in host_dir.iterdir():
        if not snap.is_dir() or not sites_index.is_snapshot_ts(snap.name):
            continue
        data = asset_audit.get_audit(snap)
        rel_paths = [m["rel"] for m in data.get("missing", [])]
        if not rel_paths:
            continue
        jobs.enqueue_repair(host, snap.name, rel_paths)
        queued += 1
        total_paths += len(rel_paths)
    from .. import log as _log
    _log.get("recover").info(
        "bulk recover-missing host=%s snapshots=%d paths=%d",
        host, queued, total_paths,
    )
    resp = RedirectResponse(
        f"/sites/{_qhost(host)}?recover_done=1&snapshots={queued}&paths={total_paths}",
        status_code=303,
    )
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.get("/sites/{host}/search", response_class=HTMLResponse)
async def search(request: Request, host: str, ts: str = "", q: str = ""):
    """TF-IDF full-text search over a snapshot's HTML. Replacement for the
    archived site's 1990s search CGI (qfind.exe / vtopic.exe / etc)."""
    host = valid_host(host)
    ts = valid_ts(ts) if ts else ""
    hits: list[dict] = []
    n_docs = 0
    if ts:
        snap = safe_output_child(host, ts)
        if snap.is_dir():
            idx = _search.get_index(snap)
            n_docs = idx.get("n_docs", 0)
            if q.strip():
                hits = _search.query(idx, q.strip())
    return templates.TemplateResponse(request, "search.html", {
        "request": request, "host": host, "ts": ts, "q": q,
        "hits": hits, "n_docs": n_docs,
    })


@router.post("/sites/{host}/build-search-index")
async def build_search_index(host: str):
    """Build/refresh the .search.json index for every snapshot of the host."""
    host = valid_host(host)
    host_dir = safe_output_child(host)
    if not host_dir.is_dir():
        resp = RedirectResponse(f"/sites/{_qhost(host)}", status_code=303)
        resp.headers["HX-Trigger"] = "jobs-changed"
        return resp
    indexed = 0
    total_docs = 0
    for snap in host_dir.iterdir():
        if not snap.is_dir() or not sites_index.is_snapshot_ts(snap.name):
            continue
        idx = _search.get_index(snap, force=True)
        indexed += 1
        total_docs += idx.get("n_docs", 0)
    from .. import log as _log
    _log.get("search").info(
        "search index host=%s snapshots=%d docs=%d",
        host, indexed, total_docs,
    )
    resp = RedirectResponse(
        f"/sites/{_qhost(host)}?search_done=1&snapshots={indexed}&docs={total_docs}",
        status_code=303,
    )
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.post("/sites/{host}/repair")
async def repair_snapshot(host: str, ts: str = Form(...)):
    host = valid_host(host)
    ts = valid_ts(ts)
    path = safe_output_child(host, ts)
    data = asset_audit.get_audit(path)
    rel_paths = [m["rel"] for m in data["missing"]]
    if rel_paths:
        jobs.enqueue_repair(host, ts, rel_paths)
    resp = RedirectResponse(f"/sites/{_qhost(host)}", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.post("/sites/{host}/archive")
async def archive_one(host: str, request: Request):
    host = valid_host(host)
    form = await request.form()
    ts = (form.get("timestamp") or "").strip() or None
    from .dashboard import _default_flags
    jobs.enqueue(f"https://{host}", ts, _default_flags())
    resp = RedirectResponse(f"/sites/{_qhost(host)}", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.post("/sites/{host}/recover-imagemaps")
async def recover_imagemaps(host: str):
    """Sweep every `.map` under the host, and for any that currently hold
    Wayback's HTML error page, consult CDX for an alt-timestamp capture
    whose body is real NCSA map text. Silently skips files whose local
    copy already parses as plausible map text."""
    host = valid_host(host)
    host_dir = safe_output_child(host)
    if not host_dir.is_dir():
        resp = RedirectResponse(f"/sites/{_qhost(host)}", status_code=303)
        resp.headers["HX-Trigger"] = "jobs-changed"
        return resp
    from .. import imagemap
    recovered = 0
    attempted = 0
    for map_path in host_dir.rglob("*.map"):
        if not map_path.is_file():
            continue
        try:
            body = map_path.read_bytes()
        except OSError:
            continue
        if imagemap.is_plausible_map_text(body):
            continue  # already good, skip CDX
        attempted += 1
        # Derive ts from snapshot-folder name (path structure: host/ts/…).
        try:
            ts = map_path.relative_to(host_dir).parts[0]
        except Exception:
            continue
        result = imagemap.recover_map(map_path, host, ts)
        if result is not None:
            recovered += 1
    from .. import log as _log
    _log.get("imagemap").info(
        "imagemap recovery host=%s attempted=%d recovered=%d",
        host, attempted, recovered,
    )
    resp = RedirectResponse(
        f"/sites/{_qhost(host)}?imagemaps_done=1"
        f"&attempted={attempted}&recovered={recovered}",
        status_code=303,
    )
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.post("/sites/cleanup-orphans")
async def cleanup_all_orphans():
    """Quarantine stray files/dirs at OUTPUT_ROOT and under each host dir into
    a `_orphaned/` folder. Addresses the leak-to-mount-root bug that older
    crawler runs left behind (see wayback_resume_shim sandbox guard)."""
    summary = cleanup_orphans.cleanup_all(jobs.OUTPUT_ROOT)
    from .. import log as _log
    _log.get("cleanup").info("orphans quarantined total=%d", summary["total"])
    resp = RedirectResponse(
        f"/sites?cleanup_done=1&moved={summary['total']}", status_code=303
    )
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


@router.post("/sites/{host}/cleanup-orphans")
async def cleanup_host_orphans(host: str):
    """Quarantine non-snapshot entries under a single host dir."""
    host = valid_host(host)
    host_dir = safe_output_child(host)
    summary = cleanup_orphans.cleanup_host(host_dir)
    resp = RedirectResponse(
        f"/sites/{_qhost(host)}?cleanup_done=1&moved={summary['count']}", status_code=303
    )
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp


_GRAN_DIGITS = {"year": 4, "month": 6, "day": 8, "every": 14}


@router.post("/sites/{host}/archive-range")
async def archive_range(host: str, request: Request):
    host = valid_host(host)
    form = await request.form()
    from_d = (form.get("from_date") or "").strip()
    to_d = (form.get("to_date") or "").strip()
    gran = (form.get("granularity") or "month").strip()
    try:
        cap = int(form.get("max_count") or 100)
    except ValueError:
        cap = 100
    cap = max(1, min(cap, 500))
    digits = _GRAN_DIGITS.get(gran, 6)

    def _yyyymmdd(d: str) -> str | None:
        # d is YYYY-MM-DD from <input type=date>; CDX wants YYYYMMDD[HHMMSS].
        return d.replace("-", "") if d and len(d) == 10 and d[4] == "-" else None

    try:
        from_ts = _yyyymmdd(from_d)
        to_ts = _yyyymmdd(to_d)
        snaps = wayback.list_snapshots(
            f"https://{host}",
            from_year=int(from_ts[:4]) if from_ts else None,
            to_year=int(to_ts[:4]) if to_ts else None,
            limit=cap * 2,
            collapse_digits=digits,
        )
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(502, f"CDX: {e}")

    # CDX from/to give year-granular bounds; narrow by day precisely here.
    def _in_range(ts: str) -> bool:
        if from_ts and ts[:len(from_ts)] < from_ts:
            return False
        if to_ts and ts[:len(to_ts)] > to_ts:
            return False
        return True

    snaps = [s for s in snaps if _in_range(s["timestamp"])][:cap]
    from .dashboard import _default_flags
    flags = _default_flags()
    for s in snaps:
        try:
            jobs.enqueue(f"https://{host}", s["timestamp"], flags)
        except Exception:
            pass
    resp = RedirectResponse(f"/sites/{_qhost(host)}", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed"
    return resp
