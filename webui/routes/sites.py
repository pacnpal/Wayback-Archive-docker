"""Per-site (host) overview: indexed local snapshots + opt-in remote CDX."""
from __future__ import annotations
from pathlib import Path
from collections import defaultdict

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .. import jobs, wayback, sites_index

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

SORT_KEYS = {"ts", "size", "files"}


def _local_hosts() -> list[tuple[str, int, str]]:
    root = jobs.OUTPUT_ROOT
    if not root.exists():
        return []
    out = []
    for h in sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")):
        snaps = sorted([s.name for s in h.iterdir() if s.is_dir()], reverse=True)
        if snaps:
            out.append((h.name, len(snaps), snaps[0]))
    return out


@router.get("/sites", response_class=HTMLResponse)
async def sites_index_route(request: Request):
    return templates.TemplateResponse("sites_index.html", {
        "request": request, "hosts": _local_hosts(),
    })


@router.get("/sites/{host}", response_class=HTMLResponse)
async def site_detail(request: Request, host: str,
                      sort: str = "ts", dir: str = "desc",
                      page: int = 1, per_page: int = 50,
                      remote: int = 0, from_year: str = "", to_year: str = ""):
    idx = sites_index.get_index(host)
    # Normalise sort + direction.
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

    return templates.TemplateResponse("site_detail.html", {
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
    })


@router.post("/sites/{host}/archive")
async def archive_one(host: str, request: Request):
    form = await request.form()
    ts = (form.get("timestamp") or "").strip() or None
    from .dashboard import _default_flags
    jobs.enqueue(f"https://{host}", ts, _default_flags())
    return RedirectResponse(f"/sites/{host}", status_code=303)
