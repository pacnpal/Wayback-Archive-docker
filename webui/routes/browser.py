"""Browse + edit archived sites."""
from __future__ import annotations
from pathlib import Path

import shutil
from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates

from .. import jobs
from ._validators import valid_host, valid_ts

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

TEXT_EXTS = {".html", ".htm", ".css", ".js", ".mjs", ".json", ".xml", ".svg", ".txt", ".md"}
MODE_MAP = {".html": "htmlmixed", ".htm": "htmlmixed", ".css": "css",
            ".js": "javascript", ".mjs": "javascript", ".json": "javascript",
            ".xml": "xml", ".svg": "xml", ".txt": "text", ".md": "text"}


def _host_dir(host: str) -> Path:
    base = jobs.OUTPUT_ROOT.resolve()
    d = (jobs.OUTPUT_ROOT / host).resolve()
    if not d.is_dir() or not d.is_relative_to(base):
        raise HTTPException(404)
    return d


def _safe_path(base: Path, rel: str) -> Path:
    root = base.resolve()
    p = (base / rel).resolve()
    if not p.is_relative_to(root):
        raise HTTPException(400, "path escape")
    return p


SNAP_SORT_KEYS = {"host", "ts", "size", "files", "downloaded"}


def _all_snapshots() -> list[dict]:
    """Return [{host, ts, size_bytes, file_count, mtime}] for every snapshot."""
    from .. import sites_index
    out: list[dict] = []
    root = jobs.OUTPUT_ROOT
    if not root.exists():
        return out
    for h in (p for p in root.iterdir() if p.is_dir() and not p.name.startswith((".", "_"))):
        idx = sites_index.get_index(h.name)
        for ts, meta in idx.items():
            if not sites_index.is_snapshot_ts(ts):
                continue
            out.append({"host": h.name, "ts": ts,
                        "size_bytes": meta.get("size_bytes", 0),
                        "file_count": meta.get("file_count", 0),
                        "mtime": meta.get("mtime") or ""})
    return out


@router.get("/snapshots", response_class=HTMLResponse)
async def sites(request: Request, page: int = 1, per_page: int = 0, host: str = "",
                sort: str = "", dir: str = "", completed_only: int = -1):
    if host:
        host = valid_host(host)
    explicit_sort = bool(sort or dir)
    if not sort or not dir:
        raw = request.cookies.get("sort_snapshots") or ""
        c, _, d = raw.partition(":")
        sort = sort or c or "ts"
        dir = dir or d or "desc"
    if sort not in SNAP_SORT_KEYS:
        sort = "ts"
    if dir not in ("asc", "desc"):
        dir = "desc"
    reverse = (dir == "desc")

    # Multi-host + per-page with cookie persistence.
    qs_hosts: list[str] = []
    for v in request.query_params.getlist("hosts"):
        qs_hosts.extend([x for x in (p.strip() for p in v.split(",")) if x])
    submitted_filter = request.query_params.get("_filter") == "1"
    explicit_filter = submitted_filter or bool(
        request.query_params.getlist("hosts") or
        request.query_params.get("per_page") or
        request.query_params.get("completed_only")
    )
    cookie_raw = request.cookies.get("filter_snapshots") or ""
    cookie = {}
    for kv in cookie_raw.split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            cookie[k.strip()] = v.strip()
    if not qs_hosts and not submitted_filter:
        qs_hosts = [x for x in (p.strip() for p in cookie.get("hosts", "").split(",")) if x]
    if per_page <= 0:
        try:
            per_page = int(cookie.get("per_page") or 50)
        except ValueError:
            per_page = 50
    if completed_only < 0:
        try:
            completed_only = int(cookie.get("completed_only") or 0)
        except ValueError:
            completed_only = 0
    if host and host not in qs_hosts:  # back-compat single-value ?host=
        qs_hosts = [host]

    items = _all_snapshots()
    if qs_hosts:
        items = [i for i in items if i["host"] in qs_hosts]
    if completed_only:
        # Drop snapshots that correspond to a pending or running job row.
        with jobs.connect() as c:
            in_flight = {(r["host"], r["timestamp"]) for r in c.execute(
                "SELECT host, timestamp FROM jobs WHERE status IN ('pending','running')"
            ).fetchall()}
        items = [i for i in items if (i["host"], i["ts"]) not in in_flight]
    key_map = {
        "host": lambda r: (r["host"], r["ts"]),
        "ts": lambda r: (r["ts"], r["host"]),
        "size": lambda r: r["size_bytes"],
        "files": lambda r: r["file_count"],
        "downloaded": lambda r: r["mtime"],
    }
    items.sort(key=key_map[sort], reverse=reverse)
    total = len(items)
    per_page = max(1, min(per_page, 100000))
    pages = max(1, (total + per_page - 1) // per_page) if total else 1
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    slice_ = items[start:start + per_page]
    hosts_all = sorted({r["host"] for r in _all_snapshots()})
    resp = templates.TemplateResponse(request, "snapshots.html", {
        "request": request, "items": slice_, "page": page, "pages": pages,
        "per_page": per_page, "total": total,
        "selected_hosts": qs_hosts, "hosts_all": hosts_all,
        "sort": sort, "dir": dir, "completed_only": completed_only,
    })
    if explicit_sort:
        resp.set_cookie("sort_snapshots", f"{sort}:{dir}",
                        max_age=60 * 60 * 24 * 365, samesite="lax")
    if explicit_filter:
        val = f"hosts={','.join(qs_hosts)};per_page={per_page};completed_only={completed_only}"
        resp.set_cookie("filter_snapshots", val,
                        max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


def _delete_snapshot(host: str, ts: str) -> bool:
    base = jobs.OUTPUT_ROOT.resolve()
    target = (jobs.OUTPUT_ROOT / host / ts).resolve()
    if not target.is_relative_to(base) or not target.is_dir():
        return False
    from .. import log as _log
    _log.get("snapshots").info("delete snapshot host=%s ts=%s", host, ts)
    shutil.rmtree(target)
    try:
        from .. import sites_index
        sites_index.drop_entry(host, ts)
    except Exception:
        pass
    parent = target.parent
    if parent.is_dir() and not any(parent.iterdir()):
        parent.rmdir()
    return True


@router.post("/snapshots/bulk-action")
async def sites_bulk_action(request: Request):
    form = await request.form()
    for entry in form.getlist("snapshot"):
        if "/" not in entry:
            continue
        h, t = entry.split("/", 1)
        _delete_snapshot(h, t)
    return RedirectResponse("/snapshots", status_code=303)


def _delete_host(host: str) -> dict:
    """Wipe a host from disk and drop its job rows. Safe against path escape."""
    base = jobs.OUTPUT_ROOT.resolve()
    target = (jobs.OUTPUT_ROOT / host).resolve()
    removed_snapshots = 0
    if target.is_relative_to(base) and target.is_dir():
        removed_snapshots = sum(1 for p in target.iterdir() if p.is_dir())
        shutil.rmtree(target)
    jobs_removed = jobs.delete_jobs_for_host(host)
    from .. import log as _log
    _log.get("sites").info(
        "delete host=%s snapshots=%d jobs=%d", host, removed_snapshots, jobs_removed,
    )
    return {"snapshots_removed": removed_snapshots, "jobs_removed": jobs_removed}


@router.post("/sites/{host}/delete-all")
async def delete_host(host: str):
    host = valid_host(host)
    _delete_host(host)
    resp = RedirectResponse("/sites", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed, sites-changed"
    return resp


@router.post("/sites/bulk-delete")
async def sites_bulk_delete(request: Request):
    form = await request.form()
    for h in form.getlist("host"):
        h = (h or "").strip()
        if h:
            _delete_host(valid_host(h))
    resp = RedirectResponse("/sites", status_code=303)
    resp.headers["HX-Trigger"] = "jobs-changed, sites-changed"
    return resp


@router.get("/sites/{host}/tree", response_class=HTMLResponse)
async def tree(request: Request, host: str, ts: str, path: str = ""):
    host = valid_host(host)
    ts = valid_ts(ts)
    base = _host_dir(host) / ts
    if not base.is_dir():
        raise HTTPException(404)
    cur = _safe_path(base, path) if path else base
    if not cur.is_dir():
        raise HTTPException(404)
    entries = []
    for p in sorted(cur.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
        # Hide quarantine folders — they're visible only via their explicit routes.
        if p.is_dir() and p.name == "_orphaned":
            continue
        rel = str(p.relative_to(base))
        is_dir = p.is_dir()
        entries.append({
            "name": p.name, "rel": rel, "is_dir": is_dir,
            "size": p.stat().st_size if p.is_file() else 0,
            "is_text": (not is_dir) and (p.suffix.lower() in TEXT_EXTS),
        })
    parent = None
    if path:
        parent_rel = str(Path(path).parent)
        parent = "" if parent_rel == "." else parent_rel
    return templates.TemplateResponse(request, "tree.html", {
        "request": request, "host": host, "ts": ts, "entries": entries, "parent": parent,
    })


@router.get("/sites/{host}/view")
async def view(host: str, ts: str, path: str = "index.html"):
    # Legacy query-string viewer; redirect into the path-based one so
    # rewritten relative refs resolve correctly.
    host = valid_host(host)
    ts = valid_ts(ts)
    from urllib.parse import quote as _urlquote
    return RedirectResponse(
        f"/sites/{_urlquote(host, safe='')}/view/{_urlquote(ts, safe='')}/{path}",
        status_code=302,
    )


_IMAGEMAP_FALLBACK_HTML = (
    "<!DOCTYPE html><html><head><title>Imagemap unavailable</title>"
    "<style>body{font:14px/1.5 system-ui,sans-serif;max-width:40em;margin:3em auto;padding:0 1em;color:#444}"
    "h1{font-size:1.2em;margin:0 0 .5em} a{color:#2563eb}</style>"
    "</head><body><h1>Imagemap not available in this capture</h1>"
    "<p>The server-side imagemap file for this 1990s-era page was never captured "
    "as plain text — Wayback only has the CGI's HTML error page. Click coordinates "
    "cannot be resolved.</p>"
    "<p><a href=\"javascript:history.back()\">← back</a></p></body></html>"
)


@router.get("/sites/{host}/view/{ts}/{path:path}")
async def view_path(request: Request, host: str, ts: str, path: str = ""):
    host = valid_host(host)
    ts = valid_ts(ts)
    base = _host_dir(host) / ts

    # Intercept server-side imagemap clicks: `<img ismap>` sends coords as a
    # bare `?x,y` query. Parse the on-disk .map and 302 to the matching shape.
    if path.endswith(".map") and request.url.query:
        from .. import imagemap
        coords = imagemap.parse_query_coords(request.url.query)
        if coords:
            f = _safe_path(base, path)
            if f.is_file():
                try:
                    body = f.read_bytes()
                except OSError:
                    body = b""
                if imagemap.is_plausible_map_text(body):
                    try:
                        shapes = imagemap.parse_map(body.decode("utf-8", errors="replace"))
                        target = imagemap.resolve(shapes, *coords)
                    except Exception:
                        target = None
                    if target:
                        from urllib.parse import urlparse as _up
                        from posixpath import relpath as _relpath
                        p = _up(target)
                        # Rewrite to local viewer when the shape's URL points
                        # at the same host.
                        if (not p.scheme or not p.netloc or
                                p.netloc.lstrip("www.") == host.lstrip("www.")):
                            tpath = (p.path or "/").lstrip("/")
                            # Relative to the snapshot root; the viewer route is
                            # /sites/{host}/view/{ts}/<tpath>.
                            local_url = (
                                f"/sites/{host}/view/{ts}/"
                                f"{tpath or 'index.html'}"
                            )
                            if p.query:
                                local_url += f"?{p.query}"
                            if p.fragment:
                                local_url += f"#{p.fragment}"
                            return RedirectResponse(local_url, status_code=302)
                        # Off-host shape — let the browser handle it.
                        return RedirectResponse(target, status_code=302)
                    # Map parsed but no matching shape → 404.
                    raise HTTPException(404)
                # HTML-error masquerade → friendly fallback page.
                return HTMLResponse(_IMAGEMAP_FALLBACK_HTML, status_code=200)

    f = _safe_path(base, path or "index.html")
    if f.is_dir():
        f = f / "index.html"
    if not f.exists():
        # Fall back to the query-hashed filename when the archived page
        # references a URL with a query string. The shim's _get_local_path
        # stores `foo.png?v=1` as `foo.q-<hash>.png`; FastAPI strips the
        # query from the path parameter, so we recompute it here.
        query = request.url.query
        if query:
            from ..query_hash import suffix_for_query
            suffix = suffix_for_query(query)
            if suffix and f.suffix:
                candidate = f.with_name(f.stem + suffix + f.suffix)
                if candidate.is_file():
                    return FileResponse(candidate)
        raise HTTPException(404)
    return FileResponse(f)


@router.get("/sites/{host}/edit", response_class=HTMLResponse)
async def edit_get(request: Request, host: str, ts: str, path: str):
    host = valid_host(host)
    ts = valid_ts(ts)
    base = _host_dir(host) / ts
    f = _safe_path(base, path)
    if not f.is_file():
        raise HTTPException(404)
    ext = f.suffix.lower()
    if ext not in TEXT_EXTS:
        raise HTTPException(415, "Not a text file")
    content = f.read_text(encoding="utf-8", errors="replace")
    parent_rel = str(Path(path).parent)
    parent = "" if parent_rel == "." else parent_rel
    return templates.TemplateResponse(request, "editor.html", {
        "request": request, "host": host, "ts": ts, "path": path,
        "content": content, "mode": MODE_MAP.get(ext, "text"), "parent": parent,
    })


@router.post("/sites/{host}/edit")
async def edit_post(host: str, ts: str, path: str, content: str = Form(...)):
    host = valid_host(host)
    ts = valid_ts(ts)
    base = _host_dir(host) / ts
    f = _safe_path(base, path)
    if not f.is_file():
        raise HTTPException(404)
    f.write_text(content, encoding="utf-8")
    from urllib.parse import quote as _urlquote
    return RedirectResponse(
        f"/sites/{_urlquote(host, safe='')}/edit?ts={_urlquote(ts, safe='')}&path={_urlquote(path)}",
        status_code=303,
    )
