"""Browse + edit archived sites."""
from __future__ import annotations
from pathlib import Path

import shutil
from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates

from .. import jobs

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

TEXT_EXTS = {".html", ".htm", ".css", ".js", ".mjs", ".json", ".xml", ".svg", ".txt", ".md"}
MODE_MAP = {".html": "htmlmixed", ".htm": "htmlmixed", ".css": "css",
            ".js": "javascript", ".mjs": "javascript", ".json": "javascript",
            ".xml": "xml", ".svg": "xml", ".txt": "text", ".md": "text"}


def _host_dir(host: str) -> Path:
    d = (jobs.OUTPUT_ROOT / host).resolve()
    if not d.is_dir() or jobs.OUTPUT_ROOT.resolve() not in d.parents and d != jobs.OUTPUT_ROOT.resolve():
        raise HTTPException(404)
    return d


def _safe_path(base: Path, rel: str) -> Path:
    p = (base / rel).resolve()
    if base.resolve() != p and base.resolve() not in p.parents:
        raise HTTPException(400, "path escape")
    return p


def _all_snapshots() -> list[tuple[str, str]]:
    out = []
    root = jobs.OUTPUT_ROOT
    if not root.exists():
        return out
    for h in sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")):
        for s in sorted((x.name for x in h.iterdir() if x.is_dir()), reverse=True):
            out.append((h.name, s))
    return out


@router.get("/sites", response_class=HTMLResponse)
async def sites(request: Request, page: int = 1, per_page: int = 50, host: str = ""):
    items = _all_snapshots()
    if host:
        items = [i for i in items if i[0] == host]
    total = len(items)
    per_page = max(1, min(per_page, 100000))
    pages = max(1, (total + per_page - 1) // per_page) if total else 1
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    slice_ = items[start:start + per_page]
    hosts_all = sorted({h for h, _ in _all_snapshots()})
    return templates.TemplateResponse("sites.html", {
        "request": request, "items": slice_, "page": page, "pages": pages,
        "per_page": per_page, "total": total, "host": host, "hosts_all": hosts_all,
    })


def _delete_snapshot(host: str, ts: str) -> bool:
    base = jobs.OUTPUT_ROOT.resolve()
    target = (jobs.OUTPUT_ROOT / host / ts).resolve()
    if base not in target.parents or not target.is_dir():
        return False
    shutil.rmtree(target)
    parent = target.parent
    if parent.is_dir() and not any(parent.iterdir()):
        parent.rmdir()
    return True


@router.post("/sites/bulk-action")
async def sites_bulk_action(request: Request):
    form = await request.form()
    scope = form.get("scope", "selected")
    if scope == "shown":
        host = form.get("host") or ""
        try:
            page = int(form.get("page") or 1)
            per_page = int(form.get("per_page") or 50)
        except ValueError:
            page, per_page = 1, 50
        per_page = max(1, min(per_page, 100000))
        items = _all_snapshots()
        if host:
            items = [i for i in items if i[0] == host]
        slice_ = items[(page - 1) * per_page:(page - 1) * per_page + per_page]
        entries = [f"{h}/{t}" for h, t in slice_]
    else:
        entries = form.getlist("snapshot")
    for entry in entries:
        if "/" not in entry:
            continue
        h, t = entry.split("/", 1)
        _delete_snapshot(h, t)
    return RedirectResponse("/sites", status_code=303)


@router.post("/sites/{host}/delete-all")
async def delete_host(host: str):
    base = jobs.OUTPUT_ROOT.resolve()
    target = (jobs.OUTPUT_ROOT / host).resolve()
    if base in target.parents and target.is_dir():
        shutil.rmtree(target)
    return RedirectResponse("/sites", status_code=303)


@router.get("/sites/{host}/tree", response_class=HTMLResponse)
async def tree(request: Request, host: str, ts: str, path: str = ""):
    base = _host_dir(host) / ts
    if not base.is_dir():
        raise HTTPException(404)
    cur = _safe_path(base, path) if path else base
    if not cur.is_dir():
        raise HTTPException(404)
    entries = []
    for p in sorted(cur.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
        rel = str(p.relative_to(base))
        entries.append({"name": p.name, "rel": rel, "is_dir": p.is_dir(),
                        "size": p.stat().st_size if p.is_file() else 0})
    parent = None
    if path:
        parent_rel = str(Path(path).parent)
        parent = "" if parent_rel == "." else parent_rel
    return templates.TemplateResponse("tree.html", {
        "request": request, "host": host, "ts": ts, "entries": entries, "parent": parent,
    })


@router.get("/sites/{host}/view")
async def view(host: str, ts: str, path: str = "index.html"):
    base = _host_dir(host) / ts
    f = _safe_path(base, path)
    if f.is_dir():
        f = f / "index.html"
    if not f.exists():
        raise HTTPException(404)
    return FileResponse(f)


@router.get("/sites/{host}/edit", response_class=HTMLResponse)
async def edit_get(request: Request, host: str, ts: str, path: str):
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
    return templates.TemplateResponse("editor.html", {
        "request": request, "host": host, "ts": ts, "path": path,
        "content": content, "mode": MODE_MAP.get(ext, "text"), "parent": parent,
    })


@router.post("/sites/{host}/edit")
async def edit_post(host: str, ts: str, path: str, content: str = Form(...)):
    base = _host_dir(host) / ts
    f = _safe_path(base, path)
    if not f.is_file():
        raise HTTPException(404)
    f.write_text(content, encoding="utf-8")
    return RedirectResponse(f"/sites/{host}/edit?ts={ts}&path={path}", status_code=303)
