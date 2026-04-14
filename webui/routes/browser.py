"""Browse + edit archived sites."""
from __future__ import annotations
from pathlib import Path

import re
import shutil
from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, Response
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


@router.get("/snapshots", response_class=HTMLResponse)
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
    return templates.TemplateResponse("snapshots.html", {
        "request": request, "items": slice_, "page": page, "pages": pages,
        "per_page": per_page, "total": total, "host": host, "hosts_all": hosts_all,
    })


def _delete_snapshot(host: str, ts: str) -> bool:
    base = jobs.OUTPUT_ROOT.resolve()
    target = (jobs.OUTPUT_ROOT / host / ts).resolve()
    if base not in target.parents or not target.is_dir():
        return False
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


@router.post("/sites/{host}/delete-all")
async def delete_host(host: str):
    base = jobs.OUTPUT_ROOT.resolve()
    target = (jobs.OUTPUT_ROOT / host).resolve()
    if base in target.parents and target.is_dir():
        shutil.rmtree(target)
    return RedirectResponse("/snapshots", status_code=303)


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


_URL_ATTR_RE = re.compile(
    r'''(\s(?:src|href|srcset|poster|data|action|background|formaction)\s*=\s*["'])([^"']+)(["'])''',
    re.IGNORECASE,
)
_CSS_URL_RE = re.compile(r'''(url\(\s*["']?)([^)"']+)(["']?\s*\))''', re.IGNORECASE)


def _rewrite_abs(value: str, prefix: str) -> str:
    """Prefix absolute-path refs (/foo) with `prefix` so they resolve locally.
    Leaves schemed URLs, protocol-relative //, fragments, mailto:, /web/... untouched."""
    v = value.strip()
    if not v.startswith("/"):
        return value
    if v.startswith("//") or v.startswith("/web/") or v.startswith(prefix):
        return value
    return prefix.rstrip("/") + v


def _rewrite_srcset(value: str, prefix: str) -> str:
    out = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        bits = part.split(None, 1)
        bits[0] = _rewrite_abs(bits[0], prefix)
        out.append(" ".join(bits))
    return ", ".join(out)


def _rewrite_html(html: str, prefix: str) -> str:
    def attr_sub(m):
        lead, val, trail = m.group(1), m.group(2), m.group(3)
        if lead.lower().strip().startswith("srcset"):
            return lead + _rewrite_srcset(val, prefix) + trail
        return lead + _rewrite_abs(val, prefix) + trail
    html = _URL_ATTR_RE.sub(attr_sub, html)
    html = _CSS_URL_RE.sub(
        lambda m: m.group(1) + _rewrite_abs(m.group(2), prefix) + m.group(3), html
    )
    # Insert a <base> so any remaining relative refs resolve under the snapshot.
    base_tag = f'<base href="{prefix}">'
    if "<head" in html.lower():
        import re as _re
        html = _re.sub(r"(<head[^>]*>)", r"\1" + base_tag, html, count=1, flags=_re.I)
    else:
        html = base_tag + html
    return html


def _rewrite_css(css: str, prefix: str) -> str:
    return _CSS_URL_RE.sub(
        lambda m: m.group(1) + _rewrite_abs(m.group(2), prefix) + m.group(3), css
    )


@router.get("/sites/{host}/view")
async def view(host: str, ts: str, path: str = "index.html"):
    """Serve an archived file. Rewrites absolute-path URLs in HTML/CSS so
    references like /images/foo.gif resolve under the snapshot instead of
    hitting the dashboard origin and 404ing."""
    base = _host_dir(host) / ts
    f = _safe_path(base, path)
    if f.is_dir():
        f = f / "index.html"
    if not f.exists():
        raise HTTPException(404)
    ext = f.suffix.lower()
    prefix = f"/sites/{host}/view/{ts}/"
    if ext in (".html", ".htm"):
        body = f.read_text(encoding="utf-8", errors="replace")
        return Response(_rewrite_html(body, prefix), media_type="text/html; charset=utf-8")
    if ext == ".css":
        body = f.read_text(encoding="utf-8", errors="replace")
        return Response(_rewrite_css(body, prefix), media_type="text/css; charset=utf-8")
    return FileResponse(f)


@router.get("/sites/{host}/view/{ts}/{path:path}")
async def view_asset(host: str, ts: str, path: str):
    """Path-based companion to /sites/{host}/view used by rewritten HTML."""
    base = _host_dir(host) / ts
    f = _safe_path(base, path or "index.html")
    if f.is_dir():
        f = f / "index.html"
    if not f.exists():
        raise HTTPException(404)
    ext = f.suffix.lower()
    prefix = f"/sites/{host}/view/{ts}/"
    if ext in (".html", ".htm"):
        body = f.read_text(encoding="utf-8", errors="replace")
        return Response(_rewrite_html(body, prefix), media_type="text/html; charset=utf-8")
    if ext == ".css":
        body = f.read_text(encoding="utf-8", errors="replace")
        return Response(_rewrite_css(body, prefix), media_type="text/css; charset=utf-8")
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
