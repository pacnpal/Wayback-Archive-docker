"""In-place rewriter for absolute-path refs inside stored archived HTML/CSS.

Archived HTML sometimes contains root-relative URLs like `/images/foo.gif`.
When served from the dashboard those resolve against the dashboard origin
and 404. This module walks a snapshot directory and rewrites such refs to
relative paths so the pages render locally under `/sites/<host>/view?...`.

Uses BeautifulSoup for HTML so that unquoted HTML4 attributes (common in
pre-2000 snapshots) are handled correctly, and so the same URL extractor
can be shared with `asset_audit`.
"""
from __future__ import annotations
import re
from pathlib import Path
from posixpath import relpath

from bs4 import BeautifulSoup

_HTML_EXTS = {".html", ".htm"}
_CSS_EXTS = {".css"}

# url(...) inside CSS — tolerant of unquoted, single- or double-quoted.
_CSS_URL_RE = re.compile(r'''(url\(\s*)(["']?)([^)"']+)\2(\s*\))''', re.IGNORECASE)
# @import "foo.css"; / @import url(foo.css); — handled by _CSS_URL_RE for the
# url() form; this one covers the quoted bare-string form.
_CSS_IMPORT_RE = re.compile(r'''(@import\s+)(["'])([^"']+)(["'])''', re.IGNORECASE)
# Inline CSS meta-refresh: content="0; url=foo"
_META_REFRESH_RE = re.compile(r'''(?i)url\s*=\s*(['"]?)([^'";\s]+)\1''')

# Tokens that look like URL refs but are clearly not (JS template concat,
# method calls, bare identifiers, paths with spaces).
_NOT_URL_CHARS = re.compile(r'[+\s()<>{}\\]|^[A-Za-z_][A-Za-z_0-9]*$')


def _looks_like_url(v: str) -> bool:
    v = (v or "").strip()
    if not v:
        return False
    # Reject JS string concat (`+pic+`), function calls, bare identifiers.
    if "+" in v or "(" in v or ")" in v or " " in v:
        return False
    if "<" in v or ">" in v or "{" in v or "}" in v:
        return False
    return True


# Tag/attribute pairs that carry URL-like values. `is_srcset` splits on "," and
# takes the first whitespace-delimited token of each candidate descriptor.
# `on_tags` is None for "any tag". For `data` we restrict to <object> since
# other tags use `data` as plain data (e.g. <param data=...>).
_URL_ATTRS: tuple[tuple[str | None, str, bool], ...] = (
    (None, "src", False),
    (None, "href", False),
    (None, "srcset", True),
    (None, "imagesrcset", True),
    (None, "poster", False),
    (None, "background", False),
    (None, "action", False),
    (None, "formaction", False),
    (None, "manifest", False),
    (None, "data-src", False),
    (None, "data-srcset", True),
    (None, "data-href", False),
    (None, "data-bg", False),
    (None, "xlink:href", False),
    ("object", "data", False),
    ("embed", "src", False),
    ("video", "src", False),
    ("audio", "src", False),
    ("source", "src", False),
    ("track", "src", False),
    ("form", "action", False),
    ("image", "href", False),   # SVG <image>
    ("use", "href", False),     # SVG <use>
)


def _iter_srcset(v: str) -> list[str]:
    out = []
    for part in (v or "").split(","):
        part = part.strip()
        if not part:
            continue
        bits = part.split(None, 1)
        if bits:
            out.append(bits[0])
    return out


def extract_html_refs(html: str) -> list[str]:
    """Return every URL-ish ref in the HTML document, using a parser so that
    unquoted HTML4 attribute values are handled."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    refs: list[str] = []

    for tag_name, attr, is_srcset in _URL_ATTRS:
        selector = [tag_name] if tag_name else True
        for tag in soup.find_all(selector):
            if not tag.has_attr(attr):
                continue
            val = tag.get(attr)
            if not isinstance(val, str):
                continue
            if is_srcset:
                for tok in _iter_srcset(val):
                    if _looks_like_url(tok):
                        refs.append(tok)
            else:
                if _looks_like_url(val):
                    refs.append(val)

    # <meta http-equiv="refresh" content="0; url=foo">
    for m in soup.find_all("meta"):
        he = (m.get("http-equiv") or "").strip().lower()
        if he == "refresh":
            content = m.get("content") or ""
            mm = _META_REFRESH_RE.search(content)
            if mm and _looks_like_url(mm.group(2)):
                refs.append(mm.group(2))

    # inline style="...url(foo)..."
    for tag in soup.find_all(style=True):
        style = tag.get("style") or ""
        for m in _CSS_URL_RE.finditer(style):
            tok = m.group(3)
            if _looks_like_url(tok):
                refs.append(tok)

    # <style>...</style>
    for tag in soup.find_all("style"):
        for m in _CSS_URL_RE.finditer(tag.get_text() or ""):
            tok = m.group(3)
            if _looks_like_url(tok):
                refs.append(tok)
        for m in _CSS_IMPORT_RE.finditer(tag.get_text() or ""):
            tok = m.group(3)
            if _looks_like_url(tok):
                refs.append(tok)

    return refs


_JSON_URL_RE = re.compile(
    r'''["'](https?://[^\s"'<>\\]{4,}|/[A-Za-z0-9_\-./~%?=&+]{2,}\.(?:html?|css|js|png|jpe?g|gif|webp|svg|ico|woff2?|ttf|otf|mp4|webm|mp3|pdf))["']''',
    re.IGNORECASE,
)


def extract_json_script_refs(html: str) -> list[str]:
    """Scan <script type="application/json"> and ld+json bodies for URL-shaped
    strings. Cheap partial coverage for SPAs that embed payloads in inline
    JSON (next.js __NEXT_DATA__, wordpress block JSON, etc.)."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    refs: list[str] = []
    type_re = re.compile(r"application/(?:ld\+)?json", re.IGNORECASE)
    for tag in soup.find_all("script"):
        t = (tag.get("type") or "").strip().lower()
        if not type_re.match(t):
            continue
        body = tag.string or tag.get_text() or ""
        if not body:
            continue
        for m in _JSON_URL_RE.finditer(body):
            tok = m.group(1)
            if _looks_like_url(tok):
                refs.append(tok)
    return refs


def extract_css_refs(css: str) -> list[str]:
    refs = []
    for m in _CSS_URL_RE.finditer(css):
        tok = m.group(3)
        if _looks_like_url(tok):
            refs.append(tok)
    for m in _CSS_IMPORT_RE.finditer(css):
        tok = m.group(3)
        if _looks_like_url(tok):
            refs.append(tok)
    return refs


def _is_absolute_path_ref(v: str) -> bool:
    v = v.strip()
    if not v.startswith("/"):
        return False
    if v.startswith("//"):
        return False
    if v.startswith("/web/"):
        return False
    return True


def _abs_to_rel(value: str, file_rel_dir: str) -> str:
    v = value.strip()
    if not _is_absolute_path_ref(v):
        return value
    target = v.lstrip("/")
    src_dir = (file_rel_dir or ".").replace("\\", "/")
    try:
        return relpath(target, src_dir)
    except ValueError:
        return value


def _rewrite_attr(val: str, file_rel_dir: str, is_srcset: bool) -> tuple[str, int]:
    if not isinstance(val, str):
        return val, 0
    if is_srcset:
        hits = 0
        out = []
        for part in val.split(","):
            part = part.strip()
            if not part:
                continue
            bits = part.split(None, 1)
            new = _abs_to_rel(bits[0], file_rel_dir)
            if new != bits[0]:
                hits += 1
            bits[0] = new
            out.append(" ".join(bits))
        return ", ".join(out), hits
    new = _abs_to_rel(val, file_rel_dir)
    return new, (1 if new != val else 0)


def _rewrite_css_text(css: str, file_rel_dir: str) -> tuple[str, int]:
    hits = [0]

    def url_sub(m):
        tok = m.group(3)
        new = _abs_to_rel(tok, file_rel_dir)
        if new != tok:
            hits[0] += 1
        return m.group(1) + m.group(2) + new + m.group(2) + m.group(4)

    def import_sub(m):
        tok = m.group(3)
        new = _abs_to_rel(tok, file_rel_dir)
        if new != tok:
            hits[0] += 1
        return m.group(1) + m.group(2) + new + m.group(4)

    css = _CSS_URL_RE.sub(url_sub, css)
    css = _CSS_IMPORT_RE.sub(import_sub, css)
    return css, hits[0]


def rewrite_html(html: str, file_rel_dir: str) -> tuple[str, int]:
    """Rewrite absolute-path refs inside HTML. Returns (new_html, hit_count).
    Uses BeautifulSoup so unquoted HTML4 attributes are handled. The document
    is reserialized only if something actually changed."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    hits = 0

    for tag_name, attr, is_srcset in _URL_ATTRS:
        selector = [tag_name] if tag_name else True
        for tag in soup.find_all(selector):
            if not tag.has_attr(attr):
                continue
            val = tag.get(attr)
            if not isinstance(val, str):
                continue
            new, h = _rewrite_attr(val, file_rel_dir, is_srcset)
            if h:
                tag[attr] = new
                hits += h

    # inline styles
    for tag in soup.find_all(style=True):
        style = tag.get("style") or ""
        new, h = _rewrite_css_text(style, file_rel_dir)
        if h:
            tag["style"] = new
            hits += h

    # <style> blocks
    for tag in soup.find_all("style"):
        txt = tag.get_text() or ""
        new, h = _rewrite_css_text(txt, file_rel_dir)
        if h:
            tag.string = new
            hits += h

    if hits == 0:
        return html, 0
    return str(soup), hits


def rewrite_css(css: str, file_rel_dir: str) -> tuple[str, int]:
    return _rewrite_css_text(css, file_rel_dir)


def rewrite_snapshot(snapshot_dir: Path) -> dict:
    """Rewrite every HTML/CSS file under snapshot_dir in place. Returns a
    summary dict: {files_scanned, files_changed, refs_rewritten}."""
    scanned = changed = rewrites = 0
    for p in snapshot_dir.rglob("*"):
        if not p.is_file():
            continue
        ext = p.suffix.lower()
        if ext not in _HTML_EXTS and ext not in _CSS_EXTS:
            continue
        scanned += 1
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        rel_dir = str(p.parent.relative_to(snapshot_dir)).replace("\\", "/")
        if rel_dir == ".":
            rel_dir = ""
        if ext in _HTML_EXTS:
            new_text, hits = rewrite_html(text, rel_dir)
        else:
            new_text, hits = rewrite_css(text, rel_dir)
        if hits and new_text != text:
            p.write_text(new_text, encoding="utf-8")
            changed += 1
            rewrites += hits
    return {"files_scanned": scanned, "files_changed": changed, "refs_rewritten": rewrites}
