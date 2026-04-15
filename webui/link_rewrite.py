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

# Archive-time rewriter hooks. See rewrite_snapshot() — these run once per
# archived HTML file during the "Fix links" operator action.

# Known search-CGI endpoints across 1996–2001 sites. `<form action>` hitting
# any of these gets rerouted to our replacement /sites/{host}/search?ts=…
# (a TF-IDF index over the same archived content).
_SEARCH_CGI_RE = re.compile(
    r"(^|/)(qfind|search|vtopic|asearch|cfilter|showprefs)"
    r"(\.exe|\.cgi|\.pl|\.asp|)(\?|$|/)",
    re.IGNORECASE,
)

# Form-mail / comments / contact-form CGIs: the original SMTP target is
# long gone; neutralize the form.
_FORM_MAIL_RE = re.compile(
    r"(form-?mail|comments?|contact|feedback|usa_comments)"
    r"\.(pl|cgi|asp|exe)\b",
    re.IGNORECASE,
)

# Generic dead-CGI catchall for <form action>: anything under /cgi-bin/ or
# ending in an obvious CGI extension that wasn't matched by the search rule.
_DEAD_CGI_RE = re.compile(
    r"(^|/)(cgi-bin/|vrty_cgi/|search97cgi/)"
    r"|\.(pl|cgi|asp|exe)(\?|$)",
    re.IGNORECASE,
)

# Hit-counter / web-bug images. The original CGI returned a dynamic GIF;
# now it's a broken-image icon. Swap to a 1×1 transparent PNG.
_COUNTER_RE = re.compile(
    r"(counter|wwwcounter|hitcount|webcounter|/Counter)",
    re.IGNORECASE,
)
_COUNTER_STUB = "/static/transparent.png"

# Third-party tracking/analytics/ad script src patterns. Stripped at
# archive time so the viewer doesn't beacon to live surveillance
# endpoints when a user browses an archived page.
_TRACKER_SRC_RE = re.compile(
    r"(google-analytics\.com|googletagmanager\.com|doubleclick\.net"
    r"|googlesyndication\.com|googletagservices\.com"
    r"|facebook\.net/[^/]+/sdk\.js|connect\.facebook\.net"
    r"|stats\.wp\.com|hotjar\.com|matomo\.cloud|piwik\."
    r"|segment\.(?:com|io)|mixpanel\.com|amplitude\.com"
    r"|heap\.io|pendo\.io|fullstory\.com|optimizely\.com"
    r"|newrelic\.com|nr-data\.net|sentry\.io"
    r"|quantserve\.com|scorecardresearch\.com|chartbeat\.com"
    r"|addthis\.com|addtoany\.com|sharethis\.com)",
    re.IGNORECASE,
)


def _strip_trackers_and_referrer(soup) -> int:
    """Remove known tracker/analytics <script src> tags and prepend a
    no-referrer policy meta to <head>. Idempotent — existing referrer
    policies are preserved."""
    hits = 0
    for script in soup.find_all("script", src=True):
        src = (script.get("src") or "").strip()
        if _TRACKER_SRC_RE.search(src):
            script.decompose()
            hits += 1
    head = soup.find("head")
    if head is not None and not head.find(
            "meta", attrs={"name": "referrer"}):
        meta = soup.new_tag("meta", attrs={
            "name": "referrer", "content": "no-referrer",
        })
        head.insert(0, meta)
        hits += 1
    return hits


def _neutralize_forms_and_counters(soup, host: str, ts: str) -> int:
    """In-place rewrite of:
      - <form action=...qfind.exe...> → search replacement
      - <form action=...form-mail.pl...> → inert + visible notice
      - <img src=...counter...> → 1×1 transparent stub
    Returns count of modifications."""
    hits = 0
    search_action = f"/sites/{host}/search?ts={ts}" if host and ts else ""

    for form in soup.find_all("form"):
        action = (form.get("action") or "").strip()
        if not action:
            continue
        # Search-CGI → our replacement. Keep method GET since our route is GET.
        if _SEARCH_CGI_RE.search(action) and search_action:
            form["action"] = search_action
            form["method"] = "get"
            # Rename the first text-ish input to `q` so our route receives it.
            for inp in form.find_all("input"):
                itype = (inp.get("type") or "text").lower()
                if itype in ("text", "search", "") and inp.has_attr("name"):
                    inp["name"] = "q"
                    break
            hits += 1
            continue
        # Form-mail / contact / any cgi-bin: neutralize.
        if _FORM_MAIL_RE.search(action) or _DEAD_CGI_RE.search(action):
            form["action"] = "#"
            form["onsubmit"] = (
                "alert('This form submitted to a CGI that the archive did "
                "not capture — submission would go nowhere.'); return false;"
            )
            # Add a small visible banner at the top of the form if not already
            # present (idempotent via a marker class).
            if not form.find(class_="wa-cgi-notice"):
                from bs4 import BeautifulSoup as _BS
                notice = _BS(
                    "<div class='wa-cgi-notice' style='background:#fef3c7;"
                    "border-left:3px solid #a16207;padding:.4rem .6rem;"
                    "margin-bottom:.5rem;font-size:.85rem;color:#713f12'>"
                    "⚠ Form submissions to this CGI are not archived. "
                    "Clicking the submit button will not actually send "
                    "anything.</div>",
                    "html.parser",
                ).div
                if notice is not None:
                    form.insert(0, notice)
            hits += 1

    for img in soup.find_all("img"):
        src = (img.get("src") or "").strip()
        if src and _COUNTER_RE.search(src):
            img["src"] = _COUNTER_STUB
            img["alt"] = img.get("alt") or "(archived hit counter)"
            hits += 1

    return hits
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


def _get_base_href(soup) -> str:
    """Return the first `<base href>` value in the document (stripped), or
    empty string if none. Used to pre-resolve relative refs in pages that
    set a `<base>` tag (common in 1990s frameset-era HTML)."""
    b = soup.find("base", href=True)
    if b is None:
        return ""
    return (b.get("href") or "").strip()


def _apply_base(ref: str, base_href: str) -> str:
    """Resolve `ref` against `base_href` if set. Fragment-only refs stay as
    same-page anchors; urljoin handles scheme / absolute-path / data / mailto
    preservation itself."""
    if not base_href or not ref or ref.startswith("#"):
        return ref
    from urllib.parse import urljoin
    try:
        return urljoin(base_href, ref)
    except Exception:
        return ref


def _apply_base_srcset(val: str, base_href: str) -> str:
    """Apply _apply_base to each candidate in a srcset-shaped string while
    preserving the descriptors (`1x`, `300w`, etc)."""
    out = []
    for piece in val.split(","):
        piece = piece.strip()
        if not piece:
            continue
        bits = piece.split(None, 1)
        bits[0] = _apply_base(bits[0], base_href)
        out.append(" ".join(bits))
    return ", ".join(out)


def extract_html_refs(html: str) -> list[str]:
    """Return every URL-ish ref in the HTML document, using a parser so that
    unquoted HTML4 attribute values are handled. Relative refs are pre-
    resolved against a `<base href>` tag if the page has one."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    base_href = _get_base_href(soup)
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

    if base_href:
        refs = [_apply_base(r, base_href) for r in refs]
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


def rewrite_html(html: str, file_rel_dir: str,
                 host: str = "", ts: str = "") -> tuple[str, int]:
    """Rewrite absolute-path refs inside HTML. Returns (new_html, hit_count).
    Uses BeautifulSoup so unquoted HTML4 attributes are handled. The document
    is reserialized only if something actually changed.

    When `host` and `ts` are provided, also rewires dead CGI forms (search →
    our /sites/{host}/search, mail/contact → inert) and swaps hit-counter
    images for a transparent 1×1 stub.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    hits = 0

    # Remove <base> after absolutizing — the browser would otherwise re-apply
    # it at render time against whatever the viewer URL happens to be.
    base_href = _get_base_href(soup)
    if base_href:
        for base_tag in soup.find_all("base", href=True):
            base_tag.decompose()
            hits += 1

    for tag_name, attr, is_srcset in _URL_ATTRS:
        selector = [tag_name] if tag_name else True
        for tag in soup.find_all(selector):
            if not tag.has_attr(attr):
                continue
            val = tag.get(attr)
            if not isinstance(val, str):
                continue
            original = val
            if base_href:
                val = (_apply_base_srcset(val, base_href) if is_srcset
                       else _apply_base(val, base_href))
            new, h = _rewrite_attr(val, file_rel_dir, is_srcset)
            if h or val != original:
                tag[attr] = new
                hits += max(h, 1)

    # Rewrite dead search/mail CGI forms and hit-counter images.
    hits += _neutralize_forms_and_counters(soup, host, ts)
    # Strip live tracker/analytics scripts; add no-referrer policy.
    hits += _strip_trackers_and_referrer(soup)

    # `<img ismap>` server-side imagemaps no longer work (the CGI is dead).
    # If the surrounding anchor points somewhere other than the .map file,
    # drop the ismap attribute so the browser honors the anchor's href instead
    # of POSTing click coordinates to a dead endpoint.
    for img in soup.find_all("img"):
        if not img.has_attr("ismap"):
            continue
        a = img.find_parent("a")
        if a is None:
            continue
        href = (a.get("href") or "").strip()
        if not href:
            continue
        # If the anchor points at the same .map file the img is using for
        # coordinates, removing ismap would break the only click path — leave
        # it in place.
        if href.lower().endswith(".map") or ".map?" in href.lower():
            continue
        del img["ismap"]
        hits += 1

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
    # Derive host / ts from the snapshot path (layout: .../<host>/<ts>/).
    try:
        ts_name = snapshot_dir.name
        host_name = snapshot_dir.parent.name
    except Exception:
        ts_name = host_name = ""
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
            new_text, hits = rewrite_html(text, rel_dir, host_name, ts_name)
        else:
            new_text, hits = rewrite_css(text, rel_dir)
        if hits and new_text != text:
            p.write_text(new_text, encoding="utf-8")
            changed += 1
            rewrites += hits
    return {"files_scanned": scanned, "files_changed": changed, "refs_rewritten": rewrites}
