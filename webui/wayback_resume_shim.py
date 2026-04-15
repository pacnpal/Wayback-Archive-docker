"""Entry point that wraps upstream wayback_archive.cli.main with:
  1. Disk cache on `download_file` — any file already on disk is served from
     there (job-level resume). Cached bytes are sniffed first so we don't
     serve HTML-error-pages that were mistakenly saved as .gif/.css/etc.
  2. Sandbox on `_get_local_path` — refuse any write outside OUTPUT_DIR and
     any URL with no netloc, preventing the leak-to-mount-root bug.
  3. Query-string disambiguation — URLs that differ only by query get hash-
     suffixed filenames instead of colliding.
  4. Broader CDX fallback — when the *root* URL fails, try up to 30 nearby
     snapshots from CDX before giving up (upstream only tries a handful of
     ±hour offsets).

Usage (from webui.jobs): python -m webui.wayback_resume_shim
"""
from __future__ import annotations
import logging
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from .query_hash import suffix_for_query as _suffix_for_query


def _setup_logger() -> logging.Logger:
    lg = logging.getLogger("wayback.shim")
    if lg.handlers:
        return lg
    lg.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s wayback.shim: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    h1 = logging.StreamHandler(sys.stderr)
    h1.setFormatter(fmt)
    lg.addHandler(h1)
    try:
        f = open("/proc/1/fd/1", "w", buffering=1)
        h2 = logging.StreamHandler(f)
        h2.setFormatter(fmt)
        lg.addHandler(h2)
    except Exception:
        pass
    return lg


log = _setup_logger()
_cache_hits = 0


# --- corruption sniffing ----------------------------------------------------

_BINARY_EXTS = {
    ".gif", ".jpg", ".jpeg", ".png", ".webp", ".bmp", ".ico", ".svg",
    ".tif", ".tiff", ".pdf", ".zip", ".gz", ".tar", ".7z", ".rar",
    ".swf", ".mp3", ".mp4", ".m4a", ".m4v", ".mov", ".wav", ".ogg",
    ".webm", ".avi", ".mkv", ".flv", ".woff", ".woff2", ".ttf", ".otf",
    ".eot", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pps", ".pptx",
    ".css", ".js", ".json", ".xml", ".rss", ".atom",
}

_HTML_SNIFF = re.compile(rb"^\s*(<!doctype\s+html|<html[\s>]|<head[\s>]|<!--\s*saved|<\?xml[^>]*>\s*<html)", re.IGNORECASE)


def _url_ext(url: str) -> str:
    try:
        path = urlparse(url).path
    except Exception:
        return ""
    _, _, last = path.rpartition("/")
    _, dot, ext = last.rpartition(".")
    return ("." + ext.lower()) if dot else ""


def _looks_like_html_error(body: bytes, ext: str) -> bool:
    """True if `body` starts with HTML-ish bytes but the URL's extension
    indicates a non-HTML asset. Used to reject Wayback 404 pages stored as
    binary assets."""
    if not body or ext not in _BINARY_EXTS:
        return False
    head = body[:512].lstrip(b"\xef\xbb\xbf")
    # SVG is XML-ish so allow <?xml at the start if we're actually an SVG.
    if ext == ".svg" and head.startswith(b"<?xml"):
        return False
    return bool(_HTML_SNIFF.match(head))


# --- main monkey-patch ------------------------------------------------------

def _patch() -> None:
    from wayback_archive import downloader as d

    _orig_download_file = d.WaybackDownloader.download_file
    _orig_get_local_path = d.WaybackDownloader._get_local_path

    out_root = Path(os.environ.get("OUTPUT_DIR", "/app/output")).resolve()

    def safe_get_local_path(self, url: str) -> Path:
        """Same as upstream, but:
          * refuses URLs with no netloc (would land somewhere random),
          * injects a short query-string hash into the filename so same-path
            different-query URLs don't collide,
          * clamps the resolved path inside OUTPUT_DIR.
        """
        try:
            parsed = urlparse(url)
        except Exception:
            raise ValueError(f"unparseable url: {url!r}")
        if not parsed.netloc:
            raise ValueError(f"no netloc: {url!r}")
        # Let upstream compute its notion of the local path first.
        path = _orig_get_local_path(self, url)
        # If there's a query, splice a hash into the stem so concurrent
        # variants of the same path coexist.
        if parsed.query:
            suffix = _suffix_for_query(parsed.query)
            path = path.with_name(path.stem + suffix + path.suffix)
        # Sandbox: refuse anything that escapes OUTPUT_DIR.
        try:
            resolved = path.resolve()
            resolved.relative_to(out_root)
        except Exception:
            raise ValueError(f"path escapes OUTPUT_DIR: {path}")
        return path

    def cached_download_file(self, url: str):
        # Normalise to match whatever the main loop later calls
        # _get_local_path with.
        try:
            parsed = urlparse(url)
            netloc = (parsed.netloc or "").lower()
            if netloc.startswith("www."):
                netloc = netloc[4:]
            normalized = parsed._replace(
                netloc=netloc, fragment=""
            ).geturl()
            # Sandbox rejects relative/missing-netloc URLs outright — these
            # are the bug class that used to leak files to OUTPUT_DIR root.
            if not netloc:
                log.warning("rejecting URL without netloc: %s", url)
                raise ValueError("no netloc")
            local_path = self._get_local_path(normalized)
            if local_path.is_file() and local_path.stat().st_size > 0:
                body = local_path.read_bytes()
                if _looks_like_html_error(body, _url_ext(url)):
                    log.info("cache-bust html-masquerade %s", local_path)
                    try:
                        local_path.unlink()
                    except OSError:
                        pass
                else:
                    global _cache_hits
                    _cache_hits += 1
                    log.debug("cache-hit %s", local_path)
                    print(
                        f"         [resumed from disk] {local_path}",
                        flush=True,
                    )
                    return body
        except ValueError:
            # sandbox rejection — let upstream raise its own normal error
            return None
        except Exception:
            pass
        return _orig_download_file(self, url)

    d.WaybackDownloader._get_local_path = safe_get_local_path
    d.WaybackDownloader.download_file = cached_download_file


# --- purge partial last file ------------------------------------------------

_STEP_LINE_RE = re.compile(r"\[\d+[^\]]*\]\s+Downloading\s+\S+:\s+(.+?)\s*$")
_OUTCOME_RE = re.compile(r"✓ Downloaded|Failed to download|\[resumed from disk\]")


def _purge_partial_last_file() -> None:
    out_dir = os.environ.get("OUTPUT_DIR")
    if not out_dir:
        return
    log_path = Path(out_dir) / ".log"
    if not log_path.is_file():
        return
    try:
        with log_path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 32768))
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return
    lines = tail.splitlines()
    last_idx = -1
    last_url = None
    for i, line in enumerate(lines):
        m = _STEP_LINE_RE.search(line)
        if m:
            last_idx = i
            last_url = m.group(1).strip()
    if last_idx < 0 or not last_url:
        return
    if _OUTCOME_RE.search("\n".join(lines[last_idx + 1:])):
        return
    try:
        from wayback_archive.config import Config
        from wayback_archive.downloader import WaybackDownloader
        cfg = Config()
        if not cfg.wayback_url:
            return
        dl = WaybackDownloader(cfg)
        parsed = urlparse(last_url)
        netloc = (parsed.netloc or "").lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        normalized = parsed._replace(netloc=netloc, fragment="", query="").geturl()
        local_path = dl._get_local_path(normalized)
        if local_path.is_file():
            log.warning("purge in-flight file=%s", local_path)
            local_path.unlink()
    except Exception as e:
        log.warning("could not purge in-flight file: %s", e)


# --- redirect preservation ---------------------------------------------------

# Wayback URL pattern: https://web.archive.org/web/<ts>[<flags>]/<origin-url>
_WAYBACK_URL_RE = re.compile(
    r"^https?://web\.archive\.org/web/\d+[a-z_]*/(https?://.+)$", re.IGNORECASE
)


def _origin_from_wayback(wb_url: str) -> str | None:
    m = _WAYBACK_URL_RE.match(wb_url or "")
    return m.group(1) if m else None


def _patch_redirect_stubs() -> None:
    """Capture redirect chains on the downloader's requests session. For any
    (pre, post) pair where the origin-site paths differ, write a tiny meta-
    refresh stub at the pre-redirect local path so cross-path internal links
    still resolve locally.

    Stub creation is best-effort: silently skipped if the path computation
    or the sandbox guard rejects either side.
    """
    from wayback_archive import downloader as d
    _orig_init = d.WaybackDownloader.__init__

    def _install_session_wrapper(self):
        sess = getattr(self, "session", None)
        if sess is None or getattr(sess, "_redir_wrapped", False):
            return
        _orig_get = sess.get

        def wrapped_get(url, *a, **kw):
            resp = _orig_get(url, *a, **kw)
            try:
                history = getattr(resp, "history", None) or []
                if not history:
                    return resp
                final_origin = _origin_from_wayback(resp.url)
                if not final_origin:
                    return resp
                for hop in history:
                    pre_origin = _origin_from_wayback(hop.url)
                    if not pre_origin or pre_origin == final_origin:
                        continue
                    try:
                        pre_path = self._get_local_path(pre_origin)
                        post_path = self._get_local_path(final_origin)
                    except Exception:
                        continue
                    if pre_path == post_path or pre_path.exists():
                        continue
                    try:
                        from posixpath import relpath as _relpath
                        rel = _relpath(
                            str(post_path),
                            str(pre_path.parent) or ".",
                        )
                    except Exception:
                        continue
                    stub = (
                        f'<!DOCTYPE html><meta http-equiv="refresh" '
                        f'content="0; url={rel}"><title>Redirect</title>\n'
                    )
                    try:
                        pre_path.parent.mkdir(parents=True, exist_ok=True)
                        pre_path.write_text(stub, encoding="utf-8")
                        log.debug("redirect stub %s → %s", pre_origin, final_origin)
                    except Exception as e:
                        log.debug("redirect stub write failed: %s", e)
            except Exception as e:
                log.debug("redirect capture failed: %s", e)
            return resp

        sess.get = wrapped_get  # type: ignore[assignment]
        sess._redir_wrapped = True

    def wrapped_init(self, *a, **kw):
        _orig_init(self, *a, **kw)
        _install_session_wrapper(self)

    d.WaybackDownloader.__init__ = wrapped_init


# --- supplementary URL extractor ---------------------------------------------

def _patch_process_html() -> None:
    """Wrap upstream `_process_html` with an extra BS4 pass that surfaces URL
    refs the upstream if/elif walker misses: <video>/<audio>/<source[src]>,
    <track>, <embed>, <object[data]>, <link rel=manifest|preload|prefetch|
    icon|imagesrcset>, standalone <img srcset>, <meta http-equiv=refresh>,
    SVG <image href>, <form action>, and URL-shaped strings inside
    <script type="application/json">.

    The extras are joined against the page's base URL (so relative refs —
    which upstream sometimes queued un-absolutized — become full URLs) and
    appended to the links_to_follow list that upstream returns. Upstream's
    main loop then dedupes against visited_urls and its own scope filters,
    so double-queueing is free and non-matching domains still get dropped.
    """
    from urllib.parse import urljoin, urlparse as _urlparse
    from wayback_archive import downloader as d
    from .link_rewrite import extract_html_refs, extract_json_script_refs

    _orig_process_html = d.WaybackDownloader._process_html

    def wrapped_process_html(self, html: str, base_url: str):
        processed_html, links_to_follow = _orig_process_html(self, html, base_url)
        try:
            extra = list(extract_html_refs(html)) + list(extract_json_script_refs(html))
        except Exception as e:
            log.debug("supplementary extractor failed: %s", e)
            return processed_html, links_to_follow
        seen = {u for u in links_to_follow}
        added = 0
        for ref in extra:
            if not ref or ref.startswith(("#", "javascript:", "mailto:", "tel:",
                                           "data:", "about:")):
                continue
            try:
                absolute = urljoin(base_url, ref).split("#", 1)[0]
            except Exception:
                continue
            if not absolute or absolute in seen:
                continue
            p = _urlparse(absolute)
            if p.scheme not in ("http", "https") or not p.netloc:
                continue
            # Let upstream's scope filter (internal/google-fonts/squarespace)
            # make the final call. If it's clearly out-of-scope, drop early
            # to keep the queue small.
            try:
                is_ours = self._is_internal_url(absolute)
            except Exception:
                is_ours = False
            try:
                is_sq = self._is_squarespace_cdn(absolute)
            except Exception:
                is_sq = False
            if not (is_ours or is_sq):
                continue
            links_to_follow.append(absolute)
            seen.add(absolute)
            added += 1
        if added:
            log.debug("supplementary refs base=%s added=%d", base_url, added)
        # If a prefetch pool is running, seed it with everything we now have
        # queued (both upstream's native links and our supplementary ones).
        seed = getattr(self, "_prefetch_seed", None)
        if seed is not None and links_to_follow:
            try:
                seed(self, list(links_to_follow)[:_PREFETCH_WORKERS * 2])
            except Exception:
                pass
        return processed_html, links_to_follow

    d.WaybackDownloader._process_html = wrapped_process_html


# --- broader CDX fallback on root-URL failure --------------------------------

def _try_root_cdx_fallback() -> None:
    """If the single pending URL is the site root and it fails early, upstream
    tries only a handful of ±hour offsets and gives up. Before starting the
    run, pre-seed a wider set of nearby timestamps into the downloader's
    fallback list when the user-requested snapshot is known to be bad.

    Implementation: monkey-patch `download_file` to, on HTTP failure of the
    very first HTML fetch, query CDX for up to 30 nearby 200-status snapshots
    and try each via the raw `id_` endpoint before returning None.
    """
    from wayback_archive import downloader as d
    from .cdx import alt_timestamps, raw_fetch

    _current_download = d.WaybackDownloader.download_file
    state = {"root_tried": False}

    def broadened(self, url: str):
        content = _current_download(self, url)
        # Only widen for the first HTML fetch; subsequent assets are already
        # handled by upstream's own fallback.
        if content or state["root_tried"]:
            return content
        ext = _url_ext(url)
        if ext and ext not in (".html", ".htm", ""):
            return content
        state["root_tried"] = True
        try:
            prefer_ts = getattr(self, "original_timestamp", "") or ""
            alts = alt_timestamps(url, prefer_ts, limit=30)
        except Exception:
            alts = []
        if not alts:
            return content
        log.info("root CDX fallback url=%s trying=%d alts", url, min(len(alts), 20))
        for alt in alts[:20]:
            try:
                data = raw_fetch(self.session, alt, url)
            except Exception:
                data = None
            if data and not _looks_like_html_error(data, ext):
                log.info("root CDX fallback hit ts=%s bytes=%d", alt, len(data))
                print(f"         ✓ matched alt snapshot {alt}", flush=True)
                return data
        return content

    d.WaybackDownloader.download_file = broadened


# --- prefetch worker pool (opt-in parallelism) ------------------------------

# Shared prefetch state. When FETCH_WORKERS > 1, the supplementary-refs hook
# seeds URLs discovered during HTML processing into this pool; the next time
# the main loop calls download_file for one of them, the bytes are ready.
_PREFETCH_WORKERS = 1
_prefetch_pool = None
_prefetch_cache: dict = {}
_prefetch_lock = None


def _patch_prefetch() -> None:
    """Speculatively pre-fetch URLs we see during HTML processing, so
    `download_file` normally returns from an in-memory cache instead of
    blocking on the network.

    Upstream's per-URL processing is heavily stateful, so the main loop stays
    sequential; we just amortize the *network* wait. Safe: prefetch goes
    through the same `download_file` path (with our sandbox + corruption
    guards already in place), and only *reads* shared state.
    """
    global _PREFETCH_WORKERS, _prefetch_pool, _prefetch_lock
    try:
        _PREFETCH_WORKERS = max(1, int(os.environ.get("FETCH_WORKERS", "1")))
    except ValueError:
        _PREFETCH_WORKERS = 1
    if _PREFETCH_WORKERS <= 1:
        return

    import threading
    from concurrent.futures import ThreadPoolExecutor
    from wayback_archive import downloader as d

    log.info("prefetch workers=%d", _PREFETCH_WORKERS)
    _prefetch_pool = ThreadPoolExecutor(
        max_workers=_PREFETCH_WORKERS, thread_name_prefix="wa-prefetch"
    )
    _prefetch_lock = threading.Lock()

    _inner_download = d.WaybackDownloader.download_file

    def _prefetch_one(self, url: str):
        try:
            data = _inner_download(self, url)
        except Exception:
            data = None
        with _prefetch_lock:
            _prefetch_cache[url] = data

    def prefetching_download(self, url: str):
        with _prefetch_lock:
            hit = _prefetch_cache.pop(url, "MISS")
        if hit != "MISS":
            if hit is not None:
                log.debug("prefetch hit %s", url)
                return hit
        return _inner_download(self, url)

    # Expose a seeder so the process-html wrapper can enqueue discovered URLs.
    def seed(self, urls):
        if _prefetch_pool is None:
            return
        with _prefetch_lock:
            to_schedule = []
            for url in urls:
                if not url or url in _prefetch_cache:
                    continue
                _prefetch_cache[url] = None
                to_schedule.append(url)
        for url in to_schedule:
            _prefetch_pool.submit(_prefetch_one, self, url)

    d.WaybackDownloader.download_file = prefetching_download
    d.WaybackDownloader._prefetch_seed = seed  # type: ignore[attr-defined]


# --- Playwright HTML render fallback ----------------------------------------

def _patch_playwright() -> None:
    """When USE_PLAYWRIGHT=1 is set and the `playwright` package is available,
    wrap download_file so HTML pages are rendered through headless Chromium
    before upstream extracts assets from them. Gives us the post-JS DOM for
    SPAs (client-rendered lists, lazy-loaded images, data-src templating).

    Non-HTML fetches stay on the fast requests path. If Playwright is missing
    or fails to launch, we silently fall through — no behavior change.
    """
    if os.environ.get("USE_PLAYWRIGHT", "").strip() not in ("1", "true", "yes", "on"):
        return
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("USE_PLAYWRIGHT=1 but playwright package not installed; skipping")
        return

    log.info("playwright fallback enabled")
    from wayback_archive import downloader as d

    state: dict = {"pw": None, "browser": None, "ctx": None}

    def _ensure():
        if state["browser"]:
            return
        state["pw"] = sync_playwright().start()
        state["browser"] = state["pw"].chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        state["ctx"] = state["browser"].new_context(
            user_agent="Mozilla/5.0 Wayback-Archive/Playwright",
            ignore_https_errors=True,
        )

    _current_download = d.WaybackDownloader.download_file

    def rendered_download(self, url: str):
        ext = _url_ext(url)
        is_html = not ext or ext in (".html", ".htm")
        if not is_html:
            return _current_download(self, url)
        # Let upstream fetch first (via our shim chain: cache → CDX fallback →
        # requests). If it succeeded, render the returned HTML through the
        # browser to unveil JS-driven content, then return the rendered DOM.
        content = _current_download(self, url)
        if not content:
            return content
        try:
            _ensure()
            page = state["ctx"].new_page()
            try:
                page.set_content(
                    content.decode("utf-8", errors="replace"),
                    wait_until="networkidle",
                    timeout=15000,
                )
                rendered = page.content()
            finally:
                page.close()
            log.debug("playwright rendered %s (%d → %d bytes)",
                      url, len(content), len(rendered))
            return rendered.encode("utf-8")
        except Exception as e:
            log.warning("playwright render failed for %s: %s", url, e)
            return content

    d.WaybackDownloader.download_file = rendered_download


# --- entry point ------------------------------------------------------------

def main() -> None:
    _patch()
    _patch_prefetch()
    _patch_process_html()
    _patch_redirect_stubs()
    _try_root_cdx_fallback()
    _patch_playwright()
    _purge_partial_last_file()
    log.info("shim start output=%s", os.environ.get("OUTPUT_DIR"))
    t0 = time.monotonic()
    from wayback_archive.cli import main as cli_main
    try:
        cli_main()
    finally:
        log.info("shim end cache_hits=%d duration=%.1fs",
                 _cache_hits, time.monotonic() - t0)


if __name__ == "__main__":
    main()
    sys.exit(0)
