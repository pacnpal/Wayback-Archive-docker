"""Wayback CDX helpers shared between the resume shim (to widen root-URL
retry) and the repair shim (targeted re-fetch of missing assets).

All CDX calls route through ``webui.rate_limit`` so a single dashboard
instance stays under IA's 60 req/min ceiling regardless of how many
subprocess shims are active in parallel.
"""
from __future__ import annotations
import json
import logging
import urllib.error
import urllib.parse
import urllib.request

from . import rate_limit

log = logging.getLogger("wayback.cdx")


def alt_timestamps(url: str, prefer_ts: str, limit: int = 30) -> list[str]:
    """Ask Wayback CDX for timestamps that archived `url` with statuscode 200,
    excluding `prefer_ts`, sorted by proximity to it."""
    params = {
        "url": url,
        "output": "json",
        "limit": str(limit),
        "fl": "timestamp,statuscode",
        "filter": "statuscode:200",
    }
    q = urllib.parse.urlencode(params)
    cdx = f"https://web.archive.org/cdx/search/cdx?{q}"
    try:
        req = urllib.request.Request(
            cdx, headers={"User-Agent": "Wayback-Archive/1.0"}
        )
        with rate_limit.cdx_urlopen(req, timeout=15) as r:
            data = json.load(r)
    except rate_limit.RateLimitTimeout as e:
        log.debug("cdx gate refused url=%s err=%s", url, e)
        return []
    except Exception as e:
        log.debug("cdx lookup failed url=%s err=%s", url, e)
        return []
    if not isinstance(data, list) or len(data) < 2:
        return []
    timestamps = [row[0] for row in data[1:] if row and row[0] != prefer_ts]
    try:
        key = int(prefer_ts)
        timestamps.sort(key=lambda t: abs(int(t) - key))
    except Exception:
        pass
    return timestamps


def raw_fetch(session, ts: str, url: str, timeout: int = 15) -> bytes | None:
    """Pull raw archived bytes (no Wayback scripts injected) at a specific
    timestamp. Returns content on HTTP 200 with non-empty body, else None.

    This hits the ``/web/<ts>id_/<url>`` playback endpoint, not CDX, so it
    doesn't consume the CDX rate budget. A 429 here still signals that IA
    is unhappy with us, though, so we trip the shared outage gate.
    """
    wb = f"https://web.archive.org/web/{ts}id_/{url}"
    try:
        r = session.get(wb, timeout=timeout, allow_redirects=True)
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After") if r.headers else None
            rate_limit.observe_429(
                retry_after_seconds=rate_limit.retry_after_to_seconds(retry_after)
            )
            return None
        if r.status_code == 200 and r.content:
            return r.content
    except Exception:
        pass
    return None
