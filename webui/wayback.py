"""Wayback CDX helpers + URL construction."""
from __future__ import annotations
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional
from urllib.parse import urlparse
from . import log as _log
from . import rate_limit

logger = _log.get("wayback")

_CACHE: dict[str, tuple[float, list[dict]]] = {}
_TTL = 600


def host_of(url: str) -> str:
    h = urlparse(url).hostname or url
    return h.lower().lstrip(".")


def latest_snapshot(target_url: str) -> Optional[tuple[str, str]]:
    """Return (timestamp, archived_original_url) for newest snapshot, or None."""
    try:
        snaps = list_snapshots(target_url, limit=10000)
    except Exception:
        return None
    if not snaps:
        return None
    best = max(snaps, key=lambda s: s["timestamp"])
    return best["timestamp"], best["original"]


def latest_timestamp(target_url: str) -> Optional[str]:
    r = latest_snapshot(target_url)
    return r[0] if r else None


def build_wayback_url(target_url: str, timestamp: Optional[str] = None) -> str:
    ts = timestamp or latest_timestamp(target_url)
    if not ts:
        raise ValueError(f"No Wayback snapshots found for {target_url}")
    return f"https://web.archive.org/web/{ts}/{target_url}"


class WaybackUnreachable(RuntimeError):
    pass


def list_snapshots(url: str, from_year: Optional[int] = None, to_year: Optional[int] = None, limit: int = 500, collapse_digits: int = 8) -> list[dict]:
    key = f"{url}|{from_year}|{to_year}|{limit}|{collapse_digits}"
    now = time.time()
    if key in _CACHE and now - _CACHE[key][0] < _TTL:
        age = now - _CACHE[key][0]
        logger.debug("cdx CACHE HIT url=%s age=%.1fs rows=%d ttl=%ds",
                     url, age, len(_CACHE[key][1]), _TTL)
        return _CACHE[key][1]
    logger.debug("cdx CACHE MISS url=%s from=%s to=%s limit=%d collapse=%d",
                 url, from_year, to_year, limit, collapse_digits)

    params = {
        "url": url,
        "output": "json",
        "limit": str(limit),
        "fl": "timestamp,original,statuscode,mimetype,digest",
        "filter": "statuscode:200",
        "collapse": f"timestamp:{collapse_digits}",
    }
    if from_year:
        params["from"] = str(from_year)
    if to_year:
        params["to"] = str(to_year)
    q = urllib.parse.urlencode(params)
    cdx = f"https://web.archive.org/cdx/search/cdx?{q}"
    req = urllib.request.Request(cdx, headers={"User-Agent": "Wayback-Archive-Dashboard/1.0"})
    last_err: Optional[Exception] = None
    data = None
    for attempt in range(3):
        t0 = time.monotonic()
        logger.debug("cdx HTTP GET attempt=%d url=%s", attempt + 1, cdx)
        try:
            with rate_limit.cdx_urlopen(req, timeout=30) as r:
                data = json.load(r)
            dur_ms = (time.monotonic() - t0) * 1000
            logger.debug("cdx HTTP OK attempt=%d rows=%s duration=%.1fms",
                         attempt + 1,
                         (len(data) - 1) if isinstance(data, list) and data else 0,
                         dur_ms)
            last_err = None
            break
        except rate_limit.RateLimitTimeout as e:
            # Gate refused — don't retry blindly, the gate already
            # waited up to its own timeout. Surface to the caller.
            logger.warning("cdx rate gate refused: %s", e)
            raise WaybackUnreachable(
                f"Wayback CDX is locally rate-limited: {e}. "
                f"Wait a minute and try again."
            ) from e
        except urllib.error.HTTPError as e:
            last_err = e
            dur_ms = (time.monotonic() - t0) * 1000
            # A 429 already tripped the hard block inside cdx_urlopen —
            # retrying during our own backoff just wastes the budget.
            if e.code == 429:
                logger.warning(
                    "cdx 429 on attempt=%d — hard block installed", attempt + 1,
                )
                raise WaybackUnreachable(
                    "Wayback CDX returned 429 Too Many Requests. The "
                    "dashboard has installed a local cooldown — new "
                    "jobs will pause until it clears."
                ) from e
            logger.warning("cdx retry attempt=%d err=%s (duration=%.1fms)",
                           attempt + 1, e, dur_ms)
            time.sleep(1.5 * (attempt + 1))
        except Exception as e:
            last_err = e
            dur_ms = (time.monotonic() - t0) * 1000
            logger.warning("cdx retry attempt=%d err=%s (duration=%.1fms)",
                           attempt + 1, e, dur_ms)
            time.sleep(1.5 * (attempt + 1))
    if last_err is not None or data is None:
        logger.error("wayback unreachable: %s", last_err)
        raise WaybackUnreachable(
            f"Wayback Machine (web.archive.org) is not reachable right now: {last_err}. "
            f"This is usually a temporary Internet Archive outage — try again in a few minutes."
        )
    out: list[dict] = []
    if data and isinstance(data, list) and len(data) > 1:
        header = data[0]
        for row in data[1:]:
            out.append(dict(zip(header, row)))
    _CACHE[key] = (now, out)
    logger.debug("cdx cached key=%r rows=%d", key, len(out))
    return out
