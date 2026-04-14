"""Wayback CDX helpers + URL construction."""
from __future__ import annotations
import json
import time
import urllib.parse
import urllib.request
from typing import Optional
from urllib.parse import urlparse

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


def list_snapshots(url: str, from_year: Optional[int] = None, to_year: Optional[int] = None, limit: int = 500, collapse_digits: int = 8) -> list[dict]:
    key = f"{url}|{from_year}|{to_year}|{limit}|{collapse_digits}"
    now = time.time()
    if key in _CACHE and now - _CACHE[key][0] < _TTL:
        return _CACHE[key][1]

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
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.load(r)
    out: list[dict] = []
    if data and isinstance(data, list) and len(data) > 1:
        header = data[0]
        for row in data[1:]:
            out.append(dict(zip(header, row)))
    _CACHE[key] = (now, out)
    return out
