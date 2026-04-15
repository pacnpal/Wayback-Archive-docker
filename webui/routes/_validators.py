"""Shared validators for route path/query parameters that turn into disk paths.

These are intentionally *strict* — anything that doesn't look exactly like a
hostname (RFC-952/1123 style) or a 14-digit Wayback timestamp gets a 404.
Early-exit regex matches also double as CodeQL sanitizers so the downstream
filesystem code stops being flagged for path-injection.
"""
from __future__ import annotations
import re
from fastapi import HTTPException

_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-]{0,253}$")
_TS_RE = re.compile(r"^\d{14}$")


def valid_host(host: str) -> str:
    """Return `host` unchanged if it looks like a hostname, else raise 404.
    Rejects empty, slashes, `..`, null bytes, CRLF, unicode escapes — anything
    that could escape the `OUTPUT_DIR/<host>/…` layout."""
    if not host or not _HOST_RE.fullmatch(host):
        raise HTTPException(404)
    return host


def valid_ts(ts: str) -> str:
    """Return `ts` unchanged if it's a 14-digit snapshot timestamp, else 404."""
    if not ts or not _TS_RE.fullmatch(ts):
        raise HTTPException(404)
    return ts


def valid_ts_optional(ts: str) -> str:
    """Blank-OK variant for handlers that use empty string to mean 'all'."""
    if ts and not _TS_RE.fullmatch(ts):
        raise HTTPException(404)
    return ts
