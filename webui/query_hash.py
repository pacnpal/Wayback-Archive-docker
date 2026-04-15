"""Shared query-string hash helper.

The shim's `_get_local_path` override appends `.q-<hash>` to the filename stem
when a URL has a query string, so that `foo.png?v=1` and `foo.png?v=2` don't
collide on disk. The viewer uses the same function to find the right file
when an archived page requests `foo.png?v=1` — FastAPI strips the query from
the path parameter, so we have to read it from `request.url.query` and
recompute the suffix.

Keeping this in its own module (rather than importing from the shim) avoids
pulling the shim's monkey-patch machinery into the FastAPI process.
"""
from __future__ import annotations
import hashlib

_QUERY_HASH_LEN = 8


def suffix_for_query(query: str) -> str:
    """Return `.q-<sha1(query)[:8]>` for a non-empty query, else empty string."""
    if not query:
        return ""
    h = hashlib.sha1(query.encode("utf-8")).hexdigest()[:_QUERY_HASH_LEN]
    return f".q-{h}"
