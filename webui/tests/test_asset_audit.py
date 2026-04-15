import json
import os
import time

from webui.asset_audit import (
    audit_snapshot,
    get_audit,
    drop_audit,
    _resolve,
    AUDIT_NAME,
)


def test_audit_counts_and_missing(make_snapshot):
    snap = make_snapshot({
        "index.html": (
            '<html><body>'
            '<img src=logo.gif>'                # present
            '<img src="missing.png">'           # missing
            '<a href=sub/page.html>x</a>'       # present
            '<link rel=stylesheet href=css/a.css>'  # present
            '</body></html>'
        ),
        "logo.gif": b"GIF89a" + b"\x00" * 8,
        "sub/page.html": "<html>x</html>",
        "css/a.css": ".x{background:url(/img/bg.png)}",  # bg.png missing
    })
    r = audit_snapshot(snap)
    assert r["total_refs"] >= 5
    missing_rels = {m["rel"] for m in r["missing"]}
    assert "missing.png" in missing_rels
    assert "img/bg.png" in missing_rels
    # css/a.css and logo.gif and sub/page.html should all count as present.
    assert r["present"] >= 3


def test_get_audit_caches_then_invalidates_on_mtime(make_snapshot):
    snap = make_snapshot({
        "index.html": '<img src=a.png>',
        "a.png": b"GIF89a",
    })
    first = get_audit(snap)
    cache = snap / AUDIT_NAME
    assert cache.is_file()
    assert first["present"] == 1

    # Delete the asset and bump the index.html mtime so the snapshot's
    # latest-child mtime is newer than the cached audit.
    (snap / "a.png").unlink()
    future = time.time() + 2
    os.utime(snap / "index.html", (future, future))

    second = get_audit(snap)
    assert second["present"] == 0
    assert any(m["rel"] == "a.png" for m in second["missing"])


def test_drop_audit(make_snapshot):
    snap = make_snapshot({"index.html": "<html></html>"})
    get_audit(snap)
    assert (snap / AUDIT_NAME).is_file()
    drop_audit(snap)
    assert not (snap / AUDIT_NAME).exists()


def test_resolve_edge_cases():
    assert _resolve("index.html", "/") == "index.html"
    assert _resolve("index.html", "sub/") == "sub/index.html"
    # Fragment + query stripped.
    assert _resolve("index.html", "a.png?v=1#top") == "a.png"
    # Parent traversal rejected.
    assert _resolve("sub/page.html", "../../outside.txt") is None
    # Skip prefixes.
    assert _resolve("index.html", "mailto:x@y") is None
    assert _resolve("index.html", "javascript:void(0)") is None
    assert _resolve("index.html", "#top") is None
    assert _resolve("index.html", "https://external/z") is None
    assert _resolve("index.html", "/web/20240101/foo") is None


def test_audit_empty_dir(make_snapshot, tmp_path):
    # No html/css at all = zero refs.
    r = audit_snapshot(tmp_path)
    assert r == {"total_refs": 0, "present": 0, "missing": []}
