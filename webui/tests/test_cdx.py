import io
import json

import webui.cdx as cdx


class _FakeResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_alt_timestamps_sorted_by_proximity(monkeypatch):
    # CDX returns: header + rows. Rows = [timestamp, statuscode].
    payload = [
        ["timestamp", "statuscode"],
        ["20100101000000", "200"],  # far before
        ["20240101000000", "200"],  # == prefer_ts (excluded)
        ["20240110000000", "200"],  # close after
        ["20231215000000", "200"],  # close before
    ]

    def fake_urlopen(req, timeout=0):
        # Return a fake file-like object whose read() returns bytes; cdx uses
        # json.load which calls .read(). Our _FakeResponse supports both
        # json.load(f) (iterates over .read()) and context manager.
        return _FakeResponse(payload)

    monkeypatch.setattr(cdx.urllib.request, "urlopen", fake_urlopen)

    got = cdx.alt_timestamps("https://x.com/", "20240101000000", limit=10)
    assert "20240101000000" not in got
    assert got[0] == "20240110000000"  # closest to prefer_ts
    assert got[-1] == "20100101000000"  # farthest


def test_alt_timestamps_empty_on_malformed(monkeypatch):
    class _R:
        def read(self):
            return b"not json"
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr(cdx.urllib.request, "urlopen", lambda *a, **kw: _R())
    assert cdx.alt_timestamps("https://x.com/", "20240101000000") == []


def test_alt_timestamps_empty_on_network_error(monkeypatch):
    def boom(*a, **kw):
        raise OSError("network down")
    monkeypatch.setattr(cdx.urllib.request, "urlopen", boom)
    assert cdx.alt_timestamps("https://x.com/", "20240101000000") == []


class _StubResp:
    def __init__(self, status, content):
        self.status_code = status
        self.content = content


class _StubSession:
    def __init__(self, resp=None, exc=None):
        self._resp = resp
        self._exc = exc
        self.last_url = None

    def get(self, url, timeout=None, allow_redirects=True):
        self.last_url = url
        if self._exc:
            raise self._exc
        return self._resp


def test_raw_fetch_returns_body_on_200():
    sess = _StubSession(_StubResp(200, b"hello"))
    got = cdx.raw_fetch(sess, "20240101000000", "https://x.com/a.png")
    assert got == b"hello"
    assert "id_/https://x.com/a.png" in sess.last_url


def test_raw_fetch_none_on_non_200():
    sess = _StubSession(_StubResp(404, b"nope"))
    assert cdx.raw_fetch(sess, "20240101000000", "https://x.com/a") is None


def test_raw_fetch_none_on_exception():
    sess = _StubSession(exc=RuntimeError("boom"))
    assert cdx.raw_fetch(sess, "20240101000000", "https://x.com/a") is None
