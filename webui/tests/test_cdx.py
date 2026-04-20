import json

import pytest


@pytest.fixture
def cdx_env(tmp_path, monkeypatch):
    """Fresh OUTPUT_DIR + reloaded modules so the rate_limit gate
    starts empty each test. Without this, prior tests' rate_limit
    events in the shared DB make the CDX gate block for the full
    acquire timeout."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import importlib
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.rate_limit as rl
    importlib.reload(rl)
    import webui.cdx as cdx
    importlib.reload(cdx)
    return cdx, rl


class _FakeResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode("utf-8")
        self.status = 200

    def read(self):
        return self._body

    def getcode(self):
        return 200

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_alt_timestamps_sorted_by_proximity(cdx_env, monkeypatch):
    cdx, _ = cdx_env
    # CDX returns: header + rows. Rows = [timestamp, statuscode].
    payload = [
        ["timestamp", "statuscode"],
        ["20100101000000", "200"],  # far before
        ["20240101000000", "200"],  # == prefer_ts (excluded)
        ["20240110000000", "200"],  # close after
        ["20231215000000", "200"],  # close before
    ]

    def fake_urlopen(req, timeout=0):
        return _FakeResponse(payload)

    monkeypatch.setattr(cdx.urllib.request, "urlopen", fake_urlopen)

    got = cdx.alt_timestamps("https://x.com/", "20240101000000", limit=10)
    assert "20240101000000" not in got
    assert got[0] == "20240110000000"  # closest to prefer_ts
    assert got[-1] == "20100101000000"  # farthest


def test_alt_timestamps_empty_on_malformed(cdx_env, monkeypatch):
    cdx, _ = cdx_env

    class _R:
        status = 200
        def read(self):
            return b"not json"
        def getcode(self): return 200
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr(cdx.urllib.request, "urlopen", lambda *a, **kw: _R())
    assert cdx.alt_timestamps("https://x.com/", "20240101000000") == []


def test_alt_timestamps_empty_on_network_error(cdx_env, monkeypatch):
    cdx, _ = cdx_env

    def boom(*a, **kw):
        raise OSError("network down")
    monkeypatch.setattr(cdx.urllib.request, "urlopen", boom)
    assert cdx.alt_timestamps("https://x.com/", "20240101000000") == []


class _StubResp:
    def __init__(self, status, content):
        self.status_code = status
        self.content = content
        self.headers = {}


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


def test_raw_fetch_returns_body_on_200(cdx_env):
    cdx, _ = cdx_env
    sess = _StubSession(_StubResp(200, b"hello"))
    got = cdx.raw_fetch(sess, "20240101000000", "https://x.com/a.png")
    assert got == b"hello"
    assert "id_/https://x.com/a.png" in sess.last_url


def test_raw_fetch_none_on_non_200(cdx_env):
    cdx, _ = cdx_env
    sess = _StubSession(_StubResp(404, b"nope"))
    assert cdx.raw_fetch(sess, "20240101000000", "https://x.com/a") is None


def test_raw_fetch_none_on_exception(cdx_env):
    cdx, _ = cdx_env
    sess = _StubSession(exc=RuntimeError("boom"))
    assert cdx.raw_fetch(sess, "20240101000000", "https://x.com/a") is None


def test_raw_fetch_trips_block_on_playback_429(cdx_env):
    """Playback (/web/<ts>id_/) isn't CDX, but a 429 there still means
    IA is rejecting us — must trip the shared outage gate so jobs
    stop piling on while we're blocked."""
    cdx, rl = cdx_env
    assert rl.is_blocked() is False
    sess = _StubSession(_StubResp(429, b""))
    assert cdx.raw_fetch(sess, "20240101000000", "https://x.com/a") is None
    assert rl.is_blocked() is True
