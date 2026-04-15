"""Validator behavior: only well-formed hostnames and 14-digit timestamps pass."""
import pytest
from fastapi import HTTPException

from webui.routes._validators import valid_host, valid_ts, valid_ts_optional


# --- valid_host -------------------------------------------------------------

@pytest.mark.parametrize("good", [
    "example.com",
    "www.example.com",
    "a",
    "a.b.c.d.example.co.uk",
    "1.2.3.4",                      # bare-IP style
    "foo-bar.example.com",
    "a" + "b" * 250,                # right at the 254-char boundary
])
def test_valid_host_accepts(good):
    assert valid_host(good) == good


@pytest.mark.parametrize("bad", [
    "",
    "..",
    "../etc",
    ".example.com",                 # leading dot
    "-leading.hyphen.com",          # starts with hyphen
    "foo/bar",                      # slash (shouldn't reach us, but defence)
    "foo\x00bar",                   # null byte
    "foo\rbar",                     # CR
    "foo\nbar",                     # LF
    "foo bar.com",                  # space
    "日本.jp",                       # non-ASCII (we want IDN-encoded if used)
    "a" * 255,                      # over length
])
def test_valid_host_rejects(bad):
    with pytest.raises(HTTPException) as ei:
        valid_host(bad)
    assert ei.value.status_code == 404


# --- valid_ts ---------------------------------------------------------------

@pytest.mark.parametrize("good", [
    "20240101000000",
    "19961225051932",
    "99991231235959",
])
def test_valid_ts_accepts(good):
    assert valid_ts(good) == good


@pytest.mark.parametrize("bad", [
    "",
    "2024",
    "20240101",                     # 8 digits
    "2024010100000",                # 13 digits
    "202401010000000",              # 15 digits
    "20240101000000x",
    "20240101-00000",
    "  20240101000000",             # leading whitespace
    "..",
])
def test_valid_ts_rejects(bad):
    with pytest.raises(HTTPException) as ei:
        valid_ts(bad)
    assert ei.value.status_code == 404


# --- valid_ts_optional ------------------------------------------------------

def test_valid_ts_optional_blank_ok():
    assert valid_ts_optional("") == ""


def test_valid_ts_optional_rejects_malformed():
    with pytest.raises(HTTPException):
        valid_ts_optional("../etc")
