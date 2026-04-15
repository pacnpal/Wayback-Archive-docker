from webui.wayback_resume_shim import (
    _looks_like_html_error,
    _url_ext,
    _origin_from_wayback,
)
from webui.query_hash import suffix_for_query as _suffix_for_query


def test_html_sniff_rejects_html_in_binary_slot():
    assert _looks_like_html_error(b"<!DOCTYPE html><html>", ".gif") is True
    assert _looks_like_html_error(b"<HTML>", ".png") is True
    assert _looks_like_html_error(b"  <!-- saved from url -->", ".css") is True


def test_html_sniff_accepts_html_in_html_slot():
    assert _looks_like_html_error(b"<!DOCTYPE html>", ".html") is False
    assert _looks_like_html_error(b"<html>", ".htm") is False
    # Unknown extension — treat as binary-slot sniff disabled.
    assert _looks_like_html_error(b"<html>", "") is False


def test_html_sniff_accepts_real_binary():
    assert _looks_like_html_error(b"GIF89a\x00", ".gif") is False
    assert _looks_like_html_error(b"\x89PNG\r\n\x1a\n", ".png") is False
    assert _looks_like_html_error(b"%PDF-1.4", ".pdf") is False


def test_html_sniff_allows_svg_xml_prolog():
    assert _looks_like_html_error(b'<?xml version="1.0"?><svg></svg>', ".svg") is False


def test_html_sniff_empty():
    assert _looks_like_html_error(b"", ".gif") is False


def test_query_hash_stable():
    assert _suffix_for_query("v=1") == _suffix_for_query("v=1")


def test_query_hash_differs():
    assert _suffix_for_query("v=1") != _suffix_for_query("v=2")


def test_query_hash_empty():
    assert _suffix_for_query("") == ""


def test_url_ext_with_query():
    assert _url_ext("http://x/a/b.png?q=1") == ".png"


def test_url_ext_no_ext():
    assert _url_ext("http://x/a/") == ""
    assert _url_ext("http://x") == ""


def test_url_ext_dot_in_path_segment_before_filename():
    assert _url_ext("http://x/v1.0/foo.css") == ".css"


def test_origin_from_wayback_id_flag():
    got = _origin_from_wayback("https://web.archive.org/web/20240101000000id_/https://x.com/a")
    assert got == "https://x.com/a"


def test_origin_from_wayback_bare():
    got = _origin_from_wayback("https://web.archive.org/web/20240101000000/https://x.com/a")
    assert got == "https://x.com/a"


def test_origin_from_wayback_non_wayback():
    assert _origin_from_wayback("https://x.com/a") is None
    assert _origin_from_wayback("") is None
