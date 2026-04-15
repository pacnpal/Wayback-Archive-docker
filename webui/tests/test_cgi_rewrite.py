"""Archive-time rewrites for dead CGIs: search → local, form-mail → inert,
hit-counters → transparent pixel."""
from webui.link_rewrite import rewrite_html


HOST = "example.com"
TS = "20000101000000"


def test_search_form_rerouted_to_local():
    html = '<form action="cgi/qfind.exe" method="POST"><input name="SearchString"><input type="submit"></form>'
    new, hits = rewrite_html(html, "", HOST, TS)
    assert hits >= 1
    assert "/sites/example.com/search?ts=20000101000000" in new
    # First text input renamed to `q`.
    assert 'name="q"' in new
    # Method flipped to GET so our route receives the query string.
    assert 'method="get"' in new


def test_search_form_rerouted_vtopic():
    html = '<form action="search97cgi/vtopic.exe" method="POST"><input name="qp"></form>'
    new, _ = rewrite_html(html, "", HOST, TS)
    assert "/sites/example.com/search?ts=20000101000000" in new


def test_form_mail_neutralized():
    html = '<form action="cgi-bin/form-mail.pl" method="POST"><input name="email"><input type="submit"></form>'
    new, hits = rewrite_html(html, "", HOST, TS)
    assert hits >= 1
    assert 'action="#"' in new
    assert "onsubmit" in new
    assert "wa-cgi-notice" in new


def test_generic_cgi_bin_neutralized():
    html = '<form action="/cgi-bin/feedback.pl"><input></form>'
    new, _ = rewrite_html(html, "", HOST, TS)
    assert 'action="#"' in new
    assert "return false" in new


def test_counter_image_replaced():
    html = '<img src="/cgi/counter?page=home" alt="hit counter">'
    new, hits = rewrite_html(html, "", HOST, TS)
    assert hits >= 1
    assert "/static/transparent.png" in new


def test_counter_wwwcounter_pattern():
    html = '<img src="/wwwcounter/cgi-bin/counter.exe?a=1">'
    new, _ = rewrite_html(html, "", HOST, TS)
    assert "/static/transparent.png" in new


def test_non_cgi_form_left_alone():
    # Relative, non-CGI action — no rewiring, no notice.
    html = '<form action="local/handler" method="POST"></form>'
    new, _ = rewrite_html(html, "", HOST, TS)
    assert "local/handler" in new
    assert "wa-cgi-notice" not in new
    assert 'action="#"' not in new


def test_no_host_ts_falls_back_to_generic_neutralizer():
    # Without host/ts, the search replacement is skipped (we can't construct
    # the local URL), so the form falls into the dead-CGI neutralizer branch
    # because .exe endpoints are still recognizably dead.
    html = '<form action="cgi/qfind.exe"></form>'
    new, _ = rewrite_html(html, "")
    assert 'action="#"' in new
    assert "return false" in new
