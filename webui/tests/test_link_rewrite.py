from webui.link_rewrite import (
    extract_html_refs,
    extract_css_refs,
    extract_json_script_refs,
    rewrite_html,
    rewrite_css,
)


def test_unquoted_html4_attrs():
    html = "<a href=corporate/index.html><img src=globed.gif border=0></a>"
    refs = extract_html_refs(html)
    assert "globed.gif" in refs
    assert "corporate/index.html" in refs


def test_img_srcset_splits_and_keeps_urls():
    html = '<picture><source srcset="a.png 1x, b.png 2x"><img src="c.png"></picture>'
    refs = extract_html_refs(html)
    assert "a.png" in refs and "b.png" in refs and "c.png" in refs


def test_standalone_img_srcset():
    html = '<img srcset="hi.png 2x, lo.png 1x">'
    refs = extract_html_refs(html)
    assert "hi.png" in refs and "lo.png" in refs


def test_media_tags():
    html = (
        '<video src="/v.mp4" poster="/p.jpg"></video>'
        '<audio><source src="/a.mp3"></audio>'
        '<track src="/t.vtt">'
    )
    refs = extract_html_refs(html)
    for expected in ("/v.mp4", "/p.jpg", "/a.mp3", "/t.vtt"):
        assert expected in refs


def test_object_vs_param_data_attribute():
    html = '<object data="/x.swf"></object><param data="family">'
    refs = extract_html_refs(html)
    assert "/x.swf" in refs
    assert "family" not in refs


def test_link_rel_manifest_preload_icon():
    html = (
        '<link rel="manifest" href="/a.webmanifest">'
        '<link rel="preload" href="/p.js" as="script">'
        '<link rel="icon" href="/favicon.ico">'
    )
    refs = extract_html_refs(html)
    for expected in ("/a.webmanifest", "/p.js", "/favicon.ico"):
        assert expected in refs


def test_meta_refresh():
    html = '<meta http-equiv="refresh" content="0; url=/go.html">'
    refs = extract_html_refs(html)
    assert "/go.html" in refs


def test_svg_use_and_image():
    html = '<svg><use xlink:href="/icons.svg#x"/><image href="/bg.png"/></svg>'
    refs = extract_html_refs(html)
    assert "/icons.svg#x" in refs
    assert "/bg.png" in refs


def test_form_action():
    html = '<form action="/submit"></form>'
    assert "/submit" in extract_html_refs(html)


def test_inline_style_and_style_block():
    html = (
        '<div style="background:url(/bg.png)"></div>'
        '<style>.x{background:url("/y.png")} @import "reset.css";</style>'
    )
    refs = extract_html_refs(html)
    assert "/bg.png" in refs and "/y.png" in refs and "reset.css" in refs


def test_rejects_js_template_and_function_calls():
    html = '<img src="+pic+"><a href="btnFoo()">x</a><img src="foo bar.png">'
    refs = extract_html_refs(html)
    for bad in ("+pic+", "btnFoo()", "foo bar.png"):
        assert bad not in refs


def test_css_url_and_import():
    css = '@import "reset.css"; @import url(theme.css); .x{background:url(/img/p.png)}'
    refs = extract_css_refs(css)
    assert "reset.css" in refs
    assert "theme.css" in refs
    assert "/img/p.png" in refs


def test_unquoted_css_url():
    assert "/x.png" in extract_css_refs(".a{background:url(/x.png)}")


def test_json_script_application_json():
    html = (
        '<script type="application/json">{"img":"https://x.com/foo.png",'
        '"rel":"/a/b.jpg","s":"not a url"}</script>'
    )
    refs = extract_json_script_refs(html)
    assert "https://x.com/foo.png" in refs
    assert "/a/b.jpg" in refs


def test_json_script_ld_json():
    html = (
        '<script type="application/ld+json">'
        '{"@context":"https://schema.org","image":"/covers/a.jpg"}</script>'
    )
    refs = extract_json_script_refs(html)
    assert "/covers/a.jpg" in refs


def test_json_script_ignores_plain_script():
    html = '<script>var u = "https://ignored.com/a.png";</script>'
    refs = extract_json_script_refs(html)
    assert refs == []


def test_rewrite_html_absolute_to_relative_nested():
    html = '<img src="/images/a.gif"><a href="/foo/b.html">x</a>'
    new, hits = rewrite_html(html, "bar/baz")
    assert hits == 2
    assert "../../images/a.gif" in new
    assert "../../foo/b.html" in new


def test_rewrite_html_noop_returns_identity():
    html = '<img src="already/local.png">'
    new, hits = rewrite_html(html, "")
    assert hits == 0
    # Identity return when nothing changes — preserves unquoted HTML byte-for-byte.
    assert new == html


def test_rewrite_html_leaves_protocol_and_wayback_alone():
    html = (
        '<link rel=stylesheet href="//cdn.example.com/a.css">'
        '<img src="/web/20240101000000/foo.png">'
    )
    new, hits = rewrite_html(html, "")
    assert hits == 0
    assert new == html


def test_rewrite_css_url_and_import():
    css = '@import "/theme/a.css"; .x{background:url(/img/p.png)}'
    new, hits = rewrite_css(css, "sub/dir")
    assert hits == 2
    assert "../../theme/a.css" in new
    assert "../../img/p.png" in new


def test_rewrite_html_srcset_preserves_descriptors():
    html = '<source srcset="/a.png 1x, /b.png 2x">'
    new, hits = rewrite_html(html, "sub")
    assert hits == 2
    assert "1x" in new and "2x" in new
    assert "../a.png" in new and "../b.png" in new
