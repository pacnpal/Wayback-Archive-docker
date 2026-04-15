# Archive limitations

Not every 1990s / 2000s website feature survives being saved as a static snapshot. This dashboard reconstructs what it can and gracefully degrades the rest. Here's the honest list of what's recoverable and what isn't.

## Reconstructed

| Original feature | How we restore it |
|---|---|
| **Server-side imagemaps** (`<img ismap>` → CGI `?x,y` → 302) | Local NCSA-map parser + viewer intercept. "Recover imagemaps" button on each site attempts CDX sweep for plain-text captures of the `.map` file when the local copy is Wayback's error page. Falls back to a friendly "not available in this capture" page. |
| **Site search CGIs** (`qfind.exe`, `vtopic.exe`, `cfilter.cgi`, `Verity Search97`, etc.) | Built-in TF-IDF indexer + `/sites/{host}/search` route. The "Build search index" button walks every snapshot and caches a `.search.json`. Archive-time rewriter points known search-form actions at our replacement. |
| **Hit counters / web bugs** (`<img src=".../counter.cgi">`) | Archive-time rewriter swaps the `src` for a 1×1 transparent PNG so pages don't render with a broken-image icon. |
| **Redirects** (HTTP 301/302) | Shim captures `response.history` during crawl and writes `<meta http-equiv=refresh>` stubs at the pre-redirect local paths so cross-path internal links resolve offline. |
| **Query-string assets** (`foo.png?v=1` vs `?v=2`) | Crawler gives each a hashed filename (`foo.q-<sha1>.png`). Viewer resolves the same hash from the request's query string on 404. |

## Gracefully neutralized

| Feature | Outcome when clicked |
|---|---|
| **Contact / comment form CGIs** (`form-mail.pl`, `sn_comm.pl`, `Form-Mail.asp`) | Archive-time: `action="#"`, `onsubmit="alert(…); return false"`, visible banner noting the form won't actually send. |
| **Generic `cgi-bin/*` forms** | Same neutralization pattern. |

## Unrecoverable

These features required live server state that was never captured by Wayback. We don't attempt reconstruction — clicking them typically shows an empty page or the archive's viewer 404. No amount of replay can bring them back.

| Feature | Why |
|---|---|
| **Shopping carts / e-commerce** (`ShoppingCart.asp`, `Config.asp?cModel=…`) | Inventory, pricing, sessions, checkout — all server-side state. |
| **Product comparators** (`ndCGI.exe/comparator/ProdComp`) | Consults a live product DB. |
| **Session-based auth** | Cookies + server-side sessions, neither archived. |
| **Email submission** | SMTP targets are gone; even if we simulated delivery, the recipients no longer exist. |
| **Third-party CGIs** (`altavista.com/cgi-bin/query/`, ad servers, trackers) | Not our content; original endpoints typically dead. |
| **FrontPage extensions** (`_vti_bin/shtml.exe`) | Proprietary IIS server-side logic. |
| **Live data feeds** (stock tickers, weather, news) | Point-in-time snapshots only — no way to replay the live feed. |
| **Flash SWF state** | Ruffle runs the bytecode, but anything that `POST`s to a dead endpoint still fails. |

## Honest fallbacks

When an archived page has both a server-side mechanism and a static equivalent, we prefer the static one:

- `<img ismap>` wrapped in a plain `<a href="/foo.html">` — the `ismap` attribute is stripped at archive time so the browser follows the anchor's explicit href instead of posting click coordinates to a dead CGI.

## Contributing

Spotted a class of dead dynamic content that *could* be reconstructed from what we already have on disk? Open an issue describing the endpoint pattern + what the original CGI did. The pattern used for imagemaps and search (parse the request, lookup the already-archived content, serve a plausible response) generalizes.
