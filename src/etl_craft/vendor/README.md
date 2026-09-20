# Vendored third-party assets

## `fuse.min.js` — Fuse.js v7.0.0

Lightweight fuzzy-search, Apache-2.0 (the same license as etl-craft), copyright
Kiro Risk. Upstream: https://fusejs.io / https://github.com/krisk/Fuse

Copied here rather than loaded from a CDN, on purpose. `generate-docs` produces
a static site a team publishes wherever it likes, including internal networks
with no outbound access at all — a CDN `<script src>` would leave the search box
silently dead there. 15 KB is a fair price for a site that works everywhere.

This is the "use an existing package" half of the fuzzy-matching instruction.
The CLI's own "did you mean" suggestions use `difflib` from the standard
library instead: ranking three candidates for an error message is a different,
much smaller problem than ranking hundreds of entries as someone types.

### Updating

Download `dist/fuse.basic.min.js` from the release you want, replace the file,
and keep its copyright banner intact. Nothing in the engine imports it — it is
copied verbatim into the generated site by `docs_generator.generate_docs`.
