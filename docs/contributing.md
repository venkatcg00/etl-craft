# Contributing

The contribution workflow is in
[CONTRIBUTING.md](https://github.com/venkatcg00/etl-craft/blob/main/CONTRIBUTING.md), and the
rules every change follows (design rules, architecture, standards and the definition of done)
are in [AGENTS.md](https://github.com/venkatcg00/etl-craft/blob/main/AGENTS.md).
[Road to 1.0.0](development/road-to-1.0.0.md) is the build plan from 0.1.0 to 1.0.0: every
release, work item, test and defect. The [rewrite plan](development/rewrite-plan.md) records how
0.1.0 was built.

To build this site locally:

```bash
make docs          # strict build into site/
make docs-serve    # live preview at http://127.0.0.1:8000
```

## Published versions

The Docs workflow deploys the site to GitHub Pages on every push to `main`. Each deployment
rebuilds the whole site with `scripts/build_docs_site.py`, so no branch stores the published
pages:

| Version | Built from | Notes |
|---|---|---|
| `dev` | `main` | unreleased work; the default only while no release exists |
| `X.Y` | the newest `vX.Y.Z` tag | one version per release line |
| `latest` | the newest release line | the default version once a release exists |

A release tag appears on the site at the next deployment. Push to `main`, or run the Docs
workflow from `main` (Actions → Docs → Run workflow) right after tagging.

To preview the site as it is deployed, with its version selector:

```bash
make docs-site                          # builds every version into _site/
python -m http.server --directory _site # then open http://127.0.0.1:8000
```
