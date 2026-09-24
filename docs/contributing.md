# Contributing

The contribution workflow, including the branch rules and the definition of done, is in
[CONTRIBUTING.md](https://github.com/venkatcg00/etl-craft/blob/main/CONTRIBUTING.md). The
[rewrite plan](development/rewrite-plan.md) lists the branches that rebuild etl-craft for its
first release, and their status.

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
| `dev` | `main` | the default version until the first release |
| `X.Y` | the newest `vX.Y.Z` tag | one version per release line |
| `latest` | the newest release line | the default version once a release exists |

A release tag appears on the site at the next deployment. Push to `main`, or run the Docs
workflow from `main` (Actions → Docs → Run workflow) right after tagging.

To preview the site as it is deployed, with its version selector:

```bash
make docs-site                          # builds every version into _site/
python -m http.server --directory _site # then open http://127.0.0.1:8000
```
