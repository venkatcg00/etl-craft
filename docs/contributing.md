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

The site is published to GitHub Pages from the `gh-pages` branch by the Docs workflow, with
[mike](https://github.com/jimporter/mike) keeping one directory per version:

| Version | Published from | Notes |
|---|---|---|
| `dev` | every push to `main` | the default version until the first release |
| `X.Y`, alias `latest` | each release tag `vX.Y.Z` | `latest` becomes the default version |

To preview the versioned site, `scripts/publish_docs.sh refs/heads/main` commits a build to the
local `gh-pages` branch without pushing it, and `uv run mike serve` serves that branch with its
version selector.
