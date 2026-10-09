---
name: roadmap-item
description: Take an etl-craft Road to 1.0.0 item from a fresh branch to a merged pull request - read the item, branch, implement, update CHANGELOG and the roadmap's status table and handover notes, validate, commit, open the PR, watch CI and squash-merge. Use when asked to continue, take over, or ship the next roadmap item.
---

# Ship a Road to 1.0.0 item

## Find the item

```bash
git fetch && git switch main && git pull --ff-only
gh pr list --state open                  # finish an open item before starting another
```

- The status table near the top of `docs/development/road-to-1.0.0.md` lists the items in
  order; the first row that is not Done is next. Read its whole section (`### <id>`) to its
  last bullet, the Handover notes, and any part of `docs/development/rewrite-plan.md` it refers
  to. List the item's bullets and its "Done when" and check each off before calling it done.
- Branch name: the one the item gives, otherwise `<type>/<area>-<topic>` (CONTRIBUTING.md).

## Build

- Follow CLAUDE.md: layers, the `core` error hierarchy, lowercase aliases in raw SQL, suite
  markers on every test, and failures that name the object, the value found, what was
  expected and the remedy.
- Comments and docstrings describe current behaviour. Item ids, dates and "changed because"
  notes belong in the commit and the PR; `make history` rejects decision tags, review-item
  ids and dated notes.
- Public behaviour (commands, `craft-connector.yml`, JSON, exit codes, Python API): add a
  `CHANGELOG.md` entry under `[Unreleased]` and update the docs page and the examples in
  `docs/examples/` and `examples/demo/`. The `docs/reference/` API pages are generated.

## Record in the roadmap

- Status row: `| <id> <title> | Done | #<PR> | <what it closes or delivers> |`, followed by
  `| <next id> and later | Not started | | |`.
- Handover notes: an "Acceptance availability" line saying which suites ran and any cloud that
  could not, and a bullet under "Choices that differ from the item text" for every deviation.
- The PR number, before the first push, so CI does not run twice: issues and PRs share one
  sequence, so it is the newest number plus one.

  ```bash
  gh api 'repos/{owner}/{repo}/issues?state=all&per_page=1' --jq '.[0].number'
  ```

  Confirm it after `gh pr create`; push a correction only if it differs.

## Validate

Run the `pre-pr-checks` skill. Commit and push only after it is green.

## Commit and open the PR

- Subject: `<type>(<area>): <summary> (road to 1.0.0, <id>)`; body: short bullets of what
  changed; end with the attribution trailer the session specifies.
- Stage only your work: `git add -u -- . ':!release/evidence'`, plus new files by name.
- `git push -u origin <branch>`, then
  `gh pr create --base main --title "<subject>" --body-file "$SCRATCH/pr.md"`. Body sections:
  Summary, Changes, Validation (counts, coverage and duration per suite), Handover
  (deviations and anything left for later). End with the PR attribution line the session
  specifies.

## CI and merge

- Pull-request CI runs once per revision; every push starts it again. Watch it in the
  background: `gh pr checks <n> --watch --fail-fast`.
- On a failure: `gh run view <run-id> --log-failed`, fix it, rerun the matching local step,
  push.
- When every check is green: `gh pr merge <n> --squash`. Never pass `--delete-branch`; every
  branch is kept.
- Afterwards: `git switch main && git pull --ff-only`, and confirm the Docs deployment
  succeeded: `gh run list --branch main --limit 5`.
