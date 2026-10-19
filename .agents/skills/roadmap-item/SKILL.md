---
name: roadmap-item
description: Take an etl-craft Road to 1.0.0 item from a fresh branch to a merged pull request - find and read the item, check it against the design rules and stop with a design note when it conflicts, implement, update CHANGELOG and the roadmap's progress row and handover notes, validate, open the PR with every template section filled and hand the squash-merge to GitHub auto-merge. Use when asked to continue, take over, or ship the next roadmap item.
---

# Ship a Road to 1.0.0 item

`AGENTS.md` is the rulebook; this is the procedure. `gh` below is the GitHub CLI; use your
environment's GitHub tools where it is not available.

## 1. Find the item

```bash
git fetch origin && git switch main && git pull --ff-only
gh pr list --state open                  # finish an open item before starting another
gh run list --branch main --limit 5      # the last merge's Docs deployment succeeded
```

An open pull request from an earlier session comes first. If its CI failed, auto-merge left it
open: read the log (`gh run view <run-id> --log-failed`), find the cause, fix it, rerun the
matching local step and push. Auto-merge stays on and merges it once the checks pass.

- The Progress table near the top of `docs/development/road-to-1.0.0.md` lists the work in
  order. The next item is the first row whose status is `In progress` or `Planned` and whose
  name starts with a workstream id (`S5.O.3 ...`). Release-gate rows such as `0.4 gate` are not
  items: they track a release and are never "next". If the owner named an item, take that one.
- Read the item's whole section (`### <id>`) to its last bullet, the Handover notes ("Priority
  correction", "Acceptance availability", "Choices that differ from the item text"), and every
  document the item links (`docs/development/metadata-and-ui.md` for S5.O to S5.Q).
- Write the item's "Done when" down as a checklist. If the item has no "Done when", derive one
  from its Problem and Change. Show it in the pull request and treat it as a decision to record.

## 2. Check it before building

Go through the stop-and-ask list in `AGENTS.md` ("Taking a request"). Pay particular attention
to these:

- **Item text against a design rule.** Item text written before a rule changed can contradict
  it, and the design rule wins. Examples: "warn and continue" where the rule is to fail, or
  "repeat in every pull request" where the CI cost policy says release validation.
- **Public surface the item does not name.** New settings, flags, parameters, tables, columns or
  dependencies.
- **Size.** If the item cannot ship as one cohesive pull request, propose the slices in a
  design note. Do not start a series on your own.
- **Fit with recent work.** Read the last few merged pull requests in the same area
  (`git log --oneline -15 -- <paths>`). If the item redesigns something just merged, ask first.

When one applies, write the design note from `AGENTS.md` and stop. In a non-interactive task the
note is the result: no branch and no pull request.

## 3. Build

- Branch: the name the item gives, otherwise `<type>/<area>-<topic>` (`CONTRIBUTING.md`).
- Search before writing (`git grep`). Reuse the shared helpers named in `AGENTS.md`, and keep
  SQL in the dialects layer.
- Follow the engineering standards and test rules in `AGENTS.md`. Each new test fails without
  the change: `git stash push src`, run it, `git stash pop`.
- Public behaviour (commands, `craft-connector.yml`, config files, JSON, exit codes, Python API):
  add a `CHANGELOG.md` entry under `[Unreleased]`, and update the docs page and the examples in
  `docs/examples/` and `examples/demo/`. The `docs/reference/` API pages are generated.
- Delete the path the item replaces, with its tests and docs.

## 4. Record in the roadmap

Only these parts of `docs/development/road-to-1.0.0.md` change in an item's pull request:

- **Its Progress row.** Set the status (`Done`, or `In progress` when the item lists slices and
  this is one of them), the pull request number and a one-line "Closes". Leave the other rows
  alone.
- **Handover notes:**
  - one short "Acceptance availability" paragraph: which suites ran and which could not;
  - a bullet under "Choices that differ from the item text" for every decision that differs
    from the item text, and for every "Done when" you had to derive.

Never edit the item's own text. Workstation tips, timings and logs belong in the pull request,
not the roadmap.

The pull request number goes in before the first push, so CI does not run twice. Issues and
pull requests share one sequence, so it is the newest number plus one:

```bash
gh api 'repos/{owner}/{repo}/issues?state=all&per_page=1' --jq '.[0].number'
```

Confirm it after the pull request opens, and push a correction only if it differs.

## 5. Validate

Run `.agents/skills/pre-pr-checks/SKILL.md`, including its self-review. Commit and push only
after it is green.

## 6. Commit and open the pull request

- **Subject:** `<type>(<area>): <summary> (road to 1.0.0, <id>)`. **Body:** short bullets of
  what changed and why, then any attribution trailer your environment requires.
- **Staging:** stage only your work: `git add -u -- . ':!release/evidence'`, plus new files by
  name.
- **Open it:** `git push -u origin <branch>`, then open the pull request against `main` with
  every section of `.github/pull_request_template.md` filled in:
  - **Summary**
  - **Roadmap item:** the id, plus "Done when" as a checked list
  - **Decisions:** the judgement calls you made
  - **Concerns and deviations:** "None" when empty
  - **New public surface and dependencies:** "None" when empty
  - **Validation:** per suite, the counts, coverage, duration and anything that could not run
  - **Handover**

  The PR policy workflow checks the sections and the "Done when" ticks; edit the description to
  fix it.

## 7. Hand the merge to GitHub

- Unless a required local suite could not run, turn on auto-merge and end the session:
  `gh pr merge <n> --squash --auto`. GitHub squash-merges once every required check passes and
  keeps the branch. Never pass `--delete-branch`.
- Do not watch CI, poll checks or wait for logs: every check re-sends the whole session. The next
  session finds the pull request in step 1 if CI failed.
- When a required local suite could not run (`AGENTS.md`, "Validation"), leave the pull
  request unmerged with the reason in it, and do not turn on auto-merge. A cloud suite that was
  unavailable on its one try does not hold the merge: record it and merge.
- Rerun a failed job only for a cause outside the change, after reading its log, and say which.
