# CLAUDE.md

The project's instructions for every coding agent are in `AGENTS.md`; they apply in full:

@AGENTS.md

## Claude Code

- `.claude/skills/` links to the shared skills in `.agents/skills/`: `/roadmap-item` takes an
  item from branch to merge, and `/pre-pr-checks` runs the validation.
  `/fixing-accessibility` reviews HTML for accessibility (the catalog pages, the S7.A UI).
  It is vendored from `ibelick/ui-skills` at `91b63e6` under its MIT license.
- Six test skills write and audit tests for one kind of defect each: `/test-etl-correctness`
  (written rows), `/test-run-state` (transitions, leases, races), `/test-chaos-recovery`
  (crashes and outages), `/test-security`, `/test-upgrades` and `/test-failure-messages`.
- Run long validation steps in the background with their logs in the session's scratchpad, and
  wait for the completion notice rather than polling.
- For changes to `api/`, tokens, secrets, SQL generation or file paths, the self-review includes
  the built-in `security-review` skill; for large diffs, also `code-review`.

- `/product-audit` audits the release candidate as a product and reports before fixing.
