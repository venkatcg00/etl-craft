# CLAUDE.md

The project's instructions for every coding agent are in `AGENTS.md`; they apply in full:

@AGENTS.md

## Claude Code

- `.claude/skills/` links to the shared skills in `.agents/skills/`: `/roadmap-item` takes an
  item from branch to merge, and `/pre-pr-checks` runs the validation.
- Run long validation steps in the background with their logs in the session's scratchpad, and
  wait for the completion notice rather than polling.
- For changes to `api/`, tokens, secrets, SQL generation or file paths, the self-review includes
  the built-in `security-review` skill; for large diffs, also `code-review`.
