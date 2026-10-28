---
name: product-audit
description: Audit etl-craft as a finished product before a release - usability for a new user, the CLI, UI and catalog, observability during real failures, capability against the plan, security of the running service, dependencies and images, and code health - and report findings before fixing anything. Use before 0.9.0 and every later release candidate, or when asked for a product, usability or release audit.
---

# Product audit

Tests prove that code does what it was written to do. This audit asks what a new user, an
operator at 3 a.m. and an attacker on the internal network would find, on the release
candidate as it will be published: the wheel and images, not the repository checkout.

## Rules

- **Report first.** Change no code until the owner has seen the report and chosen the fixes.
- **Be a new user.** Use only the published docs, `--help` and what the product shows. A
  feature someone can reach only through the source, an Engine DB query or knowledge of the
  developers' intentions counts as missing.
- **Reproduce, or say you didn't.** Mark every finding as reproduced, inferred from the code,
  or unverified. Never describe a screen you didn't render or a command you didn't run.
- **Stay in scope.** Make the existing product complete and pleasant to use. No new
  orchestration features, infrastructure or enterprise extras, and no redesign for taste.
  Never cite another tool as the reason for a finding; describe the user's problem.
- **Two passes.** After the checklist, do a second pass looking only for what the checklist
  didn't anticipate.

## Setup

- Build the candidate wheel and images. Install the wheel into a fresh virtual environment
  outside the repository, and use a fresh Engine DB.
- Keep everything under the session's scratchpad: logs, screenshots, the report.
- Start the services you need with `make services-up`.

## Areas and tools

### 1. First use, by agents with no context

- Start fresh subagents that know nothing about the project. Give each only the published
  docs site and the wheel.
- Tasks, one subagent each:
  - install, set up, sign in, run the demo;
  - deploy the multi-project layout with an API server, a UI copy and an agent;
  - break a task, find out why from the product alone, fix it and recover;
  - grant a colleague access to one pipeline, and check they can't see anything else.
- Each one reports every point where it got stuck, guessed or read source code. Each of those
  is a finding.

### 2. The CLI

- Run every command's `--help`, and every command in its success, failure and empty cases.
- Check:
  - error messages name the object, the value, the expectation and the remedy;
  - exit codes;
  - `--json` against the text output;
  - table alignment, ordering, units and durations;
  - hints that name the next command;
  - typo suggestions;
  - commands that need ids no other command shows.

### 3. The UI and the catalog

- Use Playwright MCP for repeatable flows. Take screenshots at 390px and 1440px, in light and
  dark themes.
- Cover:
  - sign-in, sign-out, session expiry and returning users;
  - navigation, search, filters, sorting and large lists;
  - empty, loading, error and success states;
  - browser back, forward and refresh;
  - every button giving feedback;
  - version, help and settings being findable.
- Accessibility:
  - the `fixing-accessibility` skill on the HTML;
  - `npx @axe-core/cli` and `npx lighthouse --only-categories=accessibility,best-practices` on
    each rendered page type.

### 4. Observability and recovery

- Break things on purpose with the `test-chaos-recovery` scenarios: kill a task, the agent and
  the controller; stop the Engine DB; fill the disk; let a lease expire.
- For each one, answer from the product alone:
  - What happened?
  - Which run and task did it affect?
  - What is the system doing now?
  - What should I do?
  Check the message, the audit rows, the attempt log, `status`/`explain`, the UI and
  `/metrics`.
- Run `promtool check metrics` on `/metrics`, through the `prom/prometheus` image.

### 5. Capability

- List every capability the roadmap marks Done, and the Support Insights demo's workflows.
- For each one, can a user do it, see it and undo it through the CLI, the API and the UI, where
  the plan says each should? Something that works only through config files or SQL, where the
  plan promises a command or a page, is a finding.

### 6. Security of the running service

- Run `test-security`'s matrix against the candidate.
- Fuzz the API from its schema: `uvx schemathesis run <api>/openapi.json --checks all`, with a
  viewer token and an admin token.
- Run the OWASP ZAP baseline (the `zaproxy/zap-stable` image, `zap-baseline.py`) against the UI
  and the served catalog: headers, cookies, CSRF.
- Scan dependencies with Semgrep MCP `semgrep_scan_supply_chain` and `uvx pip-audit` against
  `uv.lock`.
- Scan the images with `trivy image` (the `aquasec/trivy` image).
- Use the internal-deployment threat model (`docs/deploying/security.md`). Report
  internet-only risks separately, as Low.

### 7. Code health

- Run Semgrep MCP `semgrep_scan` on `src/`, and `ponytail-audit` for over-engineering.
- Ask the owner to run `/code-review ultra` on the candidate branch. Only the owner can start
  it.

## Report

Write one Markdown report:

1. Every finding, grouped and without duplicates:
   - title;
   - severity: Critical, High, Medium or Low;
   - area and the page, command or workflow;
   - the actual behaviour and the expected behaviour;
   - why it matters to the user;
   - the smallest fix;
   - the source files;
   - reproduced, inferred or unverified.
2. An inventory of every page, command and workflow actually examined.
3. Everyday capabilities that are missing.
4. Visual and layout inconsistencies.
5. Anything that needs manual checking, and why it couldn't be checked here.
6. A proposed order of small fixes, each independently testable, grouped into pull requests.

## Known findings to check again

From the 2026-10-27 audit of 0.3.0. Re-check each one; report it again only if it is still
true.

**CLI**
- `logs` needs an attempt id that no command shows.
- `history`, `steps`, `status` and `explain` refuse once a run has ended.
- `--run-id` also needs `--pipeline-code`.
- Selection errors show internal ids.
- `status`:
  - alphabetical order;
  - six-decimal durations;
  - `SLA=MET` on a failed run;
  - "blocked by failures: (none)".
- A failure summary has no next step. `explain` uses jargon.
- `pipelines`:
  - no last or next run;
  - a blank paused column;
  - SLA shown in fractional hours;
  - no `--all`.
- Tables are tab-separated and unaligned; `account list` has no header.
- `audit` lists oldest first, with no limit.
- INFO logs show by default.
- A wrong password gives no way forward. `whoami` is unlabelled.
- `token create` doesn't say the token is shown once.
- Users can't change their own password.
- The rerun hint omits `--with-downstream`.
- `run`'s options are ungrouped, and all commands are in one flat help list.

**Server**
- The demo controller doesn't say the API is off.
- `/` returns a bare 404.
- `--no-ui` help omits `/metrics`.

**Catalog**
- The DAG opens unfitted.
- "Unchecked" repeats on every row.
- Pages show absolute warehouse paths.
- The home page has two search boxes.
- The mobile task table has gaps.
- The breadcrumb repeats "warehouse".
- The run page shows duplicate "Used by" entries.

**Docs**
- The README's release sentence is stale.
- The Quick Start uses migrations where the guides use config files.
- There's no troubleshooting page and no first-project guide.
- The roadmap appears in the user navigation.

**Open questions for the owner**
- Should read-only views show the latest run by default?
