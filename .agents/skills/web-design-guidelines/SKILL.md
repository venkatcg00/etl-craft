---
name: web-design-guidelines
description: Review etl-craft's browser UI (Jinja templates, htmx, plain CSS and JavaScript under src/etl_craft/api) against the Web Interface Guidelines and report findings as file:line. Use before a pull request that changes a page, a template, monitor.css, shell.css or a page script, or when asked to review the UI's design, UX or accessibility.
---

# web-design-guidelines

Vendored from [vercel-labs/web-interface-guidelines](https://github.com/vercel-labs/web-interface-guidelines)
at `434b7f9` (MIT, see `LICENSE`); the rules
are in `rules.md` and are read from there, never fetched at run time.

## How to use

1. Read `rules.md` and the files to review (default: `src/etl_craft/api/templates/**`,
   `src/etl_craft/api/static/*.css`, `src/etl_craft/api/static/*.js`).
2. Report findings grouped by file in `file:line` form, as `rules.md` describes.
3. Fix what is mechanical; list the rest with the reason.
4. Render the changed pages with the repo's Playwright (light, dark, 390 px) and look at them.

## What applies here

The UI is server-rendered Jinja with htmx and small ES modules; there is no React. Skip the rules
written for React (hydration, `useState`, `onKeyDown` props, `Link`, virtualization libraries)
and read their intent against plain HTML instead.

Project decisions take precedence over these rules:

- Headings and buttons use sentence case, as in the approved Databricks-style layout, not
  Title Case.
- Times are formatted on the server in the person's chosen timezone (`zoned`), so pages and
  exports agree; do not reformat them with `Intl` in the browser.
- Status colours follow the shared tokens in `core/static/catalog.css` (green succeeded, red
  failed, blue running, grey waiting, slate stopped; amber only for retries and warnings), and
  every state keeps its symbol and word.
- Destructive actions confirm through the shared dialog with the exact scope and effect.
