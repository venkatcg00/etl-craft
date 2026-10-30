# Browser UI

Run `etl-craft ui` with the `ui` extra, or use the controller's HTTP server. Put an HTTPS
reverse proxy in front of it, preserving the public Host header (for example nginx
`proxy_set_header Host $http_host`): browser cookies are always Secure, including local sign-in.
Open `/ui`. The minimum width is 320 CSS pixels; 960 pixels or wider gives the desktop layout.
The layout remains usable at 200% zoom.

## Sign-in and sessions

Local accounts and configured SSO use the existing login actions. Wrong account names and
passwords give the same message. Recovery means contacting the administrator shown on the
sign-in page; there are no emailed resets. SSO accounts use their identity provider.

Browser sessions have one fixed lifetime of `Auth.Session_hours` (default 12). Credentials
stay in HTTP-only, Secure, SameSite=Lax host-only cookies, never browser storage. The page
warns two minutes before expiry. Signing in again retains the requested same-site page;
external return addresses are refused. Signing out revokes the session in every tab, and
open tabs return to sign-in. Every browser change needs its CSRF challenge. Browser pages
refuse framing by other sites and ship a content security policy.

After `Auth.Lockout_after` consecutive wrong passwords (default 5; 0 disables lockout), a
local user locks until an admin runs `etl-craft account unlock --name ACCOUNT`. A locked
message appears only after the correct password. Successful sign-in resets the count.
SSO and service identities do not accumulate password lockouts. The last usable admin
cannot be locked, disabled or demoted. Admin password resets create temporary passwords;
the user must change one before opening other pages. Password changes revoke all sessions.

## Profile, settings and themes

The profile menu shows the account, deployment role, current project permission and groups,
the CLI sign-in line for this server, last sign-in in settings, keyboard shortcuts, help,
About and sign-out. Local users have Change password; SSO users do not.

Personal settings belong to the account: theme (`system`, `light`, `dark`), IANA timezone,
default project, page size (1–100) and relative or absolute times. The top-bar theme switch
saves to the same account setting. Both themes use the catalog's shared assets and data-flow
mark. Pins and saved filters will arrive with the list pages that use them.

Use Tab and Shift+Tab to move, the skip link to reach content, `?` for shortcuts, and Escape
to close the shortcut dialog or profile menu. Every project page names its environment and
governance; production has a stronger bordered badge as well as the word PRODUCTION.

```sh
etl-craft account settings --theme dark --timezone Asia/Kolkata --page-size 40
etl-craft account change-password
etl-craft account unlock --name alice
```

## Home, About and catalog

Home shows only authorized projects and pipeline work. Running runs and queued tasks are
current; failed runs and SLA misses use a rolling **last 24 h**, including the start and
excluding the end. The JSON supplies both UTC window boundaries. Counts have no links until
the filtered list pages exist. All counts come from the shared `PROJECT_HEALTH` read:

```sh
etl-craft project-health --json
etl-craft --project SALES project-health --json
etl-craft about --json
etl-craft --project SALES catalog page --path index.html --output catalog-page.html
```

About gives package, applied Engine DB schema, server and live controller versions, the
license and version-specific docs. It omits hosts, paths and connection details.

Catalog pages stream the project's existing `generate-docs` output through the same session;
a read never builds a catalog. Only whole-project viewers may open this static catalog:
it lists every pipeline, so pipeline-limited users must request whole-project access or use
authorized pipeline views. A filtered catalog is later work. The generated site's graphs,
search, tables, gaps and theme control remain one implementation, including offline mode.
`publish-docs` remains unchanged as the interim one-project server.

The corresponding API reads are `GET /api/v1/project-health`, `GET /api/v1/about`, and
`GET /api/v1/projects/{project}/catalog/page?path=index.html`. Catalog paths must be literal
relative generated HTML, CSS, JavaScript or SVG files, without traversal, encodings or symlinks.
CLI and API stream the same bytes.

## Upgrade

Run `etl-craft migrate` before starting the new server. Normal login and UI startup refuse
pending Engine migrations, naming the Engine DB key and applied/required migration. If no
admin session is available, the local `migrate` command prompts for the admin account and
password. Its upgrade-only proof reads the original account fields, issues no session and
never applies lockout counters; wrong passwords and non-admins are refused and audited. Engine migration
`0075_browser_accounts.sql` follows business calendars (0074) in both dialects. It adds
`FAILED_SIGNINS` (0), `LOCKED_DATE` (NULL), `MUST_CHANGE_PASSWORD` (N) and validated
`UI_SETTINGS` ({}) to existing `CFG_ACCOUNTS` rows. Existing passwords and accounts remain;
a later admin password reset sets the temporary-password requirement. The migration also
adds partial indexes for recent health counts and successful sign-in/catalog audit reads.
