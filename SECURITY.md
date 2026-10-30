# Security policy

## Supported versions

Until 0.9.0, the first public release, security fixes go into the current development version
only. From 0.9.0, the newest minor release line receives security fixes as patch releases, and
fixes that need an Engine DB change ship with a migration that upgrades from every released
version.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability or include credentials, connection URLs,
customer data, or proof-of-concept payloads in a public report.

Report privately by emailing [venkatcg0@gmail.com](mailto:venkatcg0@gmail.com) with the subject
`[etl-craft security]`. Include the affected version or commit, a minimal reproduction, the
security impact, and any suggested mitigation. You will receive an acknowledgement and an
assessment when the report has been reviewed; this project does not make a response-time SLA.

Please allow time for a fix or mitigation to be prepared before public disclosure. If the issue
requires immediate customer action, include a safe workaround in the report where possible.

## Secure deployment basics

- Keep secret values outside `craft-connector.yml` and do not commit local secret files.
- Use least-privilege PostgreSQL and warehouse roles, and review changes to `CFG_` metadata and
  project migrations as executable code.
- Back up the Engine DB before upgrading, and run `etl-craft migrate` after installing a new
  version.
