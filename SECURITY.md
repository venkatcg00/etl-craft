# Security policy

etl-craft is currently an Alpha project. Security fixes are assessed against the current
development version until a versioned support policy is published.

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
- Apply updates through the documented backup and migration process in
  [docs/operations.md](docs/operations.md).
