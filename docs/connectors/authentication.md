# Authentication

Every connection names an `auth_mode` and the fields that mode needs. The loader checks each
profile before anything connects: a mode the target does not offer, or a field that mode needs
and the profile lacks, stops the command with the field named.

| `auth_mode` | Presents | Needs |
|---|---|---|
| `none` | nothing: a local file, or a service that needs no login | |
| `password` | a user and a password | `user`, `secret` |
| `token` | a stored access token | `secret` (and `user` where the target needs one) |
| `key_file` | a private key: a client certificate (PostgreSQL, Trino) or a key pair (Snowflake) | `key_file`, and `secret` for its passphrase when it has one (Trino's client takes none) |
| `oauth` | a token fetched for each connection with a client-credentials grant | `client_id`, `secret`, `token_url`, optional `scope` |
| `sso` | a login someone completes in a browser or device flow; for interactive use | the target's own fields, such as `issuer` and `client_id` |
| `sts` | a short-lived cloud credential: an AWS RDS IAM token, or Snowflake workload identity | `region` and optional `role_arn` (AWS) |

`secret`, like every credential field, names a variable and is never a value; see
[Security](../deploying/security.md#secrets).

## Which target accepts which

- [Engine DB](engine-db.md): SQLite needs nothing; PostgreSQL accepts `password`, `token`,
  `key_file`, `oauth`, `sso` and `sts`.
- [Warehouses](warehouses.md#connecting-and-authenticating): each warehouse and table format,
  with its modes.
- The email relay (see [Email alerts](../guides/email-alerts.md)): `none`, `password`, or
  `oauth` (SMTP XOAUTH2, for Microsoft 365 and Google), or no login with `sendmail`.

## Verified here

Some modes are run against a live service by this project's tests: PostgreSQL `password` and
`key_file`; DuckDB `none`, and `oauth` for its Iceberg REST catalog; Trino `none`; Databricks
`token`; Snowflake `password` and `token`; the email relay `none` and `password`. The others
follow the vendor's documentation and can be used, but success is not guaranteed: `doctor` warns
about each one it finds in use.
