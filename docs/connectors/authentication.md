# Authentication

Every connection names an `Auth_mode` and the fields that mode needs. The loader checks each
profile before anything connects: a mode the target does not offer, or a field that mode needs
and the profile lacks, stops the command with the field named.

| `Auth_mode` | Presents | Needs |
|---|---|---|
| `NONE` | nothing: a local file, or a service that needs no login | |
| `PASSWORD` | a user and a password | `User`, `Secret` |
| `TOKEN` | a stored access token | `Secret` (and `User` where the target needs one) |
| `KEY_FILE` | a private key: a client certificate (PostgreSQL, Trino) or a key pair (Snowflake) | `Key_file`, and `Secret` for its passphrase when it has one (Trino's client takes none) |
| `OAUTH` | a token fetched for each connection with a client-credentials grant | `Client_id`, `Secret`, `Token_url`, optional `Scope` |
| `SSO` | a login someone completes in a browser or device flow; for interactive use | the target's own fields, such as `Issuer` and `Client_id` |
| `STS` | a short-lived cloud credential: an AWS RDS IAM token, or Snowflake workload identity | `Region` and optional `Role_arn` (AWS) |

`Secret`, like every credential field, names a variable and is never a value; see
[Security](../deploying/security.md#secrets).

## Which target accepts which

- [Engine DB](engine-db.md): SQLite needs nothing; PostgreSQL accepts `PASSWORD`, `TOKEN`,
  `KEY_FILE`, `OAUTH`, `SSO` and `STS`.
- [Warehouses](warehouses.md#connecting-and-authenticating): each warehouse and table format,
  with its modes.
- The email relay (see [Email alerts](../guides/email-alerts.md)): `NONE`, `PASSWORD`, or
  `OAUTH` (SMTP XOAUTH2, for Microsoft 365 and Google), or no login with `sendmail`.

## Verified here

Some modes are run against a live service by this project's tests: PostgreSQL `PASSWORD` and
`KEY_FILE`; DuckDB `NONE`, and `OAUTH` for its Iceberg REST catalog; Trino `NONE`; Databricks
`TOKEN`; Snowflake `PASSWORD` and `TOKEN`; the email relay `NONE` and `PASSWORD`. The others
follow the vendor's documentation and can be used, but success is not guaranteed: `doctor` warns
about each one it finds in use.
