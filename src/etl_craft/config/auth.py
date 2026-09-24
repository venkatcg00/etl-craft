"""What each Engine DB, warehouse and mail relay accepts, as data.

The loader checks every connection profile against these tables before anything connects: an
auth mode a target does not offer, or a field that mode needs and the profile lacks, is a
configuration error. The dialects read the same tables, so the loader and the connection code
cannot disagree.

Each ``auth_fields`` table maps an auth mode to the profile fields it needs beyond the connection
itself; its keys are the modes the target accepts. ``verified_auth_modes`` are the modes run
against a live service in this project; the others follow the vendor's documentation and are
untested, which ``doctor`` reports.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from etl_craft.core.enums import AuthMode, TableFormat
from etl_craft.core.errors import ConfigurationError

AuthFields = Mapping[str, tuple[str, ...]]

AUTH_EXTRA_FIELDS = (
    "key_file",
    "cert_file",
    "client_id",
    "token_url",
    "scope",
    "issuer",
    "region",
    "role_arn",
)
"""Profile fields that configure an auth mode rather than the connection."""

SECRET_AUTH_MODES = frozenset({AuthMode.PASSWORD, AuthMode.TOKEN, AuthMode.OAUTH})
"""Auth modes that always present a secret; the others need one only when a profile names one."""

POSTGRES_AUTH_FIELDS: AuthFields = {
    # A password.
    "password": ("user", "secret"),
    # A client certificate: key_file (and cert_file); secret is the key's passphrase.
    "key_file": ("user", "key_file", "secret"),
    # A stored bearer token presented as the password, such as a pre-issued Entra ID token.
    "token": ("user", "secret"),
    # A token minted per connection by a client-credentials grant, presented as the password.
    "oauth": ("user", "client_id", "secret", "token_url"),
    # libpq's OAuth device flow: someone completes the login, so it suits interactive use.
    "sso": ("user", "issuer", "client_id"),
    # An AWS RDS or Aurora IAM auth token, optionally as an assumed role.
    "sts": ("user", "region"),
}

EMAIL_AUTH_FIELDS: AuthFields = {
    "none": (),
    "password": ("user", "secret"),
    # SMTP XOAUTH2 with a client-credentials access token (Microsoft 365, Google).
    "oauth": ("user", "client_id", "secret", "token_url"),
}
EMAIL_VERIFIED_AUTH_MODES = frozenset({"none", "password"})


@dataclass(frozen=True)
class EngineSpec:
    """An Engine DB the engine can keep its metadata in."""

    name: str
    display_name: str
    jdbc_prefix: str
    auth_fields: AuthFields
    verified_auth_modes: frozenset[str]

    @property
    def auth_modes(self) -> frozenset[str]:
        """The auth modes this Engine DB accepts."""
        return frozenset(self.auth_fields)


ENGINES: tuple[EngineSpec, ...] = (
    EngineSpec(
        name="postgresql",
        display_name="PostgreSQL",
        jdbc_prefix="jdbc:postgresql:",
        auth_fields=POSTGRES_AUTH_FIELDS,
        verified_auth_modes=frozenset({"password"}),
    ),
    EngineSpec(
        name="sqlite",
        display_name="SQLite",
        jdbc_prefix="jdbc:sqlite:",
        auth_fields={"none": ()},
        verified_auth_modes=frozenset({"none"}),
    ),
)

ENGINE_NAMES = {"postgres": "postgresql", "postgresql": "postgresql", "sqlite": "sqlite"}
"""``Engine.Name`` spellings, lower-cased, and the Engine DB each selects."""


def engine_for_jdbc_url(jdbc_url: str) -> EngineSpec:
    """Return the Engine DB a JDBC URL names; raises ``ConfigurationError`` for any other."""
    lowered = jdbc_url.strip().lower()
    for spec in ENGINES:
        if lowered.startswith(spec.jdbc_prefix):
            return spec
    raise ConfigurationError(
        f"{jdbc_url!r} is not a supported Engine DB URL — use jdbc:postgresql://... "
        "(recommended for production) or jdbc:sqlite:<path>"
    )


GENERIC_AUTH_FIELDS: AuthFields = {
    "none": (),
    "password": ("user", "secret"),
    "token": ("user", "secret"),
}
"""What a warehouse without its own dialect accepts."""


@dataclass(frozen=True)
class WarehouseSpec:
    """One warehouse dialect: a database and the table format it writes.

    ``preferred_fields`` are the separate connection fields a profile may give instead of a
    whole JDBC URL; ``profile_fields`` are further settings the dialect reads from the profile.
    ``per_task_format`` says whether a task may choose another table format than the
    warehouse's own.
    """

    key: str
    display_name: str
    sqlalchemy_name: str
    table_format: TableFormat
    auth_fields: AuthFields = field(default_factory=lambda: dict(GENERIC_AUTH_FIELDS))
    verified_auth_modes: frozenset[str] = frozenset()
    preferred_fields: tuple[str, ...] = ()
    profile_fields: tuple[str, ...] = ()
    per_task_format: bool = True
    known: bool = True

    @property
    def auth_modes(self) -> frozenset[str]:
        """The auth modes this warehouse accepts."""
        return frozenset(self.auth_fields)


_DUCKDB_ICEBERG_PROFILE_FIELDS = (
    "catalog",
    "catalog_uri",
    "iceberg_warehouse",
    "s3_endpoint",
    "s3_region",
    "s3_url_style",
    "s3_use_ssl",
    "s3_key_id",
    "s3_secret",
)

_DATABRICKS_AUTH_FIELDS: AuthFields = {
    "token": ("secret",),
    # An OAuth machine-to-machine grant for a service principal.
    "oauth": ("client_id", "secret"),
    # The connector's browser login: interactive only.
    "sso": (),
}

_SNOWFLAKE_AUTH_FIELDS: AuthFields = {
    "password": ("user", "secret"),
    # A programmatic access token.
    "token": ("user", "secret"),
    # Key-pair login: key_file is the private key, secret its passphrase.
    "key_file": ("user", "key_file", "secret"),
    # The connector's own client-credentials flow.
    "oauth": ("client_id", "secret", "token_url"),
    # The external browser login: interactive only.
    "sso": ("user",),
    # Workload identity through the ambient AWS identity: no stored secret.
    "sts": (),
}

WAREHOUSES: tuple[WarehouseSpec, ...] = (
    WarehouseSpec(
        key="postgres",
        display_name="Postgres",
        sqlalchemy_name="postgresql",
        table_format=TableFormat.NATIVE,
        auth_fields=POSTGRES_AUTH_FIELDS,
        verified_auth_modes=frozenset({"password"}),
    ),
    WarehouseSpec(
        key="duckdb",
        display_name="DuckDB",
        sqlalchemy_name="duckdb",
        table_format=TableFormat.NATIVE,
        auth_fields={"none": ()},
        verified_auth_modes=frozenset({"none"}),
        per_task_format=False,
    ),
    WarehouseSpec(
        key="duckdb_iceberg",
        display_name="DuckDB",
        sqlalchemy_name="duckdb",
        table_format=TableFormat.ICEBERG,
        # How DuckDB authenticates to the REST catalog; object storage has its own s3_* fields.
        auth_fields={
            "none": (),
            "token": ("secret",),
            "oauth": ("client_id", "secret", "token_url"),
        },
        verified_auth_modes=frozenset({"none", "oauth"}),
        profile_fields=_DUCKDB_ICEBERG_PROFILE_FIELDS,
        per_task_format=False,
    ),
    WarehouseSpec(
        key="trino_iceberg",
        display_name="Trino",
        sqlalchemy_name="trino",
        table_format=TableFormat.ICEBERG,
        auth_fields={
            "none": (),
            "password": ("user", "secret"),
            "token": ("secret",),
            "oauth": ("client_id", "secret", "token_url"),
            # The cluster's OAuth 2.0 browser redirect: interactive only.
            "sso": (),
            # A client certificate; Trino's client takes no key passphrase.
            "key_file": ("key_file", "cert_file"),
        },
        verified_auth_modes=frozenset({"none"}),
        per_task_format=False,
    ),
    WarehouseSpec(
        key="databricks",
        display_name="Databricks",
        sqlalchemy_name="databricks",
        table_format=TableFormat.NATIVE,
        auth_fields=_DATABRICKS_AUTH_FIELDS,
        verified_auth_modes=frozenset({"token"}),
        preferred_fields=("jdbc_url", "catalog", "schema", "token"),
    ),
    WarehouseSpec(
        key="databricks_iceberg",
        display_name="Databricks",
        sqlalchemy_name="databricks",
        table_format=TableFormat.ICEBERG,
        auth_fields=_DATABRICKS_AUTH_FIELDS,
        verified_auth_modes=frozenset({"token"}),
        preferred_fields=("jdbc_url", "catalog", "schema", "token"),
    ),
    WarehouseSpec(
        key="snowflake",
        display_name="Snowflake",
        sqlalchemy_name="snowflake",
        table_format=TableFormat.NATIVE,
        auth_fields=_SNOWFLAKE_AUTH_FIELDS,
        verified_auth_modes=frozenset({"password", "token"}),
        preferred_fields=("user", "account", "database", "schema", "warehouse", "role", "token"),
    ),
    WarehouseSpec(
        key="snowflake_iceberg",
        display_name="Snowflake",
        sqlalchemy_name="snowflake",
        table_format=TableFormat.ICEBERG,
        auth_fields=_SNOWFLAKE_AUTH_FIELDS,
        verified_auth_modes=frozenset({"password", "token"}),
        preferred_fields=("user", "account", "database", "schema", "warehouse", "role", "token"),
    ),
)

WAREHOUSE_NAMES = {
    "postgres": "postgresql",
    "duckdb": "duckdb",
    "trino": "trino",
    "databricks": "databricks",
    "snowflake": "snowflake",
}
"""``Warehouse.Name`` spellings, lower-cased, and the SQLAlchemy dialect each selects."""


def warehouse_spec(sqlalchemy_name: str, table_format: str) -> WarehouseSpec:
    """Choose the warehouse dialect for a connection's SQLAlchemy name and a table format.

    Trino is Iceberg whichever format is asked for, because its catalog decides. PostgreSQL has
    no Iceberg tables, so asking for them is a ``ConfigurationError``. A database with no
    dialect of its own is treated as a plain ANSI warehouse (``known`` is false).
    """
    name = sqlalchemy_name.split("+", 1)[0]
    if name == "trino":
        return _BY_KEY["trino_iceberg"]
    if name == "postgresql" and table_format == TableFormat.ICEBERG:
        raise ConfigurationError(
            "PostgreSQL tables are always native: TABLE_FORMAT iceberg is not available on a "
            "Postgres warehouse (there is no postgres_iceberg dialect)"
        )
    for spec in WAREHOUSES:
        if spec.sqlalchemy_name == name and spec.table_format == table_format:
            return spec
    return WarehouseSpec(
        key=name,
        display_name=name,
        sqlalchemy_name=name,
        table_format=TableFormat(table_format),
        known=False,
    )


def warehouse_by_key(key: str) -> WarehouseSpec:
    """Return the warehouse dialect registered under ``key``, such as ``trino_iceberg``."""
    return _BY_KEY[key]


_BY_KEY = {spec.key: spec for spec in WAREHOUSES}
