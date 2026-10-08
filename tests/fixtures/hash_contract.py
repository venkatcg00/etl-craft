"""The same canonical scalar values on local and cloud warehouses."""


def golden_sql(dialect):
    aware = (
        "from_iso8601_timestamp_nanos('2020-01-02T08:34:05.123456+05:30')"
        if dialect.spec.key == "trino_iceberg"
        else "TIMESTAMPTZ '2020-01-02 08:34:05.123456+05:30'"
    )
    aware_type = "TIMESTAMP WITH TIME ZONE"
    naive = "TIMESTAMP '2020-01-02 03:04:05.123456'"
    naive_type = "TIMESTAMP"
    if dialect.spec.key.startswith("snowflake"):
        aware = "TO_TIMESTAMP_TZ('2020-01-02 08:34:05.123456+05:30')"
        aware_type = "TIMESTAMP_TZ"
        naive = "TO_TIMESTAMP_NTZ('2020-01-02 03:04:05.123456')"
        naive_type = "TIMESTAMP_NTZ"
    elif dialect.spec.key.startswith("databricks"):
        aware = "CAST('2020-01-02T08:34:05.123456+05:30' AS TIMESTAMP)"
        aware_type = "TIMESTAMP"
        naive = "CAST('2020-01-02 03:04:05.123456' AS TIMESTAMP_NTZ)"
        naive_type = "TIMESTAMP_NTZ"
    return dialect.hash_expression(
        [
            f"CAST(NULL AS {dialect.string_type})",
            "''",
            "'a|b:c'",
            "'é😀'",
            naive,
            aware,
            "CAST(12.34 AS DECIMAL(12,4))",
            "false",
            "DATE '2020-01-02'",
        ],
        [
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            naive_type,
            aware_type,
            "DECIMAL(12,4)",
            "BOOLEAN",
            "DATE",
        ],
    )
