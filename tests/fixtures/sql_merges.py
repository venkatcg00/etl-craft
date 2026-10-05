"""Shared row-content assertions for joined merge updates on local and cloud warehouses."""


def check_composite_merge(w, target, kind):
    source = (
        "SELECT 1 AS id, CAST('eu' AS VARCHAR(20)) AS region, "
        "CAST('old' AS VARCHAR(20)) AS name, CAST('Rome' AS VARCHAR(20)) AS city "
        "UNION ALL SELECT 1, 'us', 'old', 'Rome'"
    )
    w.setup(target, source, kind)
    params = {
        "SQL_ACTION": kind,
        "TARGET_OBJECT": target,
        "MERGE_KEY": "id|region",
        "MERGE_COMPARE_COLUMNS": "name|city",
    }
    if kind == "SCD1_MERGE":
        params["PRESERVE_TARGET"] = "true"
    w.run(target, SOURCE_SQL=source, **params)
    before = w.rows(f"SELECT * FROM {w.name(target)} WHERE region = 'us'")
    changed_source = source.replace(
        "CAST('Rome' AS VARCHAR(20)) AS city", "CAST('Paris' AS VARCHAR(20)) AS city"
    )
    if kind == "SCD1_MERGE":
        changed_source = changed_source.replace(
            "CAST('old' AS VARCHAR(20)) AS name", "CAST(NULL AS VARCHAR(20)) AS name"
        )
    changed = w.run(target, SOURCE_SQL=changed_source, **params)
    assert (changed.update_count, changed.insert_count) == (1, 0 if kind == "SCD1_MERGE" else 1)
    assert w.rows(f"SELECT * FROM {w.name(target)} WHERE region = 'us'") == before
    if kind == "SCD1_MERGE":
        assert w.rows(f"SELECT region, name, city FROM {w.name(target)} ORDER BY region") == [
            ("eu", "old", "Paris"),
            ("us", "old", "Rome"),
        ]
    else:
        assert w.rows(
            f"SELECT region, name, city, active_flag FROM {w.name(target)} "
            "ORDER BY region, active_flag"
        ) == [("eu", "old", "Rome", "N"), ("eu", "old", "Paris", "Y"), ("us", "old", "Rome", "Y")]
    again = w.run(target, SOURCE_SQL=changed_source, **params)
    assert (again.update_count, again.insert_count) == (0, 0)
