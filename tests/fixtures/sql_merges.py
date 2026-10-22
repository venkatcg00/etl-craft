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
    w.run(target, SELECT_QUERY=source, **params)
    before = w.rows(f"SELECT * FROM {w.name(target)} WHERE region = 'us'")
    changed_source = source.replace(
        "CAST('Rome' AS VARCHAR(20)) AS city", "CAST('Paris' AS VARCHAR(20)) AS city"
    )
    if kind == "SCD1_MERGE":
        changed_source = changed_source.replace(
            "CAST('old' AS VARCHAR(20)) AS name", "CAST(NULL AS VARCHAR(20)) AS name"
        )
    changed = w.run(target, SELECT_QUERY=changed_source, **params)
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
    again = w.run(target, SELECT_QUERY=changed_source, **params)
    assert (again.update_count, again.insert_count) == (0, 0)


MERGE_FAULTS = [
    ("SCD1_MERGE", "sql.scd1.after_update"),
    ("SCD1_MERGE", "sql.scd1.after_insert"),
    ("SCD1_MERGE", "sql.scd1.after_commit"),
    ("SCD2_MERGE", "sql.scd2.after_close"),
    ("SCD2_MERGE", "sql.scd2.after_versions"),
    ("SCD2_MERGE", "sql.scd2.after_insert"),
    ("SCD2_MERGE", "sql.scd2.after_commit"),
]


def merge_rows(world, target):
    from sqlalchemy import text

    with world.warehouse.connect() as conn:
        return [
            {key.lower(): value for key, value in row.items()}
            for row in conn.execute(
                text(f"SELECT * FROM {world.name(target)} ORDER BY ROW_ID")
            ).mappings()
        ]


def check_killed_merge(world, kind, point):
    """Recover a killed merge to independently specified rows, hashes and identities."""
    import hashlib
    import multiprocessing

    from fixtures.lock_fencing import expire_leases
    from fixtures.sql_warehouse import run_killed_sql

    w = world
    target = "crash_people"
    source = "SELECT 1 AS id, CAST('old' AS VARCHAR(20)) AS name UNION ALL SELECT 2, 'same'"
    w.setup(target, source, kind)
    params = {
        "SQL_ACTION": kind,
        "TARGET_OBJECT": target,
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
    }
    w.run("seed", SELECT_QUERY=source, **params)
    before = merge_rows(w, target)
    w.new_run()
    params["SELECT_QUERY"] = (
        "SELECT 1 AS id, CAST('new' AS VARCHAR(20)) AS name "
        "UNION ALL SELECT 2, 'same' UNION ALL SELECT 3, 'add'"
    )
    context = w.task("merge", **params)
    w.warehouse.dispose()
    process = multiprocessing.get_context("spawn").Process(
        target=run_killed_sql, args=(context, point)
    )
    try:
        process.start()
        process.join(60)
        assert process.exitcode == 137, point
    finally:
        if process.is_alive():
            process.kill()
            process.join(5)
    expire_leases(w.engine_db)
    w.run("merge", **params)
    rows = merge_rows(w, target)
    expected = [(1, "new", "Y"), (2, "same", "Y"), (3, "add", "Y")]
    if kind == "SCD2_MERGE":
        expected.append((1, "old", "N"))
    assert sorted((r["id"], r["name"], r["active_flag"]) for r in rows) == sorted(expected)
    assert len({r["row_id"] for r in rows}) == len(expected)
    assert next(r for r in rows if r["id"] == 2) == next(r for r in before if r["id"] == 2)
    for row in rows:
        assert (
            row["hash_key"]
            == hashlib.md5(f"V{len(row['name'])}:{row['name']}".encode()).hexdigest()
        )
        assert row["delete_flag"] == "N"
        assert row["create_date"] <= row["update_date"]
        if row["id"] != 2:
            assert row["pipeline_id"] == w.pipeline_id
            assert row["pipeline_run_id"] == w.pipeline_run_id
            assert row["task_run_id"] == context.task_run_id
    again = w.run("merge", **params)
    assert again.insert_count == again.update_count == 0
    assert merge_rows(w, target) == rows
