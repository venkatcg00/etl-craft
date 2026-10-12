"""SCD row lifecycle checks shared by local and cloud warehouses."""

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.handlers.sql.tables import AUDIT_COLUMNS
from etl_craft.services.upgrade_targets import upgrade_targets
from etl_craft.warehouse.connection import warehouse_dialect


def check_scd_lifecycle(w, target, kind):
    shape = "SELECT 1 AS id, CAST('Ann' AS VARCHAR(20)) AS name"
    w.setup(target, shape, kind)
    params = {
        "SQL_ACTION": kind,
        "TARGET_OBJECT": target,
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
    }
    w.run(f"load_{target}", SOURCE_SQL=shape, **params)
    before = w.rows(f"SELECT * FROM {w.name(target)} ORDER BY row_id")
    unchanged = w.run(f"load_{target}", SOURCE_SQL=shape, **params)
    assert (unchanged.insert_count, unchanged.update_count) == (0, 0)
    assert w.rows(f"SELECT * FROM {w.name(target)} ORDER BY row_id") == before

    changed_shape = shape.replace("'Ann'", "'Anna'")
    w.run(f"load_{target}", SOURCE_SQL=changed_shape, **params)
    history = w.rows(f"SELECT * FROM {w.name(target)} WHERE active_flag = 'N' ORDER BY row_id")
    creation = w.rows(
        f"SELECT create_date, created_by FROM {w.name(target)} WHERE active_flag = 'Y'"
    )
    deletion = {
        "SQL_ACTION": "DELETE_ROWS",
        "TARGET_OBJECT": target,
        "MERGE_KEY": "id",
        "SOURCE_SQL": "SELECT 1 AS id",
    }
    assert w.run(f"delete_{target}", **deletion).delete_count == 1
    assert w.rows(
        f"SELECT active_flag, delete_flag FROM {w.name(target)} WHERE delete_flag = 'Y'"
    ) == [("N", "Y")]
    assert (
        w.rows(f"SELECT create_date, created_by FROM {w.name(target)} WHERE delete_flag = 'Y'")
        == creation
    )
    assert (
        w.rows(f"SELECT * FROM {w.name(target)} WHERE delete_flag = 'N' ORDER BY row_id") == history
    )
    deleted = w.rows(f"SELECT * FROM {w.name(target)} ORDER BY row_id")
    assert w.run(f"delete_{target}", **deletion).delete_count == 0
    assert w.rows(f"SELECT * FROM {w.name(target)} ORDER BY row_id") == deleted

    revived = w.run(f"load_{target}", SOURCE_SQL=changed_shape, **params)
    assert (revived.update_count, revived.insert_count) == (
        (1, 0) if kind == "SCD1_MERGE" else (0, 1)
    )
    assert w.rows(
        f"SELECT name, active_flag, delete_flag FROM {w.name(target)} WHERE active_flag = 'Y'"
    ) == [("Anna", "Y", "N")]
    if kind == "SCD2_MERGE":
        assert (
            w.rows(f"SELECT * FROM {w.name(target)} WHERE active_flag = 'N' ORDER BY row_id")
            == deleted
        )
        # Older soft deletes left the deleted current version active.
        w.execute(f"UPDATE {w.name(target)} SET DELETE_FLAG = 'Y' WHERE ACTIVE_FLAG = 'Y'")
        legacy = w.rows(f"SELECT * FROM {w.name(target)} ORDER BY row_id")
        selected = f"{w.schema}.{target}"
        assert upgrade_targets(w.engine_db, w.config, target=selected, dry_run=True)[0].changed
        assert w.rows(f"SELECT * FROM {w.name(target)} ORDER BY row_id") == legacy
        assert upgrade_targets(w.engine_db, w.config, target=selected)[0].changed
        position = w.columns(target).index("active_flag")
        expected = [(*row[:position], "N", *row[position + 1 :]) for row in legacy]
        assert w.rows(f"SELECT * FROM {w.name(target)} ORDER BY row_id") == expected
        assert not upgrade_targets(w.engine_db, w.config, target=selected)[0].changed
    else:
        assert w.rows(f"SELECT create_date, created_by FROM {w.name(target)}") == creation


def create_legacy_scd1(w, target):
    """Create the legacy audited shape directly, retaining its generated row identities."""
    shape = "SELECT 1 AS id, CAST('Ann' AS VARCHAR(20)) AS name UNION ALL SELECT 2, 'Bo'"
    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(
            AUDIT_COLUMNS,
            "SCD1_MERGE",
            tuple(c for c in AUDIT_COLUMNS["SCD1_MERGE"] if c != "ACTIVE_FLAG"),
        )
        w.setup(target, shape, "SCD1_MERGE")
    params = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": target,
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
        "SOURCE_SQL": shape,
    }
    context = w.task(f"load_{target}", **params)
    dialect = warehouse_dialect(w.config)
    row_id_column = ", ROW_ID" if dialect.surrogate_key == "computed" else ""
    row_id_value = ", id" if row_id_column else ""
    w.execute(
        f"INSERT INTO {w.name(target)} (id, name, PIPELINE_ID, PIPELINE_RUN_ID, TASK_RUN_ID, "
        f"HASH_KEY, CREATE_DATE, CREATED_BY, UPDATE_DATE, UPDATED_BY, DELETE_FLAG{row_id_column}) "
        f"SELECT id, name, {context.pipeline_id}, {context.pipeline_run_id}, "
        f"{context.task_run_id}, "
        "'00000000000000000000000000000000', CURRENT_TIMESTAMP, 'legacy', "
        f"CURRENT_TIMESTAMP, 'legacy', 'N'{row_id_value} FROM ({shape}) s"
    )
    return params


def check_scd1_upgrade(w, target):
    params = create_legacy_scd1(w, target)
    w.execute(f"UPDATE {w.name(target)} SET DELETE_FLAG = 'Y' WHERE id = 2")
    fields = "id, name, row_id, create_date, created_by, update_date, updated_by, delete_flag"
    before = w.rows(f"SELECT {fields} FROM {w.name(target)} ORDER BY id")
    with pytest.raises(HandlerError, match=r"ACTIVE_FLAG.*upgrade-targets"):
        w.run(f"load_{target}", **params)
    selected = f"{w.schema}.{target}"
    dry = upgrade_targets(w.engine_db, w.config, target=selected, dry_run=True)[0]
    assert dry.columns == ("ACTIVE_FLAG",) and dry.changed
    assert "active_flag" not in w.columns(target)
    result = upgrade_targets(w.engine_db, w.config, target=selected)[0]
    assert result.columns == ("ACTIVE_FLAG",) and result.changed
    assert w.rows(f"SELECT id, active_flag FROM {w.name(target)} ORDER BY id") == [
        (1, "Y"),
        (2, "N"),
    ]
    assert w.rows(f"SELECT {fields} FROM {w.name(target)} ORDER BY id") == before
    assert not upgrade_targets(w.engine_db, w.config, target=selected)[0].changed
    # A non-transactional warehouse may lose the process between ADD and backfill.
    w.execute(f"UPDATE {w.name(target)} SET ACTIVE_FLAG = NULL WHERE id = 2")
    assert upgrade_targets(w.engine_db, w.config, target=selected)[0].changed
    assert w.rows(f"SELECT id, active_flag FROM {w.name(target)} ORDER BY id") == [
        (1, "Y"),
        (2, "N"),
    ]
    assert w.rows(f"SELECT {fields} FROM {w.name(target)} ORDER BY id") == before
