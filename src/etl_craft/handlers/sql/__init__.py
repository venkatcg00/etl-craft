"""``HANDLER=SQL``: wrap the task's read-only SELECT in one of seven actions on the warehouse.

The task supplies a SELECT, inline in ``SOURCE_SQL`` or as a file under the project's
``sql_files/`` in ``SOURCE_SQL_FILE``, and names its ``SQL_ACTION`` and ``TARGET_OBJECT``; the
engine owns every write. The parameters are checked first (``spec``), then the action runs in
one warehouse transaction (``actions``), with every statement logged (``session``).
"""

from __future__ import annotations

import logging

from sqlalchemy.engine import Engine

from etl_craft.config.targets import active_catalog, parse_warehouse_url
from etl_craft.core.enums import TableFormat
from etl_craft.core.errors import ConfigurationError, HandlerError
from etl_craft.dialects.warehouse import WarehouseDialect, resolve
from etl_craft.handlers.registry import HandlerResult, TaskContext
from etl_craft.handlers.sql.actions import ACTIONS, ActionContext, utc_now
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.spec import read_sql_task
from etl_craft.warehouse.connection import open_warehouse

logger = logging.getLogger(__name__)


def run(context: TaskContext, engine_db: Engine) -> HandlerResult:
    """Run the SQL task described by ``context`` and return its counts."""
    task = read_sql_task(context)
    config = context.config
    if config.warehouse is None:
        raise ConfigurationError("a SQL task needs a Warehouse section in craft-connector.yml")
    dialect = task_dialect(context)
    problem = dialect.task_storage_problem(context.task_params)
    if problem is not None:
        raise HandlerError(problem)
    catalog = active_catalog(config)
    logger.info(
        "%s %s from %s on %s",
        task.action,
        f"{catalog}.{task.target_object}",
        task.source,
        dialect.display_name,
    )
    logger.debug("the SELECT:\n%s", task.select_sql)
    with open_warehouse(config, engine_db) as warehouse, warehouse.begin() as conn:
        session = Session(
            conn,
            dialect,
            catalog=catalog,
            action=task.action,
            target_object=task.target_object,
            task_run_id=context.task_run_id,
            params=context.task_params,
        )
        action = ActionContext(
            task=task,
            context=context,
            engine_db=engine_db,
            user=config.warehouse.active.user or "",
            now=utc_now(),
        )
        try:
            result = ACTIONS[task.action](session, action)
        finally:
            session.sweep()
    return result


def task_dialect(context: TaskContext) -> WarehouseDialect:
    """Choose the dialect: the warehouse connection and the task's table format.

    The format is the task's ``TABLE_FORMAT`` parameter, else ``Warehouse.Table_format``. Where
    the connection fixes the format (Trino, DuckDB over Iceberg), a task cannot choose another.
    """
    config = context.config
    assert config.warehouse is not None
    default = config.warehouse_table_format
    written = (context.task_params.get("TABLE_FORMAT") or "").strip().lower()
    formats = [member.value for member in TableFormat]
    if written and written not in formats:
        raise HandlerError(f"TABLE_FORMAT={written!r} is not one of {', '.join(formats)}")
    table_format = written or default
    url = parse_warehouse_url(config.warehouse.active.jdbc_url)
    dialect = resolve(url.dialect, table_format)
    if not dialect.per_task_format and table_format != default:
        raise HandlerError(
            f"on {dialect.display_name} the table format is fixed by the connection "
            f"(Warehouse.Table_format={default}); TABLE_FORMAT={written} cannot change it"
        )
    return dialect
