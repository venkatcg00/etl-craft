"""Physical process exit and independently confirmed warehouse query endings."""

import logging

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import EtlCraftError
from etl_craft.warehouse.connection import warehouse_dialect
from etl_craft.warehouse.fencing import confirm_stopped

logger = logging.getLogger(__name__)


def confirm(config: ConnectorConfig, tag: str) -> str | None:
    """Return an actionable pending reason, never an acknowledgement guessed from process exit."""
    if config.warehouse is None or warehouse_dialect(config).process_local_queries:
        logger.info("%s: embedded warehouse query ended with its process", tag)
        return None
    if not warehouse_dialect(config).session_fencing:
        reason = (
            f"{tag}: only the process stopped; {warehouse_dialect(config).spec.display_name} "
            "has no query cancellation adapter; expected confirmed warehouse query endings; "
            "stop its queries in the warehouse before retrying"
        )
        logger.error("%s", reason)
        return reason
    try:
        confirm_stopped(config, tag)
    except EtlCraftError as error:
        reason = f"{tag}: warehouse cancellation remains pending: {error}"
        logger.error("%s", reason)
        return reason
    logger.info("%s: warehouse queries confirmed stopped", tag)
    return None
