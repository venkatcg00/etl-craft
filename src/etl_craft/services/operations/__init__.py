"""Actor-scoped operations shared by the CLI and Python callers."""

from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.serialization import to_json

__all__ = ["OperationContext", "to_json"]
