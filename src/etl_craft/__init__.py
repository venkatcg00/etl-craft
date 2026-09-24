"""etl-craft: a metadata-driven ETL orchestration engine."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("etl-craft")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
