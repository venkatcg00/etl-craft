"""Frozen, typed archive snapshots shared by Engine DB and warehouse access."""

import base64
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal

from etl_craft.core.enums import ArchiveKind, ArchiveStep
from etl_craft.core.errors import ArchiveError
from etl_craft.core.text import is_safe_identifier, sha256_hex
from etl_craft.core.time import as_utc

Scalar = str | int | bool | None


@dataclass(frozen=True)
class ArchiveColumn:
    """The source name and lossless logical representation, independent of SQL types."""

    name: str
    kind: ArchiveKind

    def __post_init__(self) -> None:
        """Refuse unsafe persisted identifiers before SQL generation."""
        if not is_safe_identifier(self.name):
            raise ArchiveError(
                f"archive column={self.name!r}; expected a SQL identifier; "
                "restore the source schema"
            )


def encoded(column: ArchiveColumn, value: object, *, warehouse: bool = False) -> Scalar:
    """Normalize timestamps; preserve decimal/float precision and exact JSON/string bytes."""
    if value is None:
        return None
    if column.kind == ArchiveKind.TIMESTAMP:
        return as_utc(value).isoformat(timespec="microseconds")
    if column.kind == ArchiveKind.DATE:
        return value.isoformat() if isinstance(value, date) else str(value)
    if column.kind == ArchiveKind.BOOLEAN:
        return bool(value)
    if column.kind == ArchiveKind.INTEGER:
        return int(str(value))
    if column.kind == ArchiveKind.JSON and not warehouse:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
    if column.kind == ArchiveKind.BINARY and not warehouse:
        if not isinstance(value, bytes):
            raise ArchiveError(f"archive column {column.name}: expected bytes; restore source type")
        return base64.b64encode(value).decode()
    return str(value)


def bound(column: ArchiveColumn, value: Scalar) -> object:
    """Send native dates and UTC timestamps to warehouse drivers, exact text otherwise."""
    if value is None:
        return None
    if column.kind == ArchiveKind.TIMESTAMP:
        return datetime.fromisoformat(str(value))
    if column.kind == ArchiveKind.DATE:
        return date.fromisoformat(str(value))
    return value


def kind_of(python_type: type) -> ArchiveKind:
    """Map reflected source types without losing unconstrained numeric precision."""
    return {
        bool: ArchiveKind.BOOLEAN,
        int: ArchiveKind.INTEGER,
        datetime: ArchiveKind.TIMESTAMP,
        date: ArchiveKind.DATE,
        Decimal: ArchiveKind.DECIMAL,
        float: ArchiveKind.FLOAT,
        dict: ArchiveKind.JSON,
        list: ArchiveKind.JSON,
        bytes: ArchiveKind.BINARY,
    }.get(python_type, ArchiveKind.TEXT)


@dataclass(frozen=True)
class ArchivePart:
    """Rows from one source table; nullable compound keys remain explicit."""

    table: str
    columns: tuple[ArchiveColumn, ...]
    keys: tuple[str, ...]
    rows: tuple[tuple[Scalar, ...], ...]

    def __post_init__(self) -> None:
        """Validate captured table, keys and row widths."""
        names = {column.name for column in self.columns}
        if (
            not is_safe_identifier(self.table)
            or not self.table.startswith("AUD_")
            or not self.keys
            or not set(self.keys) <= names
            or any(len(row) != len(self.columns) for row in self.rows)
        ):
            raise ArchiveError(
                f"archive table={self.table!r}: invalid source shape; restore the admitted snapshot"
            )

    def key(self, row: tuple[Scalar, ...]) -> tuple[Scalar, ...]:
        """Select the stable source identity, never the row's current position."""
        names = [c.name for c in self.columns]
        return tuple(row[names.index(key)] for key in self.keys)

    def digest(self, row: tuple[Scalar, ...], version: str) -> str:
        """Namespace content versions by source schema as well as typed values."""
        return sha256_hex(
            json.dumps(
                [version, [(c.name, c.kind, v) for c, v in zip(self.columns, row, strict=True)]],
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        )


@dataclass(frozen=True)
class ArchiveBatch:
    """An immutable admitted snapshot; only its exact verified source rows may be changed."""

    batch_id: str
    origin: str
    version: str
    destination: str
    schema: str
    step: ArchiveStep
    query: str
    cutoff: str | None
    parts: tuple[ArchivePart, ...]

    def __post_init__(self) -> None:
        """Reject incomplete durable state before opening the destination."""
        if not self.parts or not is_safe_identifier(self.schema) or not self.version:
            raise ArchiveError(
                f"archive batch {self.batch_id!r}: invalid snapshot shape; expected "
                "a schema, version and source parts; restore the admitted snapshot"
            )

    def dump(self) -> str:
        """Persist the complete decision before warehouse writes start."""
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"), allow_nan=False)

    @classmethod
    def load(cls, value: str) -> "ArchiveBatch":
        """Refuse corrupt durable state instead of reconstructing a different decision."""
        try:
            data = json.loads(value)
            data["step"] = ArchiveStep(data["step"])
            data["parts"] = tuple(
                ArchivePart(
                    p["table"],
                    tuple(ArchiveColumn(c["name"], ArchiveKind(c["kind"])) for c in p["columns"]),
                    tuple(p["keys"]),
                    tuple(tuple(row) for row in p["rows"]),
                )
                for p in data["parts"]
            )
            return cls(**data)
        except (ValueError, TypeError, KeyError) as error:
            raise ArchiveError(
                "pending archive BODY is invalid; expected its original typed snapshot; "
                "restore the Engine DB snapshot before retrying purge"
            ) from error
