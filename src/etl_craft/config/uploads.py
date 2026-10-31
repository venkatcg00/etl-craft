"""The project-wide ceiling for stewardship files."""

from dataclasses import dataclass
from pathlib import Path

from etl_craft.core.errors import ConfigurationError


@dataclass(frozen=True)
class Uploads:
    """Bound live upload storage before any pipeline-specific validation."""

    max_file_mb: int = 50

    def __post_init__(self) -> None:
        """Require a positive whole-megabyte ceiling."""
        if (
            isinstance(self.max_file_mb, bool)
            or not isinstance(self.max_file_mb, int)
            or self.max_file_mb < 1
        ):
            raise ConfigurationError(
                f"Uploads.Max_file_mb={self.max_file_mb!r}; expected a positive integer; "
                "set the maximum file size in MiB"
            )


def parse_uploads(raw: dict[str, object], path: Path) -> Uploads:
    """Parse the blanket limit without accepting unused configuration keys."""
    from etl_craft.config.loader_helpers import mapping, reject_unknown

    values = mapping(raw, "Uploads", path, required=False)
    reject_unknown(values, frozenset({"Max_file_mb"}), "Uploads", path)
    value = values.get("Max_file_mb", 50)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConfigurationError(
            f"Uploads.Max_file_mb={value!r}; expected a positive integer; "
            "set the maximum file size in MiB"
        )
    return Uploads(value)
