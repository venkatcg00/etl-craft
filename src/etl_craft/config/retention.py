"""Explicit history lifetimes; absent periods preserve existing installations."""

import re
from pathlib import Path

from etl_craft.config.loader_helpers import profiled_settings, reject_unknown, whole_number
from etl_craft.config.model import Retention
from etl_craft.config.resolve import Resolver
from etl_craft.core.errors import ConfigurationError

KEYS = frozenset(
    {
        "Run_history_days",
        "Attempt_log_days",
        "Consumption_days",
        "Interventions_days",
        "Config_history_days",
        "Daily_at",
    }
)


def parse_retention(
    raw: dict[str, object], profile: str | None, path: Path, resolver: Resolver
) -> Retention:
    """Reject guessed units and invalid wall-clock times before a deletion can start."""
    settings = profiled_settings("Retention", raw, profile, path, resolver, nested=frozenset())
    reject_unknown(settings.settings, KEYS, "Retention", path)
    daily = settings.text("Daily_at")
    if daily is not None and not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", daily):
        raise ConfigurationError(
            f"{path}: Retention.Daily_at={daily!r}; expected HH:MM in 24-hour time; "
            "set e.g. '03:00' or omit it for manual purge"
        )
    return Retention(
        **{
            key.lower(): whole_number(settings, key, None, path, minimum=1, maximum=36500)
            for key in sorted(KEYS)
            if key != "Daily_at"
        },
        daily_at=daily,
    )
