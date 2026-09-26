import pytest

from etl_craft.core import errors
from etl_craft.core.errors import EtlCraftError, ExitCode
from etl_craft.core.graph import CycleError, SelfDependencyError, UnknownTaskError

pytestmark = pytest.mark.unit

EXIT_CODES = {
    errors.EtlCraftError: ExitCode.UNEXPECTED,
    errors.ConfigurationError: ExitCode.CONFIGURATION,
    errors.UsageError: ExitCode.USAGE,
    errors.MetadataError: ExitCode.METADATA,
    errors.GraphError: ExitCode.GRAPH,
    SelfDependencyError: ExitCode.SELF_DEPENDENCY,
    CycleError: ExitCode.DEPENDENCY_CYCLE,
    UnknownTaskError: ExitCode.UNKNOWN_TASK,
    errors.RunStateError: ExitCode.RUN_STATE,
    errors.RunRefusedError: ExitCode.RUN_REFUSED,
    errors.ConnectionTestError: ExitCode.CONNECTION_TEST,
    errors.EngineDbError: ExitCode.ENGINE_DB,
    errors.MigrationError: ExitCode.MIGRATION,
    errors.LockTimeoutError: ExitCode.LOCK_TIMEOUT,
    errors.HandlerError: ExitCode.HANDLER,
    errors.CloningError: ExitCode.CLONING,
    errors.RemoteUnsupportedError: ExitCode.REMOTE_UNSUPPORTED,
}


def all_error_classes(root=EtlCraftError):
    found = {root}
    for subclass in root.__subclasses__():
        found |= all_error_classes(subclass)
    return found


def test_the_exit_codes():
    assert [(code.name, int(code)) for code in ExitCode] == [
        ("SUCCESS", 0),
        ("FAILURE", 1),
        ("USAGE", 2),
        ("CONFIGURATION", 3),
        ("METADATA", 4),
        ("GRAPH", 5),
        ("SELF_DEPENDENCY", 6),
        ("DEPENDENCY_CYCLE", 7),
        ("UNKNOWN_TASK", 8),
        ("RUN_STATE", 9),
        ("RUN_REFUSED", 10),
        ("CONNECTION_TEST", 11),
        ("ENGINE_DB", 12),
        ("MIGRATION", 13),
        ("LOCK_TIMEOUT", 14),
        ("HANDLER", 15),
        ("UNEXPECTED", 16),
        ("CLONING", 17),
        ("REMOTE_UNSUPPORTED", 18),
    ]


@pytest.mark.parametrize(
    ("error", "exit_code"), EXIT_CODES.items(), ids=lambda value: getattr(value, "__name__", "")
)
def test_each_error_carries_its_exit_code(error, exit_code):
    assert issubclass(error, EtlCraftError)
    assert error.exit_code is exit_code


def test_every_error_class_is_covered_and_has_its_own_code():
    classes = {cls for cls in all_error_classes() if cls.__module__.startswith("etl_craft.")}
    assert classes == set(EXIT_CODES)
    codes = [cls.exit_code for cls in classes]
    assert len(codes) == len(set(codes)), "two error classes share an exit code"
    # No error class claims the statuses reserved for outcomes.
    assert not {ExitCode.SUCCESS, ExitCode.FAILURE} & set(codes)


def test_an_instance_reports_its_class_code():
    assert errors.MetadataError("x").exit_code is ExitCode.METADATA
    assert CycleError((1, 2, 1)).exit_code is ExitCode.DEPENDENCY_CYCLE


def test_engine_db_errors_share_a_base():
    assert issubclass(errors.MigrationError, errors.EngineDbError)
    assert issubclass(errors.LockTimeoutError, errors.EngineDbError)


def test_an_error_keeps_its_message():
    with pytest.raises(EtlCraftError, match="no active pipeline 'P1'"):
        raise errors.MetadataError("no active pipeline 'P1'")


def test_the_exit_codes_page_lists_every_code():
    import re
    from pathlib import Path

    page = Path(__file__).parents[2] / "docs" / "reference" / "exit-codes.md"
    rows = re.findall(r"^\| `(\d+)` \| `([A-Z_]+)` \|", page.read_text(encoding="utf-8"), re.M)
    assert [(int(number), name) for number, name in rows] == [
        (int(code), code.name) for code in ExitCode
    ]
