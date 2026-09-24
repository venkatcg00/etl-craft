import pytest

from etl_craft.core import errors
from etl_craft.core.errors import EtlCraftError, ExitCode

pytestmark = pytest.mark.unit


EXIT_CODES = {
    errors.EtlCraftError: ExitCode.FAILURE,
    errors.ConfigurationError: ExitCode.USAGE,
    errors.UsageError: ExitCode.USAGE,
    errors.MetadataError: ExitCode.FAILURE,
    errors.GraphError: ExitCode.FAILURE,
    errors.RunStateError: ExitCode.FAILURE,
    errors.RunRefusedError: ExitCode.FAILURE,
    errors.ConnectionTestError: ExitCode.FAILURE,
    errors.EngineDbError: ExitCode.FAILURE,
    errors.MigrationError: ExitCode.FAILURE,
    errors.LockTimeoutError: ExitCode.FAILURE,
    errors.HandlerError: ExitCode.FAILURE,
}


def test_exit_codes_are_zero_one_and_two():
    assert [int(code) for code in ExitCode] == [0, 1, 2]
    assert (ExitCode.SUCCESS, ExitCode.FAILURE, ExitCode.USAGE) == (0, 1, 2)


@pytest.mark.parametrize(("error", "exit_code"), EXIT_CODES.items())
def test_each_error_carries_its_exit_code(error, exit_code):
    assert issubclass(error, EtlCraftError)
    assert error.exit_code is exit_code
    assert error("message").exit_code is exit_code


def test_every_error_class_in_the_module_is_covered_above():
    defined = {
        obj
        for obj in vars(errors).values()
        if isinstance(obj, type)
        and issubclass(obj, Exception)
        and obj.__module__ == errors.__name__
    }
    assert defined == set(EXIT_CODES)


def test_engine_db_errors_share_a_base():
    assert issubclass(errors.MigrationError, errors.EngineDbError)
    assert issubclass(errors.LockTimeoutError, errors.EngineDbError)


def test_a_subclass_inherits_its_family_exit_code():
    class MissingSecret(errors.ConfigurationError):
        pass

    assert MissingSecret.exit_code is ExitCode.USAGE


def test_an_error_keeps_its_message():
    with pytest.raises(EtlCraftError, match="no active pipeline 'P1'"):
        raise errors.MetadataError("no active pipeline 'P1'")
