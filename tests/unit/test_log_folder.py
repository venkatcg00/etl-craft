"""A task's log folder must be writable, with free space, before its process starts."""

from collections import namedtuple

import pytest

from etl_craft.core.errors import ConfigurationError
from etl_craft.execution import supervisor
from etl_craft.execution.supervisor import require_log_folder

pytestmark = pytest.mark.unit


def test_a_writable_folder_with_space_is_created(tmp_path):
    require_log_folder(tmp_path / "logs" / "P" / "run-1")
    assert (tmp_path / "logs" / "P" / "run-1").is_dir()


def test_a_folder_that_cannot_be_created_is_named(tmp_path):
    (tmp_path / "logs").write_text("a file, not a folder")
    with pytest.raises(ConfigurationError, match=r"cannot create the log folder .*logs/P"):
        require_log_folder(tmp_path / "logs" / "P")


def test_a_folder_this_user_cannot_write_is_refused_with_the_remedy(tmp_path, monkeypatch):
    monkeypatch.setattr(supervisor.os, "access", lambda path, mode: False)
    with pytest.raises(ConfigurationError, match=r"is not writable .*Orchestration\.Log_dir"):
        require_log_folder(tmp_path)


def test_a_disk_without_room_is_refused_naming_what_is_free_and_needed(tmp_path, monkeypatch):
    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr(supervisor.shutil, "disk_usage", lambda path: usage(1, 1, 10 * 2**20))
    with pytest.raises(ConfigurationError, match=r"has 10 MiB free; a task needs at least 64 MiB"):
        require_log_folder(tmp_path)
