"""Bounded per-item Python execution with durable results and retry selection."""

from __future__ import annotations

import importlib
import multiprocessing
import time
from collections.abc import Callable, Iterable
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from contextvars import copy_context
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from etl_craft.core.enums import ItemStatus
from etl_craft.core.errors import HandlerError, PermanentError
from etl_craft.core.faults import fault_point
from etl_craft.core.items import ItemRecord, ItemState, ItemStore, bounded_keys, progress
from etl_craft.core.log import log_context
from etl_craft.core.text import mask_arguments

if TYPE_CHECKING:
    from etl_craft.scripting import ScriptResult, ScriptTask

THREAD_CEILING = 256
DEFAULT_PARALLEL = 16
THREAD_MEMORY_MB = 2
PROCESS_MEMORY_MB = 64
KEPT = frozenset({ItemStatus.SUCCESS, ItemStatus.SKIPPED})


def options(
    max_items: int, parallel: int | None, retries: int, memory: int, processes: bool, cpu: float = 1
) -> int:
    """Validate the complete request before any item runs, using its admitted memory."""
    for name, value, minimum in (("max_items", max_items, 0), ("retries", retries, 0)):
        if type(value) is not int or value < minimum:
            raise HandlerError(
                f"fan_out: {name}={value!r}; expected a whole number >= {minimum}; correct {name}"
            )
    if type(processes) is not bool:
        raise HandlerError(
            f"fan_out processes={processes!r}; expected a boolean; use True or False"
        )
    bound = min(THREAD_CEILING, memory // (PROCESS_MEMORY_MB if processes else THREAD_MEMORY_MB))
    bound = min(bound, int(cpu)) if processes else bound
    width = min(DEFAULT_PARALLEL, bound) if parallel is None else parallel
    if type(width) is not int or not 1 <= width <= bound:
        raise HandlerError(
            f"fan_out: max_parallel={width!r}; expected 1-{bound} within "
            f"{memory} MB and ceiling {THREAD_CEILING}; lower max_parallel or "
            f"raise MEMORY_REQUEST_MB"
        )
    return width


def importable(fn: Callable[[str], ScriptResult]) -> None:
    """Spawned workers require an importable top-level callable, never an inherited engine."""
    try:
        module = importlib.import_module(fn.__module__)
        found = getattr(module, fn.__qualname__, None)
    except (ImportError, AttributeError) as error:
        raise HandlerError(
            f"fan_out: function {fn!r} is not importable; expected a top-level "
            f"function; move it to an importable module"
        ) from error
    if found is not fn:
        raise HandlerError(
            f"fan_out: function {fn!r} is not importable; expected a top-level "
            f"function; move it to an importable module"
        )


@dataclass(frozen=True)
class FanOutOptions:
    """One bounded invocation, shared by validation and execution."""

    max_items: int
    max_parallel: int | None = None
    retries: int = 0
    processes: bool = False


def execute(
    task: ScriptTask,
    items: Iterable[str],
    fn: Callable[[str], ScriptResult],
    requested: FanOutOptions,
) -> ScriptResult:
    """Keep only bounded futures; committed successes survive failure, timeout and LOST."""
    from etl_craft.scripting import ScriptResult

    max_items = requested.max_items
    if type(max_items) is not int or max_items < 0:
        raise HandlerError(
            f"fan_out: max_items={max_items!r}; expected a nonnegative whole "
            f"number; correct max_items"
        )
    if not callable(fn):
        raise HandlerError(f"fan_out fn={fn!r}; expected a callable; pass the item function")
    keys = bounded_keys(items, max_items)
    if not keys and task._items is None:
        return ScriptResult(0)
    if task._items is None:
        raise HandlerError(
            f"{task.task_code}: fan_out has no admitted item store; expected an "
            f"owned task attempt; run the script through etl-craft run"
        )
    state = task._items.items()
    if not keys and not state.records:
        return ScriptResult(0)
    width = options(
        max_items,
        requested.max_parallel,
        requested.retries,
        state.memory_mb,
        requested.processes,
        state.cpu,
    )
    if requested.processes:
        importable(fn)
    keys, known, selected = prepare_work(task, keys, state, max_items)
    executor = (
        ProcessPoolExecutor(width, mp_context=multiprocessing.get_context("spawn"))
        if requested.processes
        else ThreadPoolExecutor(width, thread_name_prefix="etl-item")
    )
    with executor:
        permanent = Work(task, fn, executor, requested.retries, width).run(
            [key for key in keys if not selected or key in selected], known
        )
    final = task._items.items().records
    failed = [r for r in final if r.status not in KEPT]
    if failed:
        detail = "; ".join(f"{r.key} ({r.status}): {r.error or r.status}" for r in failed)
        error_class = PermanentError if permanent else HandlerError
        raise error_class(
            f"{task.task_code}: fan_out items did not succeed: {detail}; fix the "
            f"failed items and retry this task run"
        )
    return ScriptResult(sum(r.row_count for r in final))


def prepare_work(
    task: ScriptTask,
    keys: list[str],
    state: ItemState,
    max_items: int,
) -> tuple[list[str], dict[str, ItemRecord], set[str]]:
    """Bind discovery to retained work before making any batch registration writes."""
    assert task._items is not None
    known = {r.key: r for r in state.records}
    keys = list(dict.fromkeys((*known, *keys)))
    if len(keys) > max_items:
        raise HandlerError(
            f"{task.task_code}: {len(keys)} current and retained items exceed "
            f"max_items={max_items}; increase the bound or start a new run"
        )
    selected = {
        r.key for r in known.values() if r.status == ItemStatus.QUEUED and r.error is not None
    }
    new_keys = set(keys) - {r.key for r in state.records}
    if selected and new_keys:
        raise HandlerError(
            f"{task.task_code}: item rerun discovery added keys={sorted(new_keys)[:16]}; "
            "expected the recorded work list; run the full task or a new run to admit new items"
        )
    for first in range(0, len(keys), 1000):
        task._items.prepare_items(tuple(keys[first : first + 1000]))
    for key in keys:
        old = known.get(key)
        known[key] = (
            replace(old, status=ItemStatus.SKIPPED)
            if old is not None and old.status in KEPT
            else old
            if old is not None
            else ItemRecord(key, 0, ItemStatus.QUEUED)
        )
    return keys, known, selected


@dataclass
class Work:
    """Bound the work queue and keep orchestration in the task's main thread."""

    task: ScriptTask
    fn: Callable[[str], ScriptResult]
    executor: Executor
    retries: int
    width: int

    def run(self, keys: list[str], known: dict[str, ItemRecord]) -> bool:
        """Fill free positions only, recording each ending before another starts."""
        assert self.task._items is not None
        waiting = iter(keys)
        active: dict[Future[ScriptResult], tuple[ItemRecord, int]] = {}
        started = time.monotonic()
        completed = sum(r.status in KEPT for r in known.values())
        skipped = completed
        goal = completed + sum(known[key].status not in KEPT for key in keys)
        permanent = False
        exhausted = False
        while active or not exhausted:
            while len(active) < self.width and not exhausted:
                key = next(waiting, None)
                if key is None:
                    exhausted = True
                    break
                old = known[key]
                if old.status in KEPT:
                    continue
                event = self.submit(key, old.number + 1)
                if event.status != ItemStatus.RUNNING:
                    completed += 1
                    known[key] = event
                    continue
                active[self.future(key)] = (event, self.retries)
            if not active:
                continue
            done, _ = wait(active, return_when=FIRST_COMPLETED)
            for future in done:
                event, left = active.pop(future)
                ended = finish(self.task._items, event, future)
                self.task.logger.info(
                    "item %s attempt %s %s; rows=%s; error=%s",
                    ended.key,
                    ended.number,
                    ended.status,
                    ended.row_count,
                    ended.error,
                )
                known[event.key] = replace(ended, error=None)
                failed_permanently = ended.status == ItemStatus.FAILED and isinstance(
                    future.exception(),
                    PermanentError,
                )
                permanent = permanent or failed_permanently
                if ended.status == ItemStatus.FAILED and left and not failed_permanently:
                    event = self.submit(event.key, ended.number + 1)
                    if event.status == ItemStatus.RUNNING:
                        active[self.future(event.key)] = (event, left - 1)
                        continue
                    known[event.key] = event
                completed += 1
                self.progress(known, completed, skipped, started, goal)
        return permanent

    def future(self, key: str) -> Future[ScriptResult]:
        """Preserve actors and task sessions in threads; spawn processes without inherited DBs."""
        if isinstance(self.executor, ThreadPoolExecutor):
            return self.executor.submit(copy_context().run, evaluate, self.fn, key)
        return self.executor.submit(evaluate, self.fn, key)

    def submit(self, key: str, number: int) -> ItemRecord:
        """Respect a mark or cancel committed before this start."""
        assert self.task._items is not None
        event = self.task._items.item_event(
            ItemRecord(
                key,
                number,
                ItemStatus.RUNNING,
                started=datetime.now(UTC).isoformat(),
                report_id=str(uuid4()),
            )
        )
        if event.status == ItemStatus.RUNNING:
            self.task.logger.info("item %s attempt %s RUNNING", key, number)
        return event

    def progress(
        self, known: dict[str, ItemRecord], done: int, skipped: int, start: float, goal: int
    ) -> None:
        """Report persisted outcomes with a projection based on completed work."""
        eta = (time.monotonic() - start) * max(0, goal - done) / max(1, done - skipped)
        self.task.logger.info("%s; projected finish in %.1fs", progress(tuple(known.values())), eta)


def evaluate(fn: Callable[[str], ScriptResult], key: str) -> ScriptResult:
    """Validate a worker's result inside its future so failures remain retryable evidence."""
    from etl_craft.scripting import ScriptResult

    with log_context(item_key=key):
        result = fn(key)
    if (
        not isinstance(result, ScriptResult)
        or type(result.row_count) is not int
        or result.row_count < 0
        or result.offset is not None
    ):
        raise HandlerError(
            f"item {key}: result type={type(result).__name__}; expected "
            "ScriptResult with nonnegative row_count "
            "and no item offset; return the offset from the task"
        )
    return result


def finish(store: ItemStore, event: ItemRecord, future: Future[ScriptResult]) -> ItemRecord:
    """Record failures at the worker boundary; a requested cancellation wins at ending."""
    error = future.exception()
    if error is None:
        ending = replace(event, status=ItemStatus.SUCCESS, row_count=future.result().row_count)
    else:
        message = str(mask_arguments({"message": f"{type(error).__name__}: {error}"})["message"])[
            :4096
        ]
        ending = replace(event, status=ItemStatus.FAILED, error=message)
    fault_point("item.before_result")
    result = store.item_event(
        replace(
            ending,
            ended=datetime.now(UTC).isoformat(),
            report_id=str(uuid4()),
        )
    )
    fault_point("item.after_result")
    return result
