"""Run two callers together at a named function boundary."""

import importlib
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

import pytest


def two_at_once(fn_a, fn_b, *, at, timeout=10):
    """Patch ``module.function`` so each caller waits once, then return both results in order."""
    module_name, name = at.rsplit(".", 1)
    module = importlib.import_module(module_name)
    original = getattr(module, name)
    barrier = threading.Barrier(2, timeout=timeout)
    local = threading.local()

    @wraps(original)
    def together(*args, **kwargs):
        if not getattr(local, "arrived", False):
            local.arrived = True
            barrier.wait()
        return original(*args, **kwargs)

    with pytest.MonkeyPatch.context() as patch, ThreadPoolExecutor(max_workers=2) as pool:
        patch.setattr(module, name, together)
        futures = [pool.submit(fn) for fn in (fn_a, fn_b)]
        try:
            return tuple(future.result(timeout=timeout * 2) for future in futures)
        finally:
            barrier.abort()
