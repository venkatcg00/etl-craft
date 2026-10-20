"""Explicit manual/release catalog load acceptance, excluded from ordinary discovery."""

import os
from time import perf_counter

import pytest

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.services.catalog.collect import build_catalog
from etl_craft.services.catalog.site import write_site
from fixtures.catalog_scale import seed_catalog

pytestmark = [pytest.mark.harness, pytest.mark.timeout(240)]


def test_large_catalog_cold_and_incremental_builds_finish_within_budget(
    engine_db, tmp_path, record_testsuite_property
):
    engine, config = engine_db.engine, engine_db.config
    with acting_as(SYSTEM_ACTOR):
        with engine.begin() as conn:
            seed_catalog(conn, prefix="LOAD", pipelines=300, tasks=10, runs=50)
        for phase in ("cold", "incremental"):
            started = perf_counter()
            catalog = build_catalog(engine, config)
            site = write_site(catalog, config, tmp_path / "catalog")
            seconds = perf_counter() - started
            record_testsuite_property(f"{engine.dialect.name}.{phase}_seconds", f"{seconds:.3f}")
            assert len(catalog.pipelines) == 300
            assert len(catalog.tasks) == 3000
            assert all(len(p.runs) == 30 for p in catalog.pipelines.values())
            assert all(len(t.runs) == 30 for t in catalog.tasks.values())
            assert site.pages >= 15_300
            assert seconds < 60, f"{engine.dialect.name} {phase}: {seconds:.3f}s exceeds 60s budget"
            sample = site.folder / "pipelines/LOAD_0.html"
            if phase == "cold":
                os.utime(sample, ns=(1_000_000_000_000_000_000, 1_000_000_000_000_000_000))
            else:
                assert sample.stat().st_mtime_ns == 1_000_000_000_000_000_000
