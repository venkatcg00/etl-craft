"""Prometheus family metadata, escaping and cumulative histograms."""

import re
from pathlib import Path

import pytest
import yaml

from etl_craft.core.metrics import BUCKETS, Metrics
from etl_craft.engine.metrics import FAMILIES
from fixtures.metrics import parse

pytestmark = pytest.mark.unit


def test_text_format_has_unique_metadata_escaped_labels_and_cumulative_buckets():
    registry = Metrics()
    registry.define("example_total", "A counter.", "counter")
    registry.define("duration_seconds", "A histogram.", "histogram")
    labels = (("project", 'quotes " slash \\ newline\n'),)
    registry.add("example_total", labels, 2)
    for duration in (0, 0.005, 2, 40):
        registry.add("duration_seconds", labels, duration)
    types, samples = parse(registry.render({}))
    assert types["etl_craft_duration_seconds"] == "histogram"
    assert samples[("etl_craft_example_total", labels)] == 2
    buckets = [
        samples[("etl_craft_duration_seconds_bucket", tuple(sorted((*labels, ("le", str(bound))))))]
        for bound in (*BUCKETS, "+Inf")
    ]
    assert buckets == sorted(buckets)
    assert buckets[-1] == samples[("etl_craft_duration_seconds_count", labels)] == 4
    assert samples[("etl_craft_duration_seconds_sum", labels)] == pytest.approx(42.005)


def test_every_alert_rule_metric_has_exported_family_metadata():
    registry = Metrics()
    for name, (help, kind) in FAMILIES.items():
        registry.define(name, help, kind)
    types, _ = parse(registry.render({}))
    path = Path(__file__).resolve().parents[2] / "docs/examples/prometheus-rules.yaml"
    groups = yaml.safe_load(path.read_text())["groups"]
    for group in groups:
        for rule in group["rules"]:
            names = re.findall(r"\betl_craft_[a-z_]+\b", rule["expr"])
            assert names and set(names) <= types.keys()
