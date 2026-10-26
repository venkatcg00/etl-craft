"""Strict subset of the Prometheus 0.0.4 text grammar used by the exporter."""

import re

NAME = r"[a-zA-Z_:][a-zA-Z0-9_:]*"
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\\n]|\\[\\"n])*)"(?:,|$)')
SAMPLE = re.compile(
    rf"({NAME})(?:\{{(.*)\}})? ([+-]?(?:[0-9]+(?:\.[0-9]*)?(?:e[+-]?[0-9]+)?|Inf))$"
)


def parse(body):
    help, types, samples = {}, {}, {}
    for line in body.splitlines():
        if line.startswith("# HELP "):
            _, _, name, description = line.split(" ", 3)
            assert re.fullmatch(NAME, name) and name not in help and description
            help[name] = description
            continue
        if line.startswith("# TYPE "):
            _, _, name, kind = line.split(" ", 3)
            assert name not in types and kind in {"gauge", "counter", "histogram"}
            types[name] = kind
            continue
        match = SAMPLE.fullmatch(line)
        assert match, line
        name, encoded, value = match.groups()
        labels = {}
        rest = encoded or ""
        while rest:
            label = LABEL.match(rest)
            assert label, rest
            key, raw = label.groups()
            assert key not in labels
            labels[key] = re.sub(r'\\([\\"n])', lambda m: "\n" if m[1] == "n" else m[1], raw)
            rest = rest[label.end() :]
        key = name, tuple(sorted(labels.items()))
        assert key not in samples
        family = re.sub(r"_(bucket|sum|count)$", "", name)
        assert name in types or types.get(family) == "histogram"
        samples[key] = float(value)
    assert help.keys() == types.keys()
    return types, samples


def value(body, name, **labels):
    return parse(body)[1].get(("etl_craft_" + name, tuple(sorted(labels.items()))), 0)
