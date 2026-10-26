"""Bound primary-key predicates shared by source reads and mirror writes."""


def key_predicate(keys: list[str], rows: list[list[object]]) -> tuple[str, dict[str, object]]:
    """Bind every key value; identifiers come from inspected Engine DB metadata."""
    clauses = []
    params: dict[str, object] = {}
    for number, row in enumerate(rows):
        parts = []
        for index, (key, value) in enumerate(zip(keys, row, strict=True)):
            name = f"key_{number}_{index}"
            parts.append(f"{key}=:{name}")
            params[name] = value
        clauses.append("(" + " AND ".join(parts) + ")")
    return " OR ".join(clauses), params
