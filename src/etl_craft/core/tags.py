"""Exact pipeline labels shared by metadata validation and read filters."""

import re

TAG_RULE = "1 to 64 lowercase ASCII letters, digits, '-', '_' or ':'"
_TAG = re.compile(r"[a-z0-9_:-]{1,64}\Z", re.ASCII)


def valid_tag(value: object) -> bool:
    """Check the tag as written, without normalizing case or whitespace."""
    return isinstance(value, str) and _TAG.fullmatch(value) is not None


def tag_problems(values: object) -> list[str]:
    """Name every invalid label, duplicate or collection limit with its remedy."""
    if not isinstance(values, list):
        return [
            f"TAGS={values!r}; expected a list of strings; write a JSON list of tags ({TAG_RULE})"
        ]
    problems = []
    if len(values) > 20:
        problems.append(f"TAGS has {len(values)} tags; expected at most 20; remove excess tags")
    seen = set()
    for value in values:
        if not valid_tag(value):
            problems.append(f"TAGS tag={value!r}; expected {TAG_RULE}; replace or remove this tag")
        elif value in seen:
            problems.append(
                f"TAGS tag={value!r} repeats; expected unique tags; remove the duplicate"
            )
        else:
            seen.add(value)
    return problems
