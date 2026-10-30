"""Validate submitted access rows against current project, group and pipeline identities."""

import csv
import io
from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.core.enums import ApiRole, YesNo
from etl_craft.core.enums_access import AccessIntent, Permission
from etl_craft.core.errors import UsageError
from etl_craft.core.tags import valid_tag
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import read_tags
from etl_craft.services.operations.access_models import FIELDS, AccessWrite


@dataclass(frozen=True)
class InputRow:
    """One physical command row, including malformed column counts."""

    line: int
    values: dict[str, str | None]
    well_formed: bool = True


@dataclass(frozen=True)
class Validated:
    """Resolved identities which can be written only in their validating transaction."""

    line: int
    values: dict[str, str]
    project_id: int
    group_id: int | None
    account_id: int | None


def project(conn: Connection, code: str | None) -> int:
    """Resolve an active project without guessing a namespace."""
    if not code:
        raise UsageError("project is missing; expected an explicit scope; supply --project P")
    row = conn.execute(statement(conn, "project_by_code"), {"code": code}).one_or_none()
    if row is None or row.active_flag != YesNo.YES:
        raise UsageError(
            f"project {code!r}: expected an active project; select an existing project"
        )
    return int(row.project_id)


def inputs(request: AccessWrite, intent: AccessIntent) -> list[InputRow]:
    """Read submitted bytes, never a client's path or prevalidated plan."""
    if request.content is None:
        return [
            InputRow(
                1,
                {
                    "PROJECT": request.project,
                    "GROUP_NAME": request.group,
                    "ROLE": request.role,
                    "PIPELINE_CODE": request.pipeline,
                    "TAG": request.tag,
                    "PERMISSION": request.permission,
                    "ACTION": intent,
                    "ACCOUNT": request.account,
                },
            )
        ]
    if any(
        (
            request.group,
            request.pipeline,
            request.tag,
            request.role,
            request.account,
            request.permission,
        )
    ):
        raise UsageError("access grant: content conflicts with single-row fields; choose one form")
    reader = csv.reader(io.StringIO(request.content), strict=True)
    try:
        header = next(reader, [])
        if "ACCOUNT" in header:
            raise UsageError(
                "access input line 1: ACCOUNT column is forbidden in bulk input; "
                "use access grant --account for account permissions"
            )
        if set(header) != set(FIELDS) or len(header) != len(FIELDS):
            raise UsageError(
                f"access input line 1: found {header!r}; expected {','.join(FIELDS)}; "
                f"fix the header"
            )
        result = []
        for row in reader:
            if row:
                result.append(
                    InputRow(
                        reader.line_num,
                        {
                            name: row[index] if index < len(row) else None
                            for index, name in enumerate(header)
                        },
                        len(row) == len(header),
                    )
                )
        return result
    except csv.Error as error:
        raise UsageError(
            f"access input line {reader.line_num}: invalid CSV {error}; fix the row"
        ) from error


def target_problems(conn: Connection, values: dict[str, str], project_id: int) -> list[str]:
    """Require current pipeline identities and existing, syntactically valid tags."""
    active = [
        row
        for row in conn.execute(statement(conn, "permission_pipelines"), {"project_id": project_id})
        if row.active_flag == YesNo.YES
    ]
    tags = {tag for row in active for tag in read_tags(row.pipeline_parameters, row.pipeline_code)}
    pipeline, tag = values["PIPELINE_CODE"], values["TAG"]
    problems = []
    if pipeline and pipeline not in {row.pipeline_code for row in active}:
        problems.append(
            f"pipeline {pipeline!r}; expected an active pipeline in "
            f"{values['PROJECT']!r}; check the project"
        )
    if tag and (not valid_tag(tag) or tag not in tags):
        problems.append(
            f"tag {tag!r}; expected a tag on an active pipeline in "
            f"{values['PROJECT']!r}; fix the tag"
        )
    return problems


def row_problems(
    conn: Connection, request: AccessWrite, row: InputRow, groups: dict[str, int]
) -> tuple[Validated | None, list[str]]:
    """Name all invalid fields of a row without writing any grant."""
    values = {name: (row.values.get(name) or "").strip() for name in (*FIELDS, "ACCOUNT")}
    bad = []
    project_id = None
    try:
        project_id = project(conn, values["PROJECT"])
    except UsageError as error:
        bad.append(str(error))
    if request.project is not None and values["PROJECT"] != request.project:
        bad.append(
            f"PROJECT={values['PROJECT']!r}; expected scope {request.project!r}; "
            f"select that project"
        )
    group_id = groups.get(values["GROUP_NAME"])
    accounts = {row.name: row for row in conn.execute(statement(conn, "account_list"))}
    account = accounts.get(values["ACCOUNT"])
    account_id = None if account is None else int(account.account_id)
    if sum(bool(values[key]) for key in ("GROUP_NAME", "ROLE", "ACCOUNT")) != 1:
        bad.append("expected exactly one group, deployment role or account; select one principal")
    if values["GROUP_NAME"] and group_id is None:
        remedy = (
            "use access grant --account for account permissions"
            if values["GROUP_NAME"] in accounts
            else "use group add or group list"
        )
        bad.append(f"group {values['GROUP_NAME']!r}; expected an active group; {remedy}")
    if values["ACCOUNT"] and (account is None or account.active_flag != YesNo.YES):
        bad.append(f"account {values['ACCOUNT']!r}; expected an active account; use account list")
    if values["ROLE"] and values["ROLE"] not in (ApiRole.VIEWER, ApiRole.OPERATOR):
        bad.append(f"ROLE={values['ROLE']!r}; expected viewer or operator; choose a supported role")
    optional_permission = values["ACTION"] == AccessIntent.REVOKE and not values["PERMISSION"]
    if not optional_permission and values["PERMISSION"] not in tuple(Permission):
        bad.append(
            f"PERMISSION={values['PERMISSION']!r}; expected CAN_VIEW, CAN_RUN "
            f"or CAN_MANAGE; fix the permission"
        )
    if values["ACTION"] not in tuple(AccessIntent):
        bad.append(f"ACTION={values['ACTION']!r}; expected GRANT or REVOKE; fix the intent")
    if values["PIPELINE_CODE"] and values["TAG"]:
        bad.append("expected a project, pipeline or tag; PIPELINE_CODE and TAG cannot both be set")
    if project_id is not None:
        bad.extend(target_problems(conn, values, project_id))
    if not row.well_formed or (
        any(row.values.get(name) is None for name in FIELDS) and request.content is not None
    ):
        bad.append("expected seven CSV fields; fix missing or extra cells")
    if bad:
        return None, bad
    assert project_id is not None
    return Validated(row.line, values, project_id, group_id, account_id), []


def validate(conn: Connection, request: AccessWrite, rows: list[InputRow]) -> list[Validated]:
    """Accumulate every bad line, including both sides of duplicate/conflicting identities."""
    errors: list[str] = []
    seen: dict[tuple[str, ...], int] = {}
    checked = []
    groups = {
        row.name: int(row.group_id)
        for row in conn.execute(statement(conn, "group_list"))
        if row.active_flag == YesNo.YES
    }
    for row in rows:
        valid, problems = row_problems(conn, request, row, groups)
        identity = tuple(
            (row.values.get(name) or "").strip()
            for name in ("PROJECT", "GROUP_NAME", "ROLE", "ACCOUNT", "PIPELINE_CODE", "TAG")
        )
        if identity in seen:
            previous = seen[identity]
            problems.append(
                f"identity repeats line {previous}; remove duplicate/conflicting identities"
            )
            errors.append(
                f"access input line {previous}: identity repeats line {row.line}; "
                f"remove duplicate/conflicting identities"
            )
        seen[identity] = row.line
        errors.extend(f"access input line {row.line}: {problem}" for problem in problems)
        if valid is not None:
            checked.append(valid)
    if errors:
        raise UsageError("\n".join(errors))
    return checked
