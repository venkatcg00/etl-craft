"""Row and table rule contracts shared by configuration, execution and publication."""

from collections.abc import Sequence
from dataclasses import dataclass

from etl_craft.core.enums import BusinessRuleLevel, RunStatus, YesNo
from etl_craft.core.errors import HandlerError

RULE_TEXT_LIMIT = 4096
TABLE_COLUMNS = frozenset({"PASSED", "OBSERVED", "DETAIL"})


def rule_shape_problem(level: str, key: str | None, rule_type: str) -> str | None:
    """Check the rule's level before querying its warehouse."""
    if level not in {item.value for item in BusinessRuleLevel}:
        return f"BUSINESS_RULE_LEVEL={level!r}; expected ROW or TABLE; correct business_rules.csv"
    if level == BusinessRuleLevel.ROW and not key:
        return "ROW needs BUSINESS_RULE_KEY_COLUMN; name the target row key"
    if level == BusinessRuleLevel.TABLE and key:
        return f"TABLE has BUSINESS_RULE_KEY_COLUMN={key!r}; leave that column empty"
    if level == BusinessRuleLevel.TABLE and rule_type == "INCOMPLETE":
        return "TABLE cannot use INCOMPLETE; choose REJECT or REPORT"
    return None


def table_columns_problem(columns: Sequence[str]) -> str | None:
    """Require one PASSED and only its optional text fields."""
    names = [name.upper() for name in columns]
    if "PASSED" not in names or len(set(names)) != len(names) or set(names) - TABLE_COLUMNS:
        return (
            f"returned columns {list(columns)!r}; expected PASSED and optionally OBSERVED, DETAIL; "
            "correct the SELECT output aliases"
        )
    return None


@dataclass(frozen=True)
class TableRuleResult:
    """One measured table result, never a set of row flags."""

    passed: YesNo
    observed: str | None = None
    detail: str | None = None

    def run_status(self, rule_type: str) -> RunStatus:
        """Fail a rejecting check and retain a reporting result as completed evidence."""
        return (
            RunStatus.FAILED
            if self.passed == YesNo.NO and rule_type == "REJECT"
            else RunStatus.SUCCESS
        )


def table_result(passed: object, observed: object, detail: object, label: str) -> TableRuleResult:
    """Refuse coercions and overlong evidence rather than silently changing a result."""
    for name, value in (("PASSED", passed), ("OBSERVED", observed), ("DETAIL", detail)):
        limit = 1 if name == "PASSED" else RULE_TEXT_LIMIT
        if isinstance(value, str) and len(value) > limit:
            raise HandlerError(
                f"{label}: query returned {name} length={len(value)}; limit={limit} "
                "characters; shorten the value in BUSINESS_RULE_SQL"
            )
        if name != "PASSED" and value is not None and not isinstance(value, str):
            raise HandlerError(
                f"{label}: query returned {name}={value!r}; expected text or NULL; "
                "cast the value to text in BUSINESS_RULE_SQL"
            )
    if not isinstance(passed, str) or passed not in (YesNo.YES, YesNo.NO):
        raise HandlerError(
            f"{label}: query returned PASSED={passed!r}; expected text 'Y' or 'N'; "
            "return an uppercase text CASE expression in BUSINESS_RULE_SQL"
        )
    return TableRuleResult(
        YesNo(passed),
        observed if isinstance(observed, str) else None,
        detail if isinstance(detail, str) else None,
    )


@dataclass(frozen=True)
class RuleObservation:
    """Recorded table results shared by run reads, observation history and the catalog."""

    business_rule_id: int
    business_rule_run_id: int
    business_rule_name: str
    task_code: str
    pipeline_run_id: int
    task_run_id: int
    status: str
    passed: str | None
    observed: str | None
    detail: str | None
    target_table: str

    def describe(self) -> str:
        """Render the same evidence in status and explain."""
        return (
            f"{self.task_code}: rule {self.business_rule_name} PASSED={self.passed!r} "
            f"OBSERVED={self.observed!r} DETAIL={self.detail!r}"
        )
