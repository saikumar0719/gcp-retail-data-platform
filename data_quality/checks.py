"""Reusable data-quality checks + reconciliation.

Each check returns (name, passed, detail). The DAG's quality gate fails
the run if any check fails, so bad data never reaches the marts.

Checks:
- row_count: source and target row counts match (reconciliation).
- duplicates: no duplicate event_id in the target.
- null_rate: key columns stay below a null threshold.
"""

from dataclasses import dataclass


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def row_count_check(source_count: int, target_count: int) -> CheckResult:
    passed = source_count == target_count
    return CheckResult(
        "row_count",
        passed,
        f"source={source_count} target={target_count}",
    )


def duplicates_check(rows: list, key: str) -> CheckResult:
    keys = [row[key] for row in rows]
    dupes = len(keys) - len(set(keys))
    return CheckResult("duplicates", dupes == 0, f"duplicate_keys={dupes}")


def null_rate_check(rows: list, column: str, max_rate: float = 0.01) -> CheckResult:
    if not rows:
        return CheckResult("null_rate", False, "no rows to check")
    nulls = sum(1 for row in rows if row.get(column) is None)
    rate = nulls / len(rows)
    return CheckResult(
        "null_rate", rate <= max_rate, f"{column}: {rate:.2%} nulls"
    )


def run_all_checks(date: str, **context) -> None:
    """Airflow callable. Wire real counts/rows from BigQuery here.

    Raises AssertionError on any failure so the DAG task fails loudly.
    """
    # Placeholder wiring: replace with BigQuery queries in deployment.
    # Kept explicit so a copy-paste deploy cannot silently pass.
    raise NotImplementedError(
        "Wire run_all_checks to BigQuery counts for "
        f"date={date} before scheduling this DAG."
    )
