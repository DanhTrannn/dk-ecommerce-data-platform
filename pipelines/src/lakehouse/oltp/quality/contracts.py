"""Engine-independent publication-gate result contract."""

from dataclasses import dataclass
from typing import Any


class GoldQualityGateError(RuntimeError):
    """Raised when blocking post-Gold validation fails."""


def make_rule_id(check_name: str, dataset: str) -> str:
    """Canonical stable identity for quality check rules."""
    return f"{dataset}:{check_name}"


@dataclass(frozen=True)
class QualityCheckResult:
    check_name: str
    dataset: str
    observed_value: str
    expected_value: str
    passed: bool
    details: str
    rule_id: str = ""
    severity: str = "ERROR"
    variance_percent: float = 0.0
    failed_rows: int = 0
    status: str = "PASSED"

    def __post_init__(self):
        if not self.rule_id:
            object.__setattr__(self, "rule_id", make_rule_id(self.check_name, self.dataset))
        if not self.passed and self.status == "PASSED":
            object.__setattr__(self, "status", "FAILED")


def validate_required_checks(summary: dict[str, Any], required_ids: set[str] | tuple[str, ...]) -> None:
    """Validate that quality summary covers all required check IDs with no blocking errors."""
    if not isinstance(summary, dict):
        raise GoldQualityGateError("Quality summary must be a dictionary")

    executed_ids = list(summary.get("executed_check_ids", []))
    if len(executed_ids) != len(set(executed_ids)):
        raise GoldQualityGateError("Duplicate check IDs executed in quality summary")

    executed_set = set(executed_ids)
    missing = set(required_ids) - executed_set
    if missing:
        raise GoldQualityGateError(f"Missing required checks: {sorted(missing)}")

    if summary.get("status") != "PASSED" or summary.get("failed_checks", 0) > 0:
        raise GoldQualityGateError("Quality gate summary status is FAILED or has failed checks")

    for result in summary.get("results", []):
        rule_id = result.get("rule_id", make_rule_id(result.get("check_name", "unknown"), result.get("dataset", "unknown")))
        status = str(result.get("status", "PASSED")).upper()
        if status in ("SKIPPED", "ERROR"):
            raise GoldQualityGateError(f"Check {rule_id} was skipped or execution error: status={status}")

        severity = str(result.get("severity", "ERROR")).upper()
        passed = bool(result.get("passed", False))
        failed_rows = int(result.get("failed_rows", 0))

        if severity == "ERROR":
            if not passed:
                raise GoldQualityGateError(f"Blocking check failed: {rule_id}")
            if failed_rows > 0:
                raise GoldQualityGateError(f"Blocking check has {failed_rows} failed rows: {rule_id}")
