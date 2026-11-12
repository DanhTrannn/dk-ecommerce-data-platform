"""Engine-independent publication-gate result contract."""

from dataclasses import dataclass


class GoldQualityGateError(RuntimeError):
    """Raised when blocking post-Gold validation fails."""


@dataclass(frozen=True)
class QualityCheckResult:
    check_name: str
    dataset: str
    observed_value: str
    expected_value: str
    passed: bool
    details: str
    severity: str = "ERROR"
    variance_percent: float = 0.0
    failed_rows: int = 0
