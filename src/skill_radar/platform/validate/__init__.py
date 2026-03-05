"""Validation framework for the Skill Radar platform.

Public API
----------
.. autofunction:: run_checks
.. automodule:: skill_radar.platform.validate.models
.. automodule:: skill_radar.platform.validate.sinks
"""

from skill_radar.platform.validate.models import (
    CheckResult,
    CheckStatus,
    ExitCode,
    ValidationReport,
)
from skill_radar.platform.validate.runner import run_checks

__all__ = [
    "CheckResult",
    "CheckStatus",
    "ExitCode",
    "ValidationReport",
    "run_checks",
]
