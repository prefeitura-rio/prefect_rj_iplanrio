"""Pipeline labels (code_owner, severity) propagated to observability.

Labels are defined at deployment time in prefect.yaml and injected
into all logs via structured logging context.
"""

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Literal, Optional

_labels_context: ContextVar["PipelineLabels"] = ContextVar(
    "pipeline_labels", default=None
)

SeverityLevel = Literal["low", "medium", "high", "critical"]


@dataclass(frozen=True)
class PipelineLabels:
    """Labels attached to every log in the pipeline execution.

    These are defined in prefect.yaml and propagated through the
    execution context to ensure observability systems can correlate
    logs with ownership and criticality.

    :param code_owner: GitHub username of the developer responsible.
    :param severity: Criticality level: "low", "medium", "high", "critical".
    """

    code_owner: str
    severity: SeverityLevel

    def __post_init__(self):
        valid = {"low", "medium", "high", "critical"}
        if self.severity not in valid:
            raise ValueError(
                f"severity must be one of {valid}, got '{self.severity}'"
            )


def set_labels(code_owner: str, severity: SeverityLevel) -> None:
    """Set labels for this execution context.

    Call this at the start of your @flow function.

    :param code_owner: GitHub username.
    :param severity: Criticality: "low", "medium", "high", "critical".
    """
    _labels_context.set(PipelineLabels(code_owner=code_owner, severity=severity))


def get_labels() -> Optional[PipelineLabels]:
    """Get labels for the current execution context."""
    return _labels_context.get()


def get_labels_dict() -> dict[str, object]:
    """Get labels as dict for logging.

    Returns a dict suitable for the logging filter.
    """
    labels = get_labels()
    if labels is None:
        return {}
    return {
        "code_owner": labels.code_owner,
        "severity": labels.severity,
    }
