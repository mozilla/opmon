"""OpMon."""
import enum
from typing import TYPE_CHECKING, Any, List, Optional

if TYPE_CHECKING:
    from opmon.config import Summary

import attr


class MonitoringPeriod(enum.Enum):
    """
    Monitoring period.

    Used as x-axis.
    """

    BUILD_ID = "build_id"
    DAY = "submission_date"


class Channel(enum.Enum):
    """Release channel."""

    NIGHTLY = "nightly"
    BETA = "beta"
    RELEASE = "release"

    @classmethod
    def has_value(cls, value: str) -> bool:
        """Check if a specific value is represented by the enum."""
        return value in cls._value2member_map_  # type: ignore


@attr.s(auto_attribs=True)
class DataSource:
    """Represents a table or view, from which Metrics may be monitored."""

    name: str
    from_expression: str
    submission_date_column: str
    build_id_column: str
    client_id_column: str


@attr.s(auto_attribs=True)
class Metric:
    """Represents a metric to be monitored."""

    name: str
    data_source: DataSource
    select_expression: str
    friendly_name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    type: Optional[str] = None


@attr.s(auto_attribs=True)
class Dimension:
    """Represents a dimension for segmenting client populations."""

    name: str
    data_source: DataSource
    select_expression: str
    friendly_name: Optional[str] = None
    description: Optional[str] = None


class AlertType(enum.Enum):
    """Different types of alerts."""

    # alert when confidence intervals of different branches don't overlap
    CI_OVERLAP = "ci_overlap"

    # alert if defined thresholds are exceeded/too low
    THRESHOLD = "threshold"

    # alert if average of most recent measurement window is below/above average of previous window
    AVG_DIFF = "avg_diff"


@attr.s(auto_attribs=True)
class Alert:
    """Represents an alert."""

    name: str
    type: AlertType
    metrics: List["Summary"]
    friendly_name: Optional[str] = None
    description: Optional[str] = None
    parameters: Optional[List[Any]] = []
    min: Optional[List[int]] = None
    max: Optional[List[int]] = None
    window_size: Optional[int] = None
    max_relative_change: Optional[float] = None
    statistics: Optional[List[str]] = None
