import enum
from typing import Optional

import attr


class MonitoringPeriod(enum.Enum):
    BUILD_ID = "build_id"
    DAY = "day"


@attr.s(auto_attribs=True)
class DataSource:
    """Represents a table or view, from which Probes may be monitored.
    Args:
        name (str): Name for the Data Source. Used in sanity metric
            column names.
        from_expression (str): FROM expression - often just a fully-qualified
            table name. Sometimes a subquery. May contain the string
            ``{dataset}`` which will be replaced with an app-specific
            dataset for Glean apps. If the expression is templated
            on dataset, default_dataset is mandatory.
    """

    name: str
    from_expression: str


@attr.s(auto_attribs=True)
class Probe:
    """Represents a probe to be monitored."""

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
