"""OpMon config."""

import copy
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

import attr
import cattr
import pytz

from opmon import (
    Alert,
    AlertType,
    Channel,
    DataSource,
    Dimension,
    Metric,
    MonitoringPeriod,
)
from opmon.experimenter import Experiment
from opmon.statistic import Statistic

_converter = cattr.Converter()


def _validate_yyyy_mm_dd(instance: Any, attribute: Any, value: Any) -> None:
    """Check if the provided string is a valid date string."""
    _parse_date(value)


def _parse_date(yyyy_mm_dd: Optional[str]) -> Optional[datetime]:
    """Convert a date string to a date type."""
    if not yyyy_mm_dd:
        return None
    return datetime.strptime(yyyy_mm_dd, "%Y-%m-%d").replace(tzinfo=pytz.utc)


@attr.s(auto_attribs=True)
class DataSourceDefinition:
    """Describes the interface for defining a data source in configuration."""

    name: str  # implicit in configuration
    from_expression: str
    submission_date_column: str = "submission_date"
    build_id_column: str = "SAFE.SUBSTR(application.build_id, 0, 8)"
    client_id_column: str = "client_id"

    def resolve(self, spec: "MonitoringSpec") -> DataSource:
        """Create the `DataSource` representation."""
        params: Dict[str, Any] = {
            "name": self.name,
            "from_expression": self.from_expression,
            "submission_date_column": self.submission_date_column,
            "build_id_column": self.build_id_column,
            "client_id_column": self.client_id_column,
        }
        return DataSource(**params)


@attr.s(auto_attribs=True)
class DataSourcesSpec:
    """
    Holds data source definitions.

    This doesn't have a resolve() method to produce a concrete DataSourcesConfiguration
    because it's just a container for the definitions, and we don't need it after the spec phase.
    """

    definitions: Dict[str, DataSourceDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "DataSourcesSpec":
        """Create a `DataSourcesSpec` for a dictionary."""
        definitions = {
            k: _converter.structure({"name": k, **v}, DataSourceDefinition) for k, v in d.items()
        }
        return cls(definitions)

    def merge(self, other: "DataSourcesSpec"):
        """
        Merge another datasource spec into the current one.

        The `other` DataSourcesSpec overwrites existing keys.
        """
        self.definitions.update(other.definitions)


_converter.register_structure_hook(
    DataSourcesSpec, lambda obj, _type: DataSourcesSpec.from_dict(obj)
)


@attr.s(auto_attribs=True)
class DataSourceReference:
    """Represents a reference to a data source."""

    name: str

    def resolve(self, spec: "MonitoringSpec") -> DataSource:
        """Return the `DataSource` that this is referencing."""
        if self.name not in spec.data_sources.definitions:
            raise ValueError(f"DataSource {self.name} has not been defined.")

        return spec.data_sources.definitions[self.name].resolve(spec)


_converter.register_structure_hook(
    DataSourceReference, lambda obj, _type: DataSourceReference(name=obj)
)


@attr.s(auto_attribs=True)
class Summary:
    """Represents a metric with a statistical treatment."""

    metric: "Metric"
    statistic: "Statistic"


@attr.s(auto_attribs=True)
class MetricDefinition:
    """Describes the interface for defining a metric in configuration."""

    name: str  # implicit in configuration
    select_expression: str
    data_source: DataSourceReference
    friendly_name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    type: Optional[str] = "scalar"
    statistics: Optional[Dict[str, Dict[str, Any]]] = {"percentile": {}}  # todo: remove default?

    def resolve(self, spec: "MonitoringSpec") -> List[Summary]:
        """Create and return a `Metric` instance from this definition."""
        summaries = []
        if self.statistics:
            for statistic_name, params in self.statistics.items():
                stats_params = copy.deepcopy(params)
                metric = Metric(
                    name=self.name,
                    data_source=self.data_source.resolve(spec),
                    select_expression=self.select_expression,
                    friendly_name=self.friendly_name,
                    description=self.description,
                    category=self.category,
                    type=self.type or "scalar",
                )

                found = False
                for statistic in Statistic.__subclasses__():
                    if statistic.name() == statistic_name:
                        found = True
                        break

                if not found:
                    raise ValueError(f"Statistic '{statistic_name}' does not exist.")

                stats_params = copy.deepcopy(params)
                summaries.append(
                    Summary(
                        metric=metric,
                        statistic=statistic.from_dict(stats_params),
                    )
                )

        return summaries


@attr.s(auto_attribs=True)
class MetricsSpec:
    """Describes the interface for defining custom metric definitions."""

    definitions: Dict[str, MetricDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "MetricsSpec":
        """Create a `MetricsSpec` from a dictionary."""
        d = dict((k.lower(), v) for k, v in d.items())

        definitions = {
            k: _converter.structure({"name": k, **v}, MetricDefinition) for k, v in d.items()
        }
        return cls(definitions=definitions)

    def merge(self, other: "MetricsSpec"):
        """
        Merge another metric spec into the current one.

        The `other` MetricsSpec overwrites existing keys.
        """
        self.definitions.update(other.definitions)


_converter.register_structure_hook(MetricsSpec, lambda obj, _type: MetricsSpec.from_dict(obj))


@attr.s(auto_attribs=True)
class MetricReference:
    """Represents a reference to a metric."""

    name: str

    def resolve(self, spec: "MonitoringSpec") -> List[Summary]:
        """Return the `DataSource` that this is referencing."""
        if self.name in spec.metrics.definitions:
            return spec.metrics.definitions[self.name].resolve(spec)
        raise ValueError(f"Could not locate metric {self.name}")


_converter.register_structure_hook(MetricReference, lambda obj, _type: MetricReference(name=obj))


@attr.s(auto_attribs=True)
class DimensionDefinition:
    """Describes the interface for defining a dimension in configuration."""

    name: str  # implicit in configuration
    select_expression: str
    data_source: DataSourceReference
    friendly_name: Optional[str] = None
    description: Optional[str] = None

    def resolve(self, spec: "MonitoringSpec") -> Dimension:
        """Create and return a `Dimension` from the definition."""
        return Dimension(
            name=self.name,
            data_source=self.data_source.resolve(spec),
            select_expression=self.select_expression,
            friendly_name=self.friendly_name,
            description=self.description,
        )


@attr.s(auto_attribs=True)
class DimensionsSpec:
    """Describes the interface for defining custom dimensions."""

    definitions: Dict[str, DimensionDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "DimensionsSpec":
        """Create a `DimensionsSpec` from a dictionary."""
        d = dict((k.lower(), v) for k, v in d.items())

        definitions = {
            k: _converter.structure({"name": k, **v}, DimensionDefinition) for k, v in d.items()
        }
        return cls(definitions=definitions)

    def merge(self, other: "DimensionsSpec"):
        """
        Merge another dimension spec into the current one.

        The `other` DimensionsSpec overwrites existing keys.
        """
        self.definitions.update(other.definitions)


_converter.register_structure_hook(DimensionsSpec, lambda obj, _type: DimensionsSpec.from_dict(obj))


@attr.s(auto_attribs=True)
class DimensionReference:
    """Represents a reference to a dimension."""

    name: str

    def resolve(self, spec: "MonitoringSpec") -> Dimension:
        """Return the referenced `Dimension`."""
        if self.name in spec.dimensions.definitions:
            return spec.dimensions.definitions[self.name].resolve(spec)
        raise ValueError(f"Could not locate dimension {self.name}")


_converter.register_structure_hook(
    DimensionReference, lambda obj, _type: DimensionReference(name=obj)
)


@attr.s(auto_attribs=True)
class AlertDefinition:
    """Describes the interface for defining an alert in configuration."""

    name: str  # implicit in configuration
    type: AlertType
    metrics: List[MetricReference]
    friendly_name: Optional[str] = None
    description: Optional[str] = None
    parameters: Optional[List[Any]] = None
    min: Optional[List[int]] = None
    max: Optional[List[int]] = None
    window_size: Optional[int] = None
    max_relative_change: Optional[float] = None
    statistics: Optional[List[str]] = None

    def __attrs_post_init__(self):
        """Validate that the right parameters have been set depending on the alert type."""
        if self.type == AlertType.CI_OVERLAP:
            none_fields = ["min", "max", "window_size", "max_relative_change"]
        elif self.type == AlertType.THRESHOLD:
            none_fields = ["window_size", "max_relative_change"]
            if self.min is None and self.max is None:
                raise ValueError(
                    "Either 'max' or 'min' needs to be set when defining a threshold alert"
                )
            if self.min and self.parameters and len(self.min) != len(self.parameters):
                raise ValueError(
                    "Number of 'min' thresholds not matching number of parameters to monitor. "
                    + "A 'min' threshold needs to be specified for each percentile."
                )
            if self.max and self.parameters and len(self.max) != len(self.parameters):
                raise ValueError(
                    "Number of 'max' thresholds not matching number of parameters to monitor. "
                    + "A 'max' threshold needs to be specified for each percentile."
                )
        elif self.type == AlertType.AVG_DIFF:
            none_fields = ["min", "max"]
            if self.window_size is None:
                raise ValueError("'window_size' needs to be specified when using avg_diff alert")
            if self.max_relative_change is None:
                raise ValueError("'max_relative_change' to be specified when using avg_diff alert")

        for field in none_fields:
            if getattr(self, field) is not None:
                raise ValueError(
                    f"For alert of type {str(self.type)}, the parameter {field} must not be set"
                )

    def resolve(self, spec: "MonitoringSpec") -> Alert:
        """Create and return a `Alert` from the definition."""
        # filter to only have metrics that actually need to be monitored
        metrics = []
        for metric_ref in {p.name for p in self.metrics}:
            if metric_ref in spec.metrics.definitions:
                metrics += spec.metrics.definitions[metric_ref].resolve(spec)
            else:
                raise ValueError(f"No definition for metric {metric_ref}.")

        statistics = None
        if self.statistics:
            statistics = []
            for stats_ref in {stat for stat in self.statistics}:
                found = False
                for statistic in Statistic.__subclasses__():
                    if statistic.name() == stats_ref:
                        found = True
                        statistics.append(stats_ref)
                        break

                if not found:
                    raise ValueError(
                        f"Statistic '{stats_ref}' does not exist in alert '{self.name}'"
                    )

        return Alert(
            name=self.name,
            type=self.type,
            metrics=metrics,
            friendly_name=self.friendly_name,
            description=self.description,
            parameters=self.parameters,
            min=self.min,
            max=self.max,
            window_size=self.window_size,
            max_relative_change=self.max_relative_change,
            statistics=statistics,
        )


@attr.s(auto_attribs=True)
class AlertsSpec:
    """Describes the interface for defining custom alerts."""

    definitions: Dict[str, AlertDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "AlertsSpec":
        """Create a `AlertsSpec` from a dictionary."""
        d = dict((k.lower(), v) for k, v in d.items())

        definitions = {
            k: _converter.structure({"name": k, **v}, AlertDefinition) for k, v in d.items()
        }
        return cls(definitions=definitions)

    def merge(self, other: "AlertsSpec"):
        """
        Merge another alert spec into the current one.

        The `other` AlertsSpec overwrites existing keys.
        """
        for alert_name, alert_definition in other.definitions.items():
            if alert_name in self.definitions:
                for key in attr.fields_dict(type(self.definitions[alert_name])):
                    if key == "metrics":
                        self.definitions[alert_name].metrics += alert_definition.metrics
                    else:
                        setattr(
                            self.definitions[alert_name],
                            key,
                            getattr(alert_definition, key)
                            or getattr(self.definitions[alert_name], key),
                        )
            else:
                self.definitions[alert_name] = alert_definition

        self.definitions.update(other.definitions)


_converter.register_structure_hook(AlertsSpec, lambda obj, _type: AlertsSpec.from_dict(obj))


@attr.s(auto_attribs=True)
class AlertReference:
    """Represents a reference to an alert."""

    name: str

    def resolve(self, spec: "MonitoringSpec") -> Alert:
        """Return the `Alert` that this is referencing."""
        if self.name not in spec.alerts.definitions:
            raise ValueError(f"Alert {self.name} has not been defined.")

        return spec.alerts.definitions[self.name].resolve(spec)


_converter.register_structure_hook(AlertReference, lambda obj, _type: AlertReference(name=obj))


@attr.s(auto_attribs=True, kw_only=True)
class PopulationConfiguration:
    """Describes the interface for defining the client population in configuration."""

    data_source: Optional[DataSource] = None
    boolean_pref: Optional[str] = None
    channel: Optional[Channel] = None
    branches: List[str] = attr.Factory(list)
    monitor_entire_population: bool = False
    group_by_dimension: Optional[Dimension] = None


@attr.s(auto_attribs=True, kw_only=True)
class PopulationSpec:
    """Describes the interface for defining the client population."""

    data_source: Optional[DataSourceReference] = None
    boolean_pref: Optional[str] = None
    channel: Optional[Channel] = None
    branches: Optional[List[str]] = None
    dimensions: List[DimensionReference] = attr.Factory(list)
    monitor_entire_population: bool = False
    group_by_dimension: Optional[DimensionReference] = None

    def resolve(
        self, spec: "MonitoringSpec", experiment: Optional[Experiment]
    ) -> PopulationConfiguration:
        """Create a `PopulationConfiguration` from the spec."""
        if self.group_by_dimension:
            if self.group_by_dimension not in self.dimensions:
                raise ValueError(
                    f"{self.group_by_dimension} not listed as dimension, but used for grouping"
                )

        return PopulationConfiguration(
            data_source=self.data_source.resolve(spec) if self.data_source else None,
            boolean_pref=self.boolean_pref
            or (experiment.boolean_pref if experiment and not experiment.is_rollout else None),
            channel=self.channel or (experiment.channel if experiment else None),
            branches=self.branches
            if self.branches is not None
            else (
                [branch.slug for branch in experiment.branches]
                if experiment and self.boolean_pref is None and not experiment.is_rollout
                else []
            ),
            monitor_entire_population=self.monitor_entire_population,
            group_by_dimension=self.group_by_dimension.resolve(spec)
            if self.group_by_dimension
            else None,
        )

    def merge(self, other: "PopulationSpec") -> None:
        """
        Merge another population spec into the current one.

        The `other` PopulationSpec overwrites existing keys.
        """
        for key in attr.fields_dict(type(self)):
            if key == "branches":
                self.branches = self.branches if self.branches is not None else other.branches
            elif key == "dimensions":
                self.dimensions += other.dimensions
            else:
                setattr(self, key, getattr(other, key) or getattr(self, key))


@attr.s(auto_attribs=True, kw_only=True)
class ProjectConfiguration:
    """Describes the interface for defining the project in configuration."""

    reference_branch: str = "control"
    name: Optional[str] = None
    xaxis: MonitoringPeriod = attr.ib(default=MonitoringPeriod.DAY)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    population: PopulationConfiguration = attr.Factory(PopulationConfiguration)
    compact_visualization: bool = False
    skip_default_metrics: bool = False
    skip: bool = False
    platform: Optional[str] = None


@attr.s(auto_attribs=True, kw_only=True)
class ProjectSpec:
    """Describes the interface for defining the project."""

    name: Optional[str] = None
    platform: Optional[str] = None
    xaxis: Optional[MonitoringPeriod] = None
    start_date: Optional[str] = attr.ib(default=None, validator=_validate_yyyy_mm_dd)
    end_date: Optional[str] = attr.ib(default=None, validator=_validate_yyyy_mm_dd)
    metrics: List[MetricReference] = attr.Factory(list)
    alerts: List[AlertReference] = attr.Factory(list)
    reference_branch: Optional[str] = None
    population: PopulationSpec = attr.Factory(PopulationSpec)
    compact_visualization: bool = False
    skip_default_metrics: bool = False
    skip: bool = False

    @classmethod
    def from_dict(cls, d: dict) -> "ProjectSpec":
        """Create a new `ProjectSpec` from a dictionary."""
        d = dict((k.lower(), v) for k, v in d.items())
        return _converter.structure(d, cls)

    def resolve(
        self, spec: "MonitoringSpec", experiment: Optional[Experiment]
    ) -> ProjectConfiguration:
        """Create a `ProjectConfiguration` from the spec."""
        return ProjectConfiguration(
            name=self.name or (experiment.name if experiment else None),
            xaxis=self.xaxis or MonitoringPeriod.DAY,
            start_date=_parse_date(
                self.start_date
                or (
                    experiment.start_date.strftime("%Y-%m-%d")
                    if experiment and experiment.start_date
                    else None
                )
            ),
            end_date=_parse_date(
                self.end_date
                or (
                    experiment.end_date.strftime("%Y-%m-%d")
                    if experiment and experiment.end_date
                    else None
                )
            ),
            population=self.population.resolve(spec, experiment),
            reference_branch=self.reference_branch
            or (
                experiment.reference_branch
                if experiment and experiment.reference_branch
                else "control"
            ),
            compact_visualization=self.compact_visualization,
            skip_default_metrics=self.skip_default_metrics,
            skip=self.skip,
            platform=self.platform,
        )

    def merge(self, other: "ProjectSpec") -> None:
        """
        Merge another project spec into the current one.

        The `other` ProjectSpec overwrites existing keys.
        """
        for key in attr.fields_dict(type(self)):
            if key == "population":
                self.population.merge(other.population)
            elif key == "metrics":
                self.metrics += other.metrics
            elif key == "alerts":
                self.alerts += other.alerts
            else:
                setattr(self, key, getattr(other, key) or getattr(self, key))


@attr.s(auto_attribs=True)
class MonitoringConfiguration:
    """
    Represents configuration options.

    All references, for example to data sources, have been resolved in this representation.
    Instead of instantiating this directly, consider using MonitoringSpec.resolve().
    """

    project: Optional[ProjectConfiguration] = None
    metrics: List[Summary] = attr.Factory(list)
    dimensions: List[Dimension] = attr.Factory(list)
    alerts: List[Alert] = attr.Factory(list)


@attr.s(auto_attribs=True)
class MonitoringSpec:
    """
    Represents a configuration file.

    The expected use is like:
        MonitoringSpec.from_dict(toml.load(my_configuration_file)).resolve()
    which will produce a fully populated, concrete `MonitoringConfiguration`.
    """

    project: ProjectSpec = attr.Factory(ProjectSpec)
    data_sources: DataSourcesSpec = attr.Factory(DataSourcesSpec)
    metrics: MetricsSpec = attr.Factory(MetricsSpec)
    dimensions: DimensionsSpec = attr.Factory(DimensionsSpec)
    alerts: AlertsSpec = attr.Factory(AlertsSpec)
    _resolved: bool = False

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "MonitoringSpec":
        """Create a `MonitoringSpec` from a dict."""
        d = dict((k.lower(), v) for k, v in d.items())
        return _converter.structure(d, cls)

    def resolve(self, experiment: Optional[Experiment] = None) -> MonitoringConfiguration:
        """Create a `MonitoringConfiguration` from the spec."""
        if self._resolved:
            raise Exception("Can't resolve an MonitoringSpec twice")

        self._resolved = True

        # filter to only have metrics that actually need to be monitored
        metrics = []
        for metric_ref in {p.name for p in self.project.metrics}:
            if metric_ref in self.metrics.definitions:
                metrics += self.metrics.definitions[metric_ref].resolve(self)
            else:
                raise ValueError(f"No definition for metric {metric_ref}.")

        # filter to only have dimensions that actually are in use
        dimensions = []
        for dimension_ref in {d.name for d in self.project.population.dimensions}:
            if dimension_ref in self.dimensions.definitions:
                dimensions.append(self.dimensions.definitions[dimension_ref].resolve(self))
            else:
                raise ValueError(f"No definition for dimension {dimension_ref}.")

        # filter to only have alerts that actually are in use
        alerts = []
        for alert_ref in {d.name for d in self.project.alerts}:
            if alert_ref in self.alerts.definitions:
                alerts.append(self.alerts.definitions[alert_ref].resolve(self))
            else:
                raise ValueError(f"No definition for alert {alert_ref}.")

        return MonitoringConfiguration(
            project=self.project.resolve(self, experiment) if self.project else None,
            metrics=metrics,
            dimensions=dimensions,
            alerts=alerts,
        )

    def merge(self, other: Optional["MonitoringSpec"]):
        """Merge another monitoring spec into the current one."""
        if other:
            self.project.merge(other.project)
            self.data_sources.merge(other.data_sources)
            self.metrics.merge(other.metrics)
            self.dimensions.merge(other.dimensions)
            self.alerts.merge(other.alerts)
