"""OpMon config."""

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

import attr
import cattr
import pytz

from opmon import Channel, DataSource, Dimension, MonitoringPeriod, Probe
from opmon.experimenter import Experiment

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
class ProbeDefinition:
    """Describes the interface for defining a probe in configuration."""

    name: str  # implicit in configuration
    select_expression: str
    data_source: DataSourceReference
    friendly_name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    type: Optional[str] = None

    def resolve(self, spec: "MonitoringSpec") -> Probe:
        """Create and return a `Probe` instance from this definition."""
        return Probe(
            name=self.name,
            data_source=self.data_source.resolve(spec),
            select_expression=self.select_expression,
            friendly_name=self.friendly_name,
            description=self.description,
            category=self.category,
            type=self.type,
        )


@attr.s(auto_attribs=True)
class ProbesSpec:
    """Describes the interface for defining custom probe definitions."""

    definitions: Dict[str, ProbeDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "ProbesSpec":
        """Create a `ProbesSpec` from a dictionary."""
        d = dict((k.lower(), v) for k, v in d.items())

        definitions = {
            k: _converter.structure({"name": k, **v}, ProbeDefinition) for k, v in d.items()
        }
        return cls(definitions=definitions)

    def merge(self, other: "ProbesSpec"):
        """
        Merge another probe spec into the current one.

        The `other` ProbesSpec overwrites existing keys.
        """
        self.definitions.update(other.definitions)


_converter.register_structure_hook(ProbesSpec, lambda obj, _type: ProbesSpec.from_dict(obj))


@attr.s(auto_attribs=True)
class ProbeReference:
    """Represents a reference to a probe."""

    name: str

    def resolve(self, spec: "MonitoringSpec") -> Probe:
        """Return the `DataSource` that this is referencing."""
        if self.name in spec.probes.definitions:
            return spec.probes.definitions[self.name].resolve(spec)
        raise ValueError(f"Could not locate probe {self.name}")


_converter.register_structure_hook(ProbeReference, lambda obj, _type: ProbeReference(name=obj))


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


@attr.s(auto_attribs=True, kw_only=True)
class PopulationConfiguration:
    """Describes the interface for defining the client population in configuration."""

    data_source: Optional[DataSource] = None
    boolean_pref: Optional[str] = None
    channel: Optional[Channel] = attr.ib(default=Channel.NIGHTLY)
    branches: List[str] = attr.Factory(list)


@attr.s(auto_attribs=True, kw_only=True)
class PopulationSpec:
    """Describes the interface for defining the client population."""

    data_source: Optional[DataSourceReference] = None
    boolean_pref: Optional[str] = None
    channel: Optional[Channel] = None
    branches: Optional[List[str]] = None
    dimensions: List[DimensionReference] = attr.Factory(list)

    def resolve(
        self, spec: "MonitoringSpec", experiment: Optional[Experiment]
    ) -> PopulationConfiguration:
        """Create a `PopulationConfiguration` from the spec."""
        return PopulationConfiguration(
            data_source=self.data_source.resolve(spec) if self.data_source else None,
            boolean_pref=self.boolean_pref or (experiment.boolean_pref if experiment else None),
            channel=self.channel or (experiment.channel if experiment else None),
            branches=self.branches
            if self.branches is not None
            else (
                [branch.slug for branch in experiment.branches]
                if experiment and self.boolean_pref is None
                else []
            ),
        )

    def merge(self, other: "PopulationSpec") -> None:
        """
        Merge another population spec into the current one.

        The `other` PopulationSpec overwrites existing keys.
        """
        for key in attr.fields_dict(type(self)):
            if key == "branches":
                self.branches = other.branches if other.branches is not None else other.branches
            else:
                setattr(self, key, getattr(other, key) or getattr(self, key))


@attr.s(auto_attribs=True, kw_only=True)
class ProjectConfiguration:
    """Describes the interface for defining the project in configuration."""

    name: Optional[str] = None
    xaxis: MonitoringPeriod = attr.ib(default=MonitoringPeriod.DAY)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    population: PopulationConfiguration = attr.Factory(PopulationConfiguration)


@attr.s(auto_attribs=True, kw_only=True)
class ProjectSpec:
    """Describes the interface for defining the project."""

    name: Optional[str] = None
    platform: Optional[str] = None
    xaxis: Optional[MonitoringPeriod] = None
    start_date: Optional[str] = attr.ib(default=None, validator=_validate_yyyy_mm_dd)
    end_date: Optional[str] = attr.ib(default=None, validator=_validate_yyyy_mm_dd)
    probes: List[ProbeReference] = attr.Factory(list)
    population: PopulationSpec = attr.Factory(PopulationSpec)

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
        )

    def merge(self, other: "ProjectSpec") -> None:
        """
        Merge another project spec into the current one.

        The `other` ProjectSpec overwrites existing keys.
        """
        for key in attr.fields_dict(type(self)):
            if key == "population":
                self.population.merge(other.population)
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
    probes: List[Probe] = attr.Factory(list)
    dimensions: List[Dimension] = attr.Factory(list)


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
    probes: ProbesSpec = attr.Factory(ProbesSpec)
    dimensions: DimensionsSpec = attr.Factory(DimensionsSpec)
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

        # filter to only have probes that actually need to be monitored
        probes = []
        for probe_ref in {p.name for p in self.project.probes}:
            if probe_ref in self.probes.definitions:
                probes.append(self.probes.definitions[probe_ref].resolve(self))
            else:
                raise ValueError(f"No definition for probe {probe_ref}.")

        # filter to only have dimensions that actually are in use
        dimensions = []
        for dimension_ref in {d.name for d in self.project.population.dimensions}:
            if dimension_ref in self.dimensions.definitions:
                dimensions.append(self.dimensions.definitions[dimension_ref].resolve(self))
            else:
                raise ValueError(f"No definition for dimension {dimension_ref}.")

        return MonitoringConfiguration(
            project=self.project.resolve(self, experiment) if self.project else None,
            probes=probes,
            dimensions=dimensions,
        )

    def merge(self, other: "MonitoringSpec"):
        """Merge another monitoring spec into the current one."""
        self.project.merge(other.project)
        self.data_sources.merge(other.data_sources)
        self.probes.merge(other.probes)
        self.dimensions.merge(other.dimensions)
