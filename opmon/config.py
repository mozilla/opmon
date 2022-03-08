from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

import attr
import cattr
import pytz

from opmon import DataSource, Dimension, MonitoringPeriod, Probe
from opmon.experimenter import Experiment

_converter = cattr.Converter()


@attr.s(auto_attribs=True)
class DataSourceDefinition:
    """Describes the interface for defining a data source in configuration."""

    name: str  # implicit in configuration
    from_expression: str

    def resolve(self, spec: "MonitoringSpec") -> DataSource:
        """Create the `DataSource` representation."""
        params: Dict[str, Any] = {"name": self.name, "from_expression": self.from_expression}
        return DataSource(**params)


class DataSourcesSpec:
    """Holds data source definitions.

    This doesn't have a resolve() method to produce a concrete DataSourcesConfiguration
    because it's just a container for the definitions, and we don't need it after the spec phase."""

    definitions: Dict[str, DataSourceDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "DataSourcesSpec":
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


@attr.s(auto_attribs=True)
class DataSourceReference:
    name: str

    def resolve(self, spec: "MonitoringSpec") -> DataSource:
        if self.name not in spec.data_sources.definitions:
            raise ValueError(f"DataSource {self.name} has not been defined.")

        return spec.data_sources.definitions[self.name].resolve(spec)


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
class DimensionDefinition:
    """Describes the interface for defining a dimension in configuration."""

    name: str  # implicit in configuration
    select_expression: str
    data_source: DataSourceReference
    friendly_name: Optional[str] = None
    description: Optional[str] = None

    def resolve(self, spec: "MonitoringSpec") -> Dimension:
        return Dimension(
            name=self.name,
            data_source=self.data_source.resolve(spec),
            select_expression=self.select_expression,
            friendly_name=self.friendly_name,
            description=self.description,
        )


@attr.s(auto_attribs=True)
class ProbesSpec:
    """Describes the interface for defining custom probe definitions."""

    definitions: Dict[str, ProbeDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "ProbesSpec":
        d = dict((k.lower(), v) for k, v in d.items())

        definitions = {
            k: _converter.structure({"name": k, **v}, ProbeDefinition) for k, v in d.items()
        }
        return cls(definitions=definitions)


@attr.s(auto_attribs=True)
class DimensionsSpec:
    """Describes the interface for defining custom dimensions."""

    definitions: Dict[str, DimensionDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "DimensionsSpec":
        d = dict((k.lower(), v) for k, v in d.items())

        definitions = {
            k: _converter.structure({"name": k, **v}, DimensionDefinition) for k, v in d.items()
        }
        return cls(definitions=definitions)


@attr.s(auto_attribs=True)
class ProbeReference:
    name: str

    def resolve(self, spec: "MonitoringSpec") -> List[Probe]:
        if self.name in spec.probes.definitions:
            return spec.probes.definitions[self.name].resolve(spec)
        raise ValueError(f"Could not locate probe {self.name}")


@attr.s(auto_attribs=True)
class DimensionReference:
    name: str

    def resolve(self, spec: "MonitoringSpec") -> List[Dimension]:
        if self.name in spec.dimensions.definitions:
            return spec.dimensions.definitions[self.name].resolve(spec)
        raise ValueError(f"Could not locate dimension {self.name}")


@attr.s(auto_attribs=True, kw_only=True)
class PopulationConfiguration:
    data_source: Optional[DataSource] = None
    boolean_pref: Optional[str] = None
    branches: List[str] = attr.Factory(list)


@attr.s(auto_attribs=True, kw_only=True)
class PopulationSpec:
    data_source: Optional[DataSourceReference] = None
    boolean_pref: Optional[str] = None
    branches: List[str] = attr.Factory(list)
    dimensions: List[DimensionReference] = attr.Factory(list)

    def resolve(
        self, spec: "MonitoringSpec", experiment: Optional[Experiment]
    ) -> PopulationConfiguration:
        """Create a `PopulationConfiguration` from the spec."""
        return PopulationConfiguration(
            data_source=self.data_source.resolve(spec) if self.data_source else None,
            boolean_pref=self.boolean_pref or (experiment.boolean_pref if experiment else None),
            branches=self.branches
            or ([branch.slug for branch in experiment.branches] if experiment else []),
        )


@attr.s(auto_attribs=True, kw_only=True)
class ProjectConfiguration:
    name: Optional[str] = None
    xaxis: MonitoringPeriod = attr.ib(default=MonitoringPeriod.DAY)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    population: PopulationConfiguration = attr.Factory(PopulationConfiguration)


def _validate_yyyy_mm_dd(instance: Any, attribute: Any, value: Any) -> None:
    _parse_date(value)


def _parse_date(yyyy_mm_dd: Optional[str]) -> Optional[datetime]:
    """Convert a date string to a date type."""
    if not yyyy_mm_dd:
        return None
    return datetime.strptime(yyyy_mm_dd, "%Y-%m-%d").replace(tzinfo=pytz.utc)


@attr.s(auto_attribs=True, kw_only=True)
class ProjectSpec:
    name: Optional[str] = None
    xaxis: MonitoringPeriod = attr.ib(default=MonitoringPeriod.DAY)
    start_date: Optional[str] = attr.ib(default=None, validator=_validate_yyyy_mm_dd)
    end_date: Optional[str] = attr.ib(default=None, validator=_validate_yyyy_mm_dd)
    probes: List[ProbeReference] = attr.Factory(list)
    population: PopulationSpec = attr.Factory(PopulationSpec)

    def resolve(
        self, spec: "MonitoringSpec", experiment: Optional[Experiment]
    ) -> ProjectConfiguration:
        """Create a `ProjectConfiguration` from the spec."""

        return ProjectConfiguration(
            name=self.name or (experiment.name if experiment else None),
            xaxis=self.xaxis,
            start_date=self.start_date or (experiment.start_date if experiment else None),
            end_date=self.end_date or (experiment.end_date if experiment else None),
            population=self.population.resolve(spec, experiment),
        )


@attr.s(auto_attribs=True)
class MonitoringConfiguration:
    """
    Represents configuration options.

    All references, for example to data sources, have been resolved in this representation.
    Instead of instantiating this directly, consider using MonitoringSpec.resolve().
    """

    project: Optional[ProjectConfiguration] = None
    probes: Dict[str, Probe] = attr.Factory(dict)
    dimensions: Dict[str, Dimension] = attr.Factory(dict)


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
        for probe_ref in self.project.probes:
            if probe_ref in self.probes.definitions:
                probes.append(self.probes.definitions[probe_ref].resolve(self))
            else:
                raise ValueError(f"No definition for probe {probe_ref}.")

        # filter to only have dimensions that actually are in use
        dimensions = []
        for dimension_ref in self.project.population.dimensions:
            if dimension_ref in self.dimensions.definitions:
                dimensions.append(self.dimensions.definitions[dimension_ref].resolve(self))
            else:
                raise ValueError(f"No definition for dimension {dimension_ref}.")

        return MonitoringConfiguration(
            project=self.project.resolve(self, experiment) if self.project else None,
            probes=probes,
            dimensions=dimensions,
        )
