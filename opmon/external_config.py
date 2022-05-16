"""
Retrieves external configuration files for opmon projects.

Opmon configuration files are stored in https://github.com/mozilla/opmon-config/
"""

from pathlib import Path
from typing import Dict, List, Optional

import attr
import toml
from git import Repo

from opmon import experimenter
from opmon.config import MonitoringSpec
from opmon.monitoring import Monitoring
from opmon.utils import TemporaryDirectory

DEFINITIONS_DIR = "definitions"
DEFAULTS_DIR = "defaults"


@attr.s(auto_attribs=True)
class ExternalConfig:
    """Represent an external config file."""

    slug: str
    spec: MonitoringSpec

    def validate(self, experiment: Optional[experimenter.Experiment] = None) -> None:
        """Validate the external config."""
        conf = self.spec.resolve(experiment)
        Monitoring(project="project", dataset="dataset", slug=self.slug, config=conf).validate()


def entity_from_path(path: Path) -> ExternalConfig:
    """Load an external config from the provided file path."""
    slug = path.stem
    config_dict = toml.loads(path.read_text())

    return ExternalConfig(
        slug=slug,
        spec=MonitoringSpec.from_dict(config_dict),
    )


@attr.s(auto_attribs=True)
class ExternalConfigCollection:
    """Collection of OpMon configurations pulled in from an external GitHub repository."""

    configs: List[ExternalConfig] = attr.Factory(list)
    definitions: Dict[str, ExternalConfig] = attr.Factory(dict)
    defaults: Dict[str, ExternalConfig] = attr.Factory(dict)

    CONFIG_URL = "https://github.com/mozilla/opmon-config"

    @classmethod
    def from_github_repo(cls) -> "ExternalConfigCollection":
        """Pull in external config files."""
        # download files to tmp directory
        with TemporaryDirectory() as tmp_dir:
            Repo.clone_from(cls.CONFIG_URL, tmp_dir)

            external_configs = []

            for config_file in tmp_dir.glob("*.toml"):
                external_configs.append(
                    ExternalConfig(
                        config_file.stem,
                        MonitoringSpec.from_dict(toml.load(config_file)),
                    )
                )

            definitions = {}

            for definition_file in tmp_dir.glob(f"**/{DEFINITIONS_DIR}/*.toml"):
                definitions[definition_file.stem] = ExternalConfig(
                    slug=definition_file.stem,
                    spec=MonitoringSpec.from_dict(toml.load(definition_file)),
                )

            defaults = {}

            for defaults_file in tmp_dir.glob(f"**/{DEFAULTS_DIR}/*.toml"):
                defaults[defaults_file.stem] = ExternalConfig(
                    slug=defaults_file.stem,
                    spec=MonitoringSpec.from_dict(toml.load(defaults_file)),
                )

        return cls(external_configs, definitions, defaults)

    def spec_for_experiment(self, slug: str) -> Optional[MonitoringSpec]:
        """Return the spec for a specific experiment."""
        for config in self.configs:
            if config.slug == slug:
                return config.spec

        return None

    def default_spec_for_platform(self, platform: str) -> Optional[MonitoringSpec]:
        """Return the default config for the provided platform."""
        default = self.defaults.get(platform, None)
        return default.spec if default else None

    def default_spec_for_type(self, type: str) -> Optional[MonitoringSpec]:
        """Return the default config for the provided type."""
        default = self.defaults.get(type, None)
        return default.spec if default else None
