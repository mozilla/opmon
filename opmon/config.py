"""
Parses configuration specifications into concrete objects.

Spec objects are direct representations of the configuration and contain unresolved references
to metrics and data sources.

Calling .resolve(config_spec, ConfigLoader.configs) on a Spec object produces a
concrete resolved Configuration class.
"""


import datetime as dt
from typing import List, Optional, Union

from metric_config_parser.config import (
    Config,
    ConfigCollection,
    DefaultConfig,
    DefinitionConfig,
    Outcome,
)
from metric_config_parser.experiment import Experiment
from metric_config_parser.monitoring import MonitoringSpec
from pytz import UTC

from opmon.bigquery_client import BeforeExecuteCallback

DEFAULT_CONFIG_REPO = "https://github.com/mozilla/opmon-config"
METRIC_HUB_REPO = "https://github.com/mozilla/metric-hub"


class _ConfigLoader:
    """
    Loads config files from an external repository.

    Config objects are converted into opmon native types.
    """

    config_collection: Optional[ConfigCollection] = None

    @property
    def configs(self) -> ConfigCollection:
        configs = getattr(self, "_configs", None)
        if configs:
            return configs

        if self.config_collection is None:
            self.config_collection = ConfigCollection.from_github_repos(
                [METRIC_HUB_REPO, DEFAULT_CONFIG_REPO]
            )
        self._configs = self.config_collection
        return self._configs

    def with_configs_from(
        self, repo_urls: Optional[List[str]], is_private: bool = False
    ) -> "_ConfigLoader":
        """Load configs from another repository and merge with default configs."""
        if repo_urls is None or len(repo_urls) < 1:
            return self

        config_collection = ConfigCollection.from_github_repos(
            repo_urls=repo_urls, is_private=is_private
        )
        self.config_collection = config_collection
        return self


ConfigLoader = _ConfigLoader()


def validate(
    config: Union[Outcome, Config, DefaultConfig, DefinitionConfig],
    experiment: Optional[Experiment] = None,
    config_getter: _ConfigLoader = ConfigLoader,
    before_execute_callback: Optional[BeforeExecuteCallback] = None,
):
    """Validate and dry run a config."""
    from opmon.monitoring import Monitoring
    from opmon.platform import PLATFORM_CONFIGS

    if isinstance(config, Config) and not (
        isinstance(config, DefaultConfig) or isinstance(config, DefinitionConfig)
    ):
        config.validate(config_getter.configs, experiment)
        resolved_config = config.spec.resolve(experiment, config_getter.configs)
    elif isinstance(config, Outcome):
        config.validate(config_getter.configs)
        print("Outcomes are currently not supported in OpMon")
    elif isinstance(config, DefaultConfig) or isinstance(config, DefinitionConfig):
        config.validate(config_getter.configs)

        if config.slug in PLATFORM_CONFIGS:
            app_name = config.slug
        else:
            app_name = "firefox_desktop"

        dummy_experiment = Experiment(
            experimenter_slug="dummy-experiment",
            normandy_slug="dummy_experiment",
            type="v6",
            status="Live",
            branches=[],
            end_date=None,
            reference_branch="control",
            is_high_population=False,
            start_date=dt.datetime.now(UTC),
            proposed_enrollment=14,
            app_name=app_name,
            outcomes=[],
        )

        spec = MonitoringSpec.default_for_platform_or_type(app_name, config_getter.configs)
        spec.merge(config.spec)
        resolved_config = spec.resolve(dummy_experiment, config_getter.configs)
    else:
        raise Exception(f"Unable to validate config: {config}")

    monitoring = Monitoring(
        "no project",
        "no dataset",
        "no derived dataset",
        config.slug,
        resolved_config,
        before_execute_callback=before_execute_callback,
    )
    monitoring.validate()
