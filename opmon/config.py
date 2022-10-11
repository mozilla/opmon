"""
Parses configuration specifications into concrete objects.

Spec objects are direct representations of the configuration and contain unresolved references
to metrics and data sources.

Calling .resolve(config_spec, ConfigLoader.configs) on a Spec object produces a
concrete resolved Configuration class.
"""


from typing import List, Optional

from jetstream_config_parser.config import ConfigCollection

DEFAULT_CONFIG_REPO = "https://github.com/mozilla/opmon-config"
# todo update once metric hub has been set up
# METRIC_HUB_REPO = "https://github.com/mozilla/jetstream-config"
METRIC_HUB_REPO = "https://github.com/mozilla/opmon-config"


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
        if repo_urls is None:
            return self

        config_collection = ConfigCollection.from_github_repos(
            repo_urls=repo_urls, is_private=is_private
        )
        self.configs.merge(config_collection)
        return self


ConfigLoader = _ConfigLoader()
