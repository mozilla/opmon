import pytest

from opmon.config import ConfigLoader


class TestConfigLoader:
    """Test cases for _ConfigLoader"""

    def test_load_configs(self):
        configs_collection = ConfigLoader
        assert configs_collection.configs is not None
        assert len(configs_collection.configs.configs) > 0

    def test_configs_from(self):
        configs_collection = ConfigLoader.with_configs_from(
            ["https://github.com/mozilla/jetstream-config"]
        )
        assert configs_collection.configs is not None
        assert len(configs_collection.configs.configs) == len(ConfigLoader.configs.configs)

    def test_spec_for_experiment(self):
        experiment = ConfigLoader.configs.configs[0].slug
        assert ConfigLoader.spec_for_experiment(experiment) is not None

    def test_spec_for_nonexisting_experiment(self):
        assert ConfigLoader.spec_for_experiment("non_exisiting") is None

    def test_get_outcome(self):
        outcome = ConfigLoader.configs.outcomes[0]
        assert ConfigLoader.get_outcome(outcome.slug, outcome.platform) is not None

    def test_get_nonexisting_outcome(self):
        assert ConfigLoader.get_outcome("non_existing", "foo") is None

    def test_get_data_source(self):
        metric = list(ConfigLoader.configs.definitions[0].spec.metrics.definitions.values())[0]
        platform = ConfigLoader.configs.definitions[0].platform
        assert ConfigLoader.get_data_source(metric.data_source.name, platform) is not None

    def test_get_nonexisting_data_source(self):
        with pytest.raises(Exception):
            ConfigLoader.get_data_source("non_existing", "foo") is None
