from metric_config_parser.monitoring import MonitoringSpec

from opmon.config import ConfigLoader


class TestConfigLoader:
    """Test cases for _ConfigLoader"""

    def test_load_configs(self):
        configs_collection = ConfigLoader
        assert configs_collection.configs is not None
        assert len(configs_collection.configs.configs) > 0

    def test_configs_from(self):
        configs_collection = ConfigLoader.with_configs_from(
            ["https://github.com/mozilla/metric-hub/tree/main/opmon"]
        )
        assert configs_collection.configs is not None
        assert len(configs_collection.configs.configs) == len(
            ConfigLoader.configs.configs
        )

    def test_spec_for_experiment(self):
        experiment = next(
            c.slug
            for c in ConfigLoader.configs.configs
            if isinstance(c.spec, MonitoringSpec)
        )
        assert ConfigLoader.configs.spec_for_project(experiment) is not None

    def test_spec_for_nonexisting_experiment(self):
        assert ConfigLoader.configs.spec_for_project("non_existing") is None

    def test_get_nonexisting_outcome(self):
        assert ConfigLoader.configs.spec_for_outcome("non_existing", "foo") is None

    def test_get_data_source(self):
        definition = [
            d for d in ConfigLoader.configs.definitions if d.platform != "functions"
        ][0]
        metric = [
            m
            for m in definition.spec.metrics.definitions.values()
            if m.data_source is not None
        ][0]
        platform = definition.platform

        assert (
            ConfigLoader.configs.get_data_source_definition(
                metric.data_source.name, platform
            )
            is not None
        )

    def test_get_nonexisting_data_source(self):
        assert (
            ConfigLoader.configs.get_data_source_definition("non_existing", "foo")
            is None
        )
