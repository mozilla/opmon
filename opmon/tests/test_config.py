from datetime import datetime
from textwrap import dedent

import pytest
import pytz
import toml

from opmon import MonitoringPeriod
from opmon.config import MetricReference, MonitoringConfiguration, MonitoringSpec


class TestConfig:
    def test_trivial_configuration(self, projects):
        spec = MonitoringSpec.from_dict({})
        assert isinstance(spec, MonitoringSpec)
        cfg = spec.resolve()
        assert isinstance(cfg, MonitoringConfiguration)
        assert cfg.metrics == []

    def test_metric_definition(self):
        config_str = dedent(
            """
            [project]
            metrics = ["test"]

            [metrics]
            [metrics.test]
            select_expression = "SELECT 1"
            data_source = "foo"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        assert spec.metrics.definitions["test"].select_expression == "SELECT 1"
        assert spec.data_sources.definitions["foo"].from_expression == "test"
        conf = spec.resolve()
        assert conf.metrics[0].metric.name == "test"
        assert conf.metrics[0].metric.data_source.name == "foo"

    def test_duplicate_metrics_are_okay(self, experiments):
        config_str = dedent(
            """
            [project]
            metrics = ["test", "test"]

            [metrics]
            [metrics.test]
            select_expression = "SELECT 1"
            data_source = "foo"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve()
        assert len(cfg.metrics) == 1

    def test_data_source_definition(self, experiments):
        config_str = dedent(
            """
            [project]
            metrics = ["test", "test2"]

            [metrics]
            [metrics.test]
            select_expression = "SELECT 1"
            data_source = "eggs"

            [metrics.test2]
            select_expression = "SELECT 1"
            data_source = "silly_knight"

            [data_sources.eggs]
            from_expression = "england.camelot"

            [data_sources.silly_knight]
            from_expression = "france"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve()
        test = [p for p in cfg.metrics if p.metric.name == "test"][0]
        test2 = [p for p in cfg.metrics if p.metric.name == "test2"][0]
        assert test.metric.data_source.name == "eggs"
        assert "camelot" in test.metric.data_source.from_expression
        assert test2.metric.data_source.name == "silly_knight"
        assert "france" in test2.metric.data_source.from_expression

    def test_merge(self, experiments):
        """Test merging configs"""
        config_str = dedent(
            """
            [metrics]
            [metrics.test]
            select_expression = "SELECT 1"
            data_source = "foo"

            [metrics.test2]
            select_expression = "SELECT 2"
            data_source = "foo"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"

            [dimensions]
            [dimensions.foo]
            select_expression = "bar"
            data_source = "foo"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))

        config_str = dedent(
            """
            [project]
            name = "foo"
            metrics = ["test", "test2"]

            [metrics]
            [metrics.test]
            select_expression = "SELECT 'd'"
            data_source = "foo"

            [data_sources]
            [data_sources.foo]
            from_expression = "bar"
            """
        )
        spec2 = MonitoringSpec.from_dict(toml.loads(config_str))
        spec.merge(spec2)
        cfg = spec.resolve()

        assert cfg.project.name == "foo"
        test = [p for p in cfg.metrics if p.metric.name == "test"][0]
        test2 = [p for p in cfg.metrics if p.metric.name == "test2"][0]
        assert test.metric.select_expression == "SELECT 'd'"
        assert test.metric.data_source.name == "foo"
        assert test.metric.data_source.from_expression == "bar"
        assert test2.metric.select_expression == "SELECT 2"

    def test_unknown_metric_failure(self, experiments):
        config_str = dedent(
            """
            [project]
            name = "foo"
            metrics = ["test", "test2"]

            [metrics]
            [metrics.test]
            select_expression = "SELECT 'd'"
            data_source = "foo"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"
            """
        )

        with pytest.raises(ValueError) as e:
            spec = MonitoringSpec.from_dict(toml.loads(config_str))
            spec.resolve()

        assert "No definition for metric test2." in str(e)

    def test_overwrite_population(self):
        config_str = dedent(
            """
            [project]
            name = "foo"
            xaxis = "build_id"
            metrics = []
            start_date = "2022-01-01"
            end_date = "2022-02-01"

            [project.population]
            data_source = "foo"
            boolean_pref = "TRUE"
            branches = ["treatment"]
            dimensions = ["os"]
            group_by_dimension = "os"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"

            [dimensions]
            [dimensions.os]
            select_expression = "os"
            data_source = "foo"
            """
        )

        spec = MonitoringSpec.from_dict(toml.loads(config_str))

        config_str = dedent(
            """
            [project]
            name = "foo bar"
            end_date = "2022-03-01"
            skip_default_metrics = true

            [project.population]
            boolean_pref = "FALSE"
            branches = ["test-1"]
            """
        )

        spec2 = MonitoringSpec.from_dict(toml.loads(config_str))
        spec.merge(spec2)
        cfg = spec.resolve()

        assert cfg.project.name == "foo bar"
        assert cfg.project.xaxis == MonitoringPeriod.BUILD_ID
        assert cfg.project.start_date == datetime(2022, 1, 1, tzinfo=pytz.utc)
        assert cfg.project.end_date == datetime(2022, 3, 1, tzinfo=pytz.utc)
        assert cfg.project.population.data_source.name == "foo"
        assert cfg.project.population.boolean_pref == "FALSE"
        assert cfg.project.population.branches == ["treatment"]
        assert cfg.project.skip_default_metrics
        assert len(cfg.dimensions) == 1

    def test_group_by_fail(self):
        config_str = dedent(
            """
            [project]
            name = "foo"
            xaxis = "build_id"
            metrics = []

            [project.population]
            data_source = "foo"
            group_by_dimension = "os"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"

            [dimensions]
            [dimensions.os]
            select_expression = "os"
            data_source = "foo"
            """
        )

        spec = MonitoringSpec.from_dict(toml.loads(config_str))

        with pytest.raises(ValueError):
            spec.resolve()

    def test_bad_project_dates(self):
        config_str = dedent(
            """
            [project]
            start_date = "My birthday"
            """
        )

        with pytest.raises(ValueError):
            MonitoringSpec.from_dict(toml.loads(config_str))

    def test_bad_project_xaxis(self):
        config_str = dedent(
            """
            [project]
            xaxis = "Nothing"
            """
        )

        with pytest.raises(ValueError):
            MonitoringSpec.from_dict(toml.loads(config_str))

    def test_alert_definition(self):
        config_str = dedent(
            """
            [project]
            alerts = ["test"]
            metrics = ["test_metric"]

            [metrics]
            [metrics.test_metric]
            select_expression = "SELECT 1"
            data_source = "foo"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"

            [alerts]
            [alerts.test]
            type = "threshold"
            metrics = ["test_metric"]
            min = [1]
            max = [3]
            percentiles = [1]
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        assert MetricReference(name="test_metric") in spec.alerts.definitions["test"].metrics
        conf = spec.resolve()
        assert conf.alerts[0].name == "test"

    def test_alert_incorrect_type(self):
        config_str = dedent(
            """
            [project]
            alerts = ["test"]

            [alerts]
            [alerts.test]
            type = "foo"
            """
        )

        with pytest.raises(ValueError):
            MonitoringSpec.from_dict(toml.loads(config_str))

    def test_alert_incorrect_config(self):
        config_str = dedent(
            """
            [project]
            alerts = ["test"]

            [alerts]
            [alerts.test]
            type = "threshold"
            metrics = []
            """
        )

        with pytest.raises(ValueError):
            MonitoringSpec.from_dict(toml.loads(config_str))

    def test_alert_incorrect_number_of_thresholds(self):
        config_str = dedent(
            """
            [project]
            alerts = ["test"]

            [alerts]
            [alerts.test]
            type = "threshold"
            min = [1, 2]
            parameters = [1, 2]
            max = [1]
            metrics = []
            """
        )

        with pytest.raises(ValueError):
            MonitoringSpec.from_dict(toml.loads(config_str))
