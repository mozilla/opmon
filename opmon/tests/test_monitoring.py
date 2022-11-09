from datetime import datetime
from textwrap import dedent

import pytest
import pytz
import toml
from metric_config_parser.monitoring import MonitoringConfiguration, MonitoringSpec

from opmon import errors
from opmon.config import ConfigLoader
from opmon.monitoring import Monitoring


class TestMonitoring:
    def test_init_monitoring(self):
        conf = MonitoringConfiguration()
        monitoring = Monitoring(project="test", dataset="test", slug="test-foo", config=conf)
        assert monitoring.normalized_slug == "test_foo"

    def test_check_runnable(self):
        config_str = dedent(
            """
            [project]
            metrics = ["test"]

            [metrics]
            [metrics.test]
            select_expression = "SELECT 1"
            data_source = "foo"

            [metrics.test.statistics]
            sum = {}

            [data_sources]
            [data_sources.foo]
            from_expression = "test"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        monitoring = Monitoring(
            project="test",
            dataset="test",
            slug="test-foo",
            config=spec.resolve(experiment=None, configs=ConfigLoader.configs),
        )

        with pytest.raises(errors.NoStartDateException):
            monitoring._check_runnable()

        config_str = dedent(
            """
            [project]
            metrics = ["test"]
            start_date = "2022-01-01"
            end_date = "2022-01-01"

            [metrics]
            [metrics.test]
            select_expression = "SELECT 1"
            data_source = "foo"

            [metrics.test.statistics]
            sum = {}

            [data_sources]
            [data_sources.foo]
            from_expression = "test"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        monitoring = Monitoring(
            project="test",
            dataset="test",
            slug="test-foo",
            config=spec.resolve(experiment=None, configs=ConfigLoader.configs),
        )

        with pytest.raises(errors.EndedException):
            monitoring._check_runnable(current_date=datetime(2022, 2, 1, tzinfo=pytz.utc))

        config_str = dedent(
            """
            [project]
            metrics = ["test"]
            start_date = "2022-01-01"
            end_date = "2022-02-01"

            [metrics]
            [metrics.test]
            select_expression = "SELECT 1"
            data_source = "foo"

            [metrics.test.statistics]
            sum = {}

            [data_sources]
            [data_sources.foo]
            from_expression = "test"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        monitoring = Monitoring(
            project="test",
            dataset="test",
            slug="test-foo",
            config=spec.resolve(experiment=None, configs=ConfigLoader.configs),
        )

        assert (
            monitoring._check_runnable(current_date=datetime(2022, 1, 2, tzinfo=pytz.utc)) is True
        )

    def test_get_metrics_sql_no_metrics(self):
        config_str = dedent(
            """
            [project]
            metrics = []
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        monitoring = Monitoring(
            project="test",
            dataset="test",
            slug="test-foo",
            config=spec.resolve(experiment=None, configs=ConfigLoader.configs),
        )

        assert "population" in monitoring._get_metrics_sql(
            submission_date=datetime(2022, 1, 2, tzinfo=pytz.utc)
        )

    def test_get_metrics_sql(self):
        config_str = dedent(
            """
            [project]
            metrics = ["test"]
            start_date = "2022-01-01"
            end_date = "2022-02-01"

            [project.population]
            channel = "nightly"

            [metrics]
            [metrics.test]
            select_expression = "SELECT 1"
            data_source = "foo"
            type = "scalar"

            [metrics.test.statistics]
            sum = {}

            [data_sources]
            [data_sources.foo]
            from_expression = "test_data_source"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        monitoring = Monitoring(
            project="test",
            dataset="test",
            slug="test-foo",
            config=spec.resolve(experiment=None, configs=ConfigLoader.configs),
        )

        sql = monitoring._get_metrics_sql(submission_date=datetime(2022, 1, 2, tzinfo=pytz.utc))
        assert "SELECT 1" in sql
        assert "test_data_source" in sql

    def test_get_metric_fenix_sql(self):
        config_str = dedent(
            """
            [project]
            metrics = ["test"]
            start_date = "2022-01-01"
            end_date = "2022-02-01"
            platform = "fenix"

            [project.population]
            channel = "nightly"
            data_source = "foo"

            [metrics]
            [metrics.test]
            select_expression = "SELECT 1"
            data_source = "foo"
            type = "scalar"

            [metrics.test.statistics]
            sum = {}

            [data_sources]
            [data_sources.foo]
            from_expression = "{dataset}.test"
            default_dataset = "test"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        monitoring = Monitoring(
            project="test",
            dataset="test",
            slug="test-foo",
            config=spec.resolve(experiment=None, configs=ConfigLoader.configs),
        )

        assert "org_mozilla_fenix." in monitoring._get_metrics_sql(
            submission_date=datetime(2022, 1, 2, tzinfo=pytz.utc)
        )
