from datetime import datetime
from textwrap import dedent

import pytest
import pytz
import toml

from opmon import MonitoringPeriod
from opmon.config import MonitoringConfiguration, MonitoringSpec, ProbeReference


class TestConfig:
    def test_trivial_configuration(self, projects):
        spec = MonitoringSpec.from_dict({})
        assert isinstance(spec, MonitoringSpec)
        cfg = spec.resolve()
        assert isinstance(cfg, MonitoringConfiguration)
        assert cfg.probes == []

    def test_probe_definition(self):
        config_str = dedent(
            """
            [project]
            probes = ["test"]

            [probes]
            [probes.test]
            select_expression = "SELECT 1"
            data_source = "foo"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        assert spec.probes.definitions["test"].select_expression == "SELECT 1"
        assert spec.data_sources.definitions["foo"].from_expression == "test"
        conf = spec.resolve()
        assert conf.probes[0].name == "test"
        assert conf.probes[0].data_source.name == "foo"

    def test_duplicate_probes_are_okay(self, experiments):
        config_str = dedent(
            """
            [project]
            probes = ["test", "test"]

            [probes]
            [probes.test]
            select_expression = "SELECT 1"
            data_source = "foo"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve()
        assert len(cfg.probes) == 1

    def test_data_source_definition(self, experiments):
        config_str = dedent(
            """
            [project]
            probes = ["test", "test2"]

            [probes]
            [probes.test]
            select_expression = "SELECT 1"
            data_source = "eggs"

            [probes.test2]
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
        test = [p for p in cfg.probes if p.name == "test"][0]
        test2 = [p for p in cfg.probes if p.name == "test2"][0]
        assert test.data_source.name == "eggs"
        assert "camelot" in test.data_source.from_expression
        assert test2.data_source.name == "silly_knight"
        assert "france" in test2.data_source.from_expression

    def test_merge(self, experiments):
        """Test merging configs"""
        config_str = dedent(
            """
            [probes]
            [probes.test]
            select_expression = "SELECT 1"
            data_source = "foo"

            [probes.test2]
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
            probes = ["test", "test2"]

            [probes]
            [probes.test]
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
        test = [p for p in cfg.probes if p.name == "test"][0]
        test2 = [p for p in cfg.probes if p.name == "test2"][0]
        assert test.select_expression == "SELECT 'd'"
        assert test.data_source.name == "foo"
        assert test.data_source.from_expression == "bar"
        assert test2.select_expression == "SELECT 2"

    def test_unknown_probe_failure(self, experiments):
        config_str = dedent(
            """
            [project]
            name = "foo"
            probes = ["test", "test2"]

            [probes]
            [probes.test]
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

        assert "No definition for probe test2." in str(e)

    def test_overwrite_population(self):
        config_str = dedent(
            """
            [project]
            name = "foo"
            xaxis = "build_id"
            probes = []
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
        assert len(cfg.dimensions) == 1

    def test_group_by_fail(self):
        config_str = dedent(
            """
            [project]
            name = "foo"
            xaxis = "build_id"
            probes = []

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
            probes = ["test_probe"]

            [probes]
            [probes.test_probe]
            select_expression = "SELECT 1"
            data_source = "foo"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"

            [alerts]
            [alerts.test]
            type = "threshold"
            probes = ["test_probe"]
            min = 1
            max = 3
            """
        )
        spec = MonitoringSpec.from_dict(toml.loads(config_str))
        assert ProbeReference(name="test_probe") in spec.alerts.definitions["test"].probes
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
            probes = []
            """
        )

        with pytest.raises(ValueError):
            MonitoringSpec.from_dict(toml.loads(config_str))
