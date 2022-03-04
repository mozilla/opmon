from opmon.config import MonitoringConfiguration, MonitoringSpec


class TestConfig:
    def test_trivial_configuration(self, projects):
        spec = MonitoringSpec.from_dict({})
        assert isinstance(spec, MonitoringSpec)
        cfg = spec.resolve()
        assert isinstance(cfg, MonitoringConfiguration)
        assert cfg.probes == []
