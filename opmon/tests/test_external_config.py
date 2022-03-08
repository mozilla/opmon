from pathlib import Path
from textwrap import dedent
from typing import cast
from unittest.mock import Mock

import pytest

from opmon.config import MonitoringSpec
from opmon.external_config import (
    ExternalConfig,
    ExternalConfigCollection,
    entity_from_path,
)


class TestExternalConfig:
    def test_from_github_repo(self):
        external_configs = ExternalConfigCollection.from_github_repo()
        assert external_configs

        assert external_configs.spec_for_experiment("not-existing-conf") is None

    class FakePath:
        def __init__(self, path, config):
            self._path = Path(path)
            self._config = config

        def __getattr__(self, key):
            return getattr(self._path, key)

        def read_text(self):
            return self._config

        def stat(self):
            m = Mock()
            m.st_mtime = 0
            return m

    @pytest.mark.parametrize(
        "path",
        [
            "definitions/fenix.toml",
            "/some/garbage/definitions/fenix.toml",
        ],
    )
    def test_entity_from_path_yields_outcome(self, path: str):
        config = dedent(
            """\
            friendly_name = "I'm an outcome!"
            description = "It's rad to be an outcome."
            """
        )
        fakepath = self.FakePath(path, config)
        entity = entity_from_path(cast(Path, fakepath))
        assert isinstance(entity, ExternalConfig)
        assert entity.slug == "fenix"

    @pytest.mark.parametrize(
        "path",
        [
            "my_cool_experiment.toml",
            "/some/garbage/foo/my_cool_experiment.toml",
        ],
    )
    def test_entity_from_path_yields_config(self, path: str):
        fakepath = self.FakePath(path, "")
        entity = entity_from_path(cast(Path, fakepath))
        assert isinstance(entity, ExternalConfig)
        assert entity.slug == "my_cool_experiment"

    def test_validating_external_config(self, monkeypatch):
        Monitoring = Mock()
        monkeypatch.setattr("opmon.external_config.Monitoring", Monitoring)
        spec = MonitoringSpec.from_dict({})
        extern = ExternalConfig(
            slug="cool_experiment",
            spec=spec,
        )
        extern.validate()
        assert Monitoring.validate.called_once()
