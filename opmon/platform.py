"""Parse and handle platform specific configs."""

from pathlib import Path
from typing import Any, Dict, MutableMapping

import attr
import toml

platform_config = toml.load(Path(__file__).parent.parent / "platform_config.toml")


class PlatformConfigurationException(Exception):
    """Custom exception type for Jetstream platform configuration related issues."""

    pass


@attr.s(auto_attribs=True)
class Platform:
    """
    Platform configuration object. Contains all required settings for OpMon.

    :param is_glean_app:
    :type is_glean_app: boolean

    :returns: returns an instance of the object with all configuration settings as attributes
    :rtype: Platform
    """

    def _check_value_not_null(self, attribute, value):
        if not value and str(value).lower() == "none":
            raise PlatformConfigurationException(
                "'%s' attribute requires a value, please double check \
                    platform configuration file. Value provided: %s"
                % (attribute.name, str(value))
            )

    app_name: str = attr.ib(validator=_check_value_not_null)
    is_glean_app: bool = True


def _generate_platform_config(config: MutableMapping[str, Any]) -> Dict[str, Platform]:
    """Take platform configuration and generate platform object map."""
    processed_config = dict()

    for platform, platform_config in config["platform"].items():
        processed_config[platform] = {
            "is_glean_app": platform_config.get("is_glean_app", True),
            "app_name": platform,
        }

    return {
        platform: Platform(**platform_config)
        for platform, platform_config in processed_config.items()
    }


PLATFORM_CONFIGS = _generate_platform_config(platform_config)
