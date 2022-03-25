"""Custom OpMon exception types."""


class OpmonException(Exception):
    """Exception thrown when an opmon project is invalid."""

    def __init__(self, message):
        """Initialize exception."""
        super().__init__(message)


class NoStartDateException(OpmonException):
    """Exception thrown when no start date has been defined."""

    def __init__(self, slug, message="Project has no start date."):
        """Initialize exception."""
        super().__init__(f"{slug} -> {message}")


class EndedException(OpmonException):
    """Exception thrown when the project has already ended."""

    def __init__(self, slug, message="Project has already ended."):
        """Initialize exception."""
        super().__init__(f"{slug} -> {message}")


class ConfigurationException(OpmonException):
    """Exception thrown when the configuration is incorrect."""

    def __init__(self, slug, message="Project has been incorrectly configured."):
        """Initialize exception."""
        super().__init__(f"{slug} -> {message}")
