class OpmonException(Exception):
    """Exception thrown when an opmon project is invalid."""

    def __init__(self, message):
        super().__init__(message)


class NoStartDateException(OpmonException):
    def __init__(self, slug, message="Project has no start date."):
        super().__init__(f"{slug} -> {message}")


class EndedException(OpmonException):
    def __init__(self, slug, message="Project has already ended."):
        super().__init__(f"{slug} -> {message}")


class ConfigurationException(OpmonException):
    def __init__(self, slug, message="Project has been incorrectly configured."):
        super().__init__(f"{slug} -> {message}")
