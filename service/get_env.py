import os
from typing import Optional


class MissingEnvironmentVariableError(Exception):
    """Raised when a required environment variable is missing."""

    def __init__(self, key: str):
        self.message = f"Required environment variable '{key}' is not set."
        super().__init__(self.message)


def get_env(
    key: str,
    default: Optional[str] = None,
    no_raise_exception: bool = False,
) -> Optional[str]:
    value = os.getenv(key, default)
    if value is not None:
        return value

    if no_raise_exception:
        return default

    raise MissingEnvironmentVariableError(key)
