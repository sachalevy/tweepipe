from enum import Enum
from typing import Any


class SnPipelineErrorMsg(Enum):
    """Standardise error codes for sutton client operations."""

    UNSUPPORTED_FILE_FORMAT = "File format is not supported."
    UNSUFFICIENT_FREE_RESOURCES = "Less credentials were available than requested."


class SnPipelineError(Exception):
    def __init__(self, message: SnPipelineErrorMsg, expression: Any = None):
        self.message = message.value
        self.expression = expression
