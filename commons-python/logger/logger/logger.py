# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Structured JSON logging implementation for NVSentinel.

This module mirrors the functionality of commons/pkg/logger/logger.go in Go,
providing consistent structured logging across all NVSentinel Python modules.

The log format matches Go's slog JSON output:
{
    "time": "2025-12-09T15:37:51.105805+05:30",
    "level": "INFO",
    "msg": "message here",
    "module": "module-name",
    "version": "v1.0.0",
    ... additional fields ...
}
"""

import logging
import sys
from datetime import datetime
from typing import Any, Final

from pythonjsonlogger import jsonlogger

# Log level mapping from string to logging constants
_LEVEL_MAP: Final[dict[str, int]] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class StructuredFormatter(jsonlogger.JsonFormatter):
    """
    JSON formatter that produces output matching Go's slog format.

    Automatically includes module name and version in every log entry,
    similar to how Go's SetDefaultStructuredLogger works.

    Attributes:
        _module: The name of the module/application using the logger.
        _version: The version of the module/application.
        _add_source: Whether to include source file/line information (debug only).
    """

    __slots__ = ("_module", "_version", "_add_source")

    def __init__(
        self,
        module: str,
        version: str,
        add_source: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._module = module
        self._version = version
        self._add_source = add_source

    def add_fields(
        self, log_record: dict[str, Any], record: logging.LogRecord, message_dict: dict[str, Any]
    ) -> None:
        """Add standard fields to match Go slog output format."""
        super().add_fields(log_record, record, message_dict)

        # Format timestamp to match Go's slog JSONHandler format exactly:
        # RFC3339 with nanosecond precision and local timezone offset
        # Go output: "2025-12-09T15:37:51.105805414+05:30"
        # Python output: "2025-12-09T15:37:51.105805+05:30" (microseconds, 6 digits)
        log_record["time"] = datetime.now().astimezone().isoformat()
        
        # Use uppercase level names to match Go slog
        log_record["level"] = record.levelname.upper()
        
        # Rename 'message' to 'msg' to match Go slog
        if "message" in log_record:
            log_record["msg"] = log_record.pop("message")
        
        # Add module context (matches Go's .With("module", module, "version", version))
        log_record["module"] = self._module
        log_record["version"] = self._version
        
        # Add source information for debug level (matches Go's AddSource option)
        if self._add_source and record.levelno <= logging.DEBUG:
            log_record["source"] = {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName,
            }
        
        # Ensure consistent field ordering: time, level, msg, module, version, then extras
        # Python 3.7+ dicts maintain insertion order
        ordered = {}
        for key in ["time", "level", "msg", "module", "version", "source"]:
            if key in log_record:
                ordered[key] = log_record.pop(key)
        ordered.update(log_record)
        log_record.clear()
        log_record.update(ordered)


def _parse_log_level(level: str) -> int:
    """Convert a string log level to a logging constant."""
    return _LEVEL_MAP.get(level.lower().strip(), logging.INFO)


def set_default_structured_logger_with_level(module: str, version: str, level: str) -> None:
    """
    Initialize the structured logger with the specified log level.
    
    This mirrors the Go SetDefaultStructuredLoggerWithLevel function.
    
    Args:
        module: The name of the module/application using the logger.
        version: The version of the module/application (e.g., "v1.0.0").
        level: The log level as a string (e.g., "debug", "info", "warn", "error").
    
    Example:
        >>> set_default_structured_logger_with_level("my-app", "v1.0.0", "debug")
        >>> import logging
        >>> logging.debug("Debug message")
        {"time": "2025-12-09T15:37:51.105805+05:30", "level": "DEBUG", "msg": "Debug message",
         "module": "my-app", "version": "v1.0.0", "source": {"file": "...", "line": 42}}
    """
    log_level = _parse_log_level(level)
    add_source = log_level <= logging.DEBUG
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove all existing handlers from root logger
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create handler that writes to stderr (matches Go behavior)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(log_level)
    
    # Apply structured formatter
    formatter = StructuredFormatter(
        module=module,
        version=version,
        add_source=add_source,
    )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

