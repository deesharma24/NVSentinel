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
Structured logging utilities for NVSentinel Python modules.

This module provides structured JSON logging that matches the format used by
Go modules via slog. It ensures consistent log output across all NVSentinel
components regardless of implementation language.

Example usage:
    from logger import set_default_structured_logger_with_level
    import logging

    # At application startup
    set_default_structured_logger_with_level("gpu-health-monitor", "v0.4.1", "info")

    # Then use standard logging
    log = logging.getLogger(__name__)
    log.info("Application started", extra={"gpu_count": 8})

Output format matches Go slog:
    {"time": "2025-12-09T10:00:00.123456Z", "level": "INFO", "msg": "Application started",
     "module": "gpu-health-monitor", "version": "v0.4.1", "gpu_count": 8}
"""

from logger.logger import set_default_structured_logger_with_level

__version__ = "0.1.0"
__all__ = ["__version__", "set_default_structured_logger_with_level"]

