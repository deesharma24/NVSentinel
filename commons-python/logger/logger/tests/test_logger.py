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

"""Tests for the structured logging module."""

import json
import logging
from io import StringIO

from logger import set_default_structured_logger_with_level
from logger.logger import StructuredFormatter, _parse_log_level


class TestParseLogLevel:
    """Tests for _parse_log_level function."""

    def test_debug_level(self):
        assert _parse_log_level("debug") == logging.DEBUG
        assert _parse_log_level("DEBUG") == logging.DEBUG
        assert _parse_log_level("  debug  ") == logging.DEBUG

    def test_info_level(self):
        assert _parse_log_level("info") == logging.INFO
        assert _parse_log_level("INFO") == logging.INFO

    def test_warn_level(self):
        assert _parse_log_level("warn") == logging.WARNING
        assert _parse_log_level("warning") == logging.WARNING
        assert _parse_log_level("WARN") == logging.WARNING

    def test_error_level(self):
        assert _parse_log_level("error") == logging.ERROR
        assert _parse_log_level("ERROR") == logging.ERROR

    def test_unknown_defaults_to_info(self):
        assert _parse_log_level("unknown") == logging.INFO
        assert _parse_log_level("") == logging.INFO
        assert _parse_log_level("trace") == logging.INFO


class TestStructuredFormatter:
    """Tests for StructuredFormatter class."""

    def test_log_format_matches_go_slog(self):
        """Verify output format matches Go slog JSON format."""
        formatter = StructuredFormatter(module="test-module", version="v1.0.0")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        log_entry = json.loads(output)

        # Verify required fields exist
        assert "time" in log_entry
        assert "level" in log_entry
        assert "msg" in log_entry
        assert "module" in log_entry
        assert "version" in log_entry

        # Verify values
        assert log_entry["level"] == "INFO"
        assert log_entry["msg"] == "Test message"
        assert log_entry["module"] == "test-module"
        assert log_entry["version"] == "v1.0.0"

    def test_field_ordering(self):
        """Verify fields are ordered: time, level, msg, module, version, then extras."""
        formatter = StructuredFormatter(module="test", version="v1.0.0")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        keys = list(json.loads(output).keys())

        # First 5 keys should be in order
        assert keys[0] == "time"
        assert keys[1] == "level"
        assert keys[2] == "msg"
        assert keys[3] == "module"
        assert keys[4] == "version"

    def test_add_source_for_debug(self):
        """Verify source information is added for debug level when enabled."""
        formatter = StructuredFormatter(module="test", version="v1.0.0", add_source=True)

        record = logging.LogRecord(
            name="test",
            level=logging.DEBUG,
            pathname="/path/to/test.py",
            lineno=42,
            msg="Debug message",
            args=(),
            exc_info=None,
        )
        record.funcName = "test_function"

        output = formatter.format(record)
        log_entry = json.loads(output)

        assert "source" in log_entry
        assert log_entry["source"]["file"] == "/path/to/test.py"
        assert log_entry["source"]["line"] == 42
        assert log_entry["source"]["function"] == "test_function"

    def test_extra_fields_included(self):
        """Verify extra fields from logging calls are included."""
        formatter = StructuredFormatter(module="test", version="v1.0.0")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )
        record.gpu_count = 8
        record.node_name = "worker-1"

        output = formatter.format(record)
        log_entry = json.loads(output)

        assert log_entry["gpu_count"] == 8
        assert log_entry["node_name"] == "worker-1"


class TestSetDefaultStructuredLoggerWithLevel:
    """Tests for set_default_structured_logger_with_level function."""

    def teardown_method(self):
        """Clean up root logger after each test."""
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_sets_correct_level(self):
        set_default_structured_logger_with_level("test", "v1.0.0", "error")
        assert logging.getLogger().level == logging.ERROR

    def test_replaces_existing_handlers(self):
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(logging.StreamHandler())
        root.addHandler(logging.StreamHandler())
        assert len(root.handlers) == 2

        set_default_structured_logger_with_level("test", "v1.0.0", "info")
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, StructuredFormatter)


class TestIntegration:
    """Integration tests for the logging module."""

    def teardown_method(self):
        """Clean up root logger after each test."""
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_full_logging_workflow(self):
        """Test complete logging workflow similar to actual usage."""
        set_default_structured_logger_with_level("gpu-health-monitor", "v0.4.1", "info")

        # Capture output
        captured = StringIO()
        logging.getLogger().handlers[0].stream = captured

        # Log some messages
        logging.info("Application started")
        logging.info("Processing GPUs", extra={"gpu_count": 8})
        logging.warning("High temperature detected", extra={"gpu_id": 0, "temp_c": 85})

        # Verify output
        lines = captured.getvalue().strip().split("\n")
        assert len(lines) == 3

        for line in lines:
            entry = json.loads(line)
            assert entry["module"] == "gpu-health-monitor"
            assert entry["version"] == "v0.4.1"
            assert "time" in entry
