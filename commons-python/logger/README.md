# NVSentinel Commons (Python)

Common utilities for NVSentinel Python modules. This package mirrors the functionality of the Go `commons` package to ensure consistency across all NVSentinel components.

## Installation

### From Source (Development)

```bash
cd commons-python/logger
poetry install
```

### As Dependency in Other Modules

Add to your `pyproject.toml`:

```toml
[tool.poetry.dependencies]
commons-python = { path = "../../commons-python/logger", develop = true }
```

## Usage

### Structured Logging

The logging module provides structured JSON logging that matches the format used by Go modules via `slog`.

```python
from logger import set_default_structured_logger_with_level
import logging

# At application startup
set_default_structured_logger_with_level("gpu-health-monitor", "v0.4.1", "info")

# Then use standard logging throughout your application
log = logging.getLogger(__name__)
log.info("Application started")
log.info("Processing GPUs", extra={"gpu_count": 8})
log.error("Failed to connect", extra={"error": "connection refused"})
```

Output:

```json
{"time": "2025-12-09T10:00:00.123Z", "level": "INFO", "msg": "Application started", "module": "gpu-health-monitor", "version": "v0.4.1"}
{"time": "2025-12-09T10:00:00.124Z", "level": "INFO", "msg": "Processing GPUs", "module": "gpu-health-monitor", "version": "v0.4.1", "gpu_count": 8}
{"time": "2025-12-09T10:00:00.125Z", "level": "ERROR", "msg": "Failed to connect", "module": "gpu-health-monitor", "version": "v0.4.1", "error": "connection refused"}
```

### Log Levels

Supported log levels: `debug`, `info`, `warn`, `error`

## API Reference

### Functions

| Function | Description |
|----------|-------------|
| `set_default_structured_logger_with_level(module, version, level)` | Initialize root logger with module context and explicit level |

## Comparison with Go Implementation

This module mirrors `commons/pkg/logger/logger.go`:

| Go Function | Python Equivalent |
|-------------|-------------------|
| `SetDefaultStructuredLoggerWithLevel(module, version, level)` | `set_default_structured_logger_with_level(module, version, level)` |

## Development

### Running Tests

```bash
cd commons-python/logger
make test
```

### Code Formatting

```bash
make format
```

## License

Apache License 2.0 - See [LICENSE](../../LICENSE) for details.
