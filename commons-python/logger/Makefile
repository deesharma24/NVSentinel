# Commons Python Makefile
# Shared Python utilities for NVSentinel modules

# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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

.PHONY: all setup test lint format clean help

# Default target
all: lint-test

# Setup Poetry environment
setup:
	@echo "Setting up Poetry environment..."
	poetry config virtualenvs.in-project true
	poetry install

# Run tests with coverage
test: setup
	@echo "Running tests..."
	poetry run pytest -v --cov=logger --cov-report=xml --cov-report=term-missing

# Run linter (black check)
lint: setup
	@echo "Running linter..."
	poetry run black --check .

# Format code
format: setup
	@echo "Formatting code..."
	poetry run black .

# Combined lint and test
lint-test: setup lint test
	@echo "All checks passed!"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf .venv
	rm -rf dist
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf __pycache__
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

# Build package
build: setup
	@echo "Building package..."
	poetry build

# Help
help:
	@echo "Commons Python Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all       - Run lint-test (default)"
	@echo "  setup     - Set up Poetry environment"
	@echo "  test      - Run tests with coverage"
	@echo "  lint      - Run linter (black check)"
	@echo "  format    - Format code with black"
	@echo "  lint-test - Run lint and tests"
	@echo "  build     - Build package"
	@echo "  clean     - Clean build artifacts"
	@echo "  help      - Show this help"

