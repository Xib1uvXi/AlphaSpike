.PHONY: install test lint format sync scan backtest clean help redis-clear redis-clear-feature redis-clear-datahub benchmark

# Default target
help:
	@echo "Available targets:"
	@echo "  install              - Install dependencies using Poetry"
	@echo "  test                 - Run all tests"
	@echo "  test-cov             - Run tests with coverage report"
	@echo "  benchmark            - Run performance benchmarks"
	@echo "  lint                 - Run pylint on source code"
	@echo "  format               - Format code with black and isort"
	@echo "  check                - Check code formatting without changes"
	@echo "  sync                 - Sync all daily bar data"
	@echo "  scan                 - Scan all symbols for feature signals (requires END_DATE)"
	@echo "  backtest             - Run yearly backtest for a feature (requires YEAR, FEATURE)"
	@echo "  redis-clear          - Clear all Redis cache keys"
	@echo "  redis-clear-feature  - Clear feature scan cache only"
	@echo "  redis-clear-datahub  - Clear datahub cache only"
	@echo "  clean                - Remove cache and temporary files"

# Install dependencies
install:
	poetry install

# Run all tests
test:
	poetry run pytest -v

# Run tests with coverage
test-cov:
	poetry run pytest --cov=src --cov-report=term-missing

# Run performance benchmarks
benchmark:
	poetry run pytest tests/test_benchmark.py -v -s -m benchmark

# Run pylint
lint:
	poetry run pylint src

# Format code
format:
	poetry run black src tests
	poetry run isort src tests

# Check formatting without changes
check:
	poetry run black --check src tests
	poetry run isort --check-only src tests

# Sync all daily bar data
sync:
	poetry run python -m src.datahub.main $(if $(END_DATE),--end-date $(END_DATE),)

# Scan all symbols for feature signals
scan:
ifndef END_DATE
	$(error END_DATE is required. Usage: make scan END_DATE=20251212)
endif
	poetry run python -m src.alphaspike.cli --end-date $(END_DATE) $(if $(NO_CACHE),--no-cache,) $(if $(WORKERS),--workers $(WORKERS),)

# Run yearly backtest for a feature
backtest:
ifndef YEAR
	$(error YEAR is required. Usage: make backtest YEAR=2025 FEATURE=bullish_cannon)
endif
ifndef FEATURE
	$(error FEATURE is required. Usage: make backtest YEAR=2025 FEATURE=bullish_cannon)
endif
	poetry run python -m src.backtest.cli --year $(YEAR) --feature $(FEATURE) $(if $(HOLDING_DAYS),--holding-days $(HOLDING_DAYS),) $(if $(WORKERS),--workers $(WORKERS),)

# Clear all Redis cache keys
redis-clear:
	poetry run python -m src.datahub.clear_cache --all

# Clear feature scan cache only
redis-clear-feature:
	poetry run python -m src.datahub.clear_cache --feature

# Clear datahub cache only
redis-clear-datahub:
	poetry run python -m src.datahub.clear_cache --datahub

# Clean cache and temporary files
clean:
	rm -rf .pytest_cache
	rm -rf __pycache__
	rm -rf src/**/__pycache__
	rm -rf tests/__pycache__
	rm -rf .coverage
	rm -rf htmlcov
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
