.PHONY: install test lint format sync scan backtest track feature-eng clean help redis-clear redis-clear-feature redis-clear-datahub benchmark

# Default target
help:
	@echo "Available targets:"
	@echo "  install              - Install dependencies using uv"
	@echo "  test                 - Run all tests"
	@echo "  test-cov             - Run tests with coverage report"
	@echo "  benchmark            - Run performance benchmarks"
	@echo "  lint                 - Run pylint on source code"
	@echo "  format               - Format code with black and isort"
	@echo "  check                - Check code formatting without changes"
	@echo "  sync                 - Sync all daily bar data"
	@echo "  scan                 - Scan all symbols for feature signals (requires END_DATE, optional: FEATURE)"
	@echo "  backtest             - Run yearly backtest for a feature (requires YEAR, FEATURE)"
	@echo "  track                - Track feature signal performance (optional: START_DATE, END_DATE, FEATURE, ANALYZE)"
	@echo "  feature-eng          - Run feature engineering pipeline (optional: FEATURE, STATS, EXPORT)"
	@echo "  redis-clear          - Clear all Redis cache keys"
	@echo "  redis-clear-feature  - Clear feature scan cache only"
	@echo "  redis-clear-datahub  - Clear datahub cache only"
	@echo "  clean                - Remove cache and temporary files"

# Install dependencies
install:
	uv sync

# Run all tests
test:
	uv run pytest -v

# Run tests with coverage
test-cov:
	uv run pytest --cov=src --cov-report=term-missing

# Run performance benchmarks
benchmark:
	uv run pytest tests/test_benchmark.py -v -s -m benchmark

# Run pylint
lint:
	mkdir -p .data/pylint
	PYLINTHOME=.data/pylint uv run pylint src

# Format code
format:
	uv run black src tests
	uv run isort src tests

# Check formatting without changes
check:
	uv run black --check src tests
	uv run isort --check-only src tests

# Sync all daily bar data
sync:
	uv run python -m src.datahub.main $(if $(END_DATE),--end-date $(END_DATE),)

# Scan all symbols for feature signals
scan:
ifndef END_DATE
	$(error END_DATE is required. Usage: make scan END_DATE=20251212)
endif
	uv run python -m src.alphaspike.cli --end-date $(END_DATE) $(if $(FEATURE),--feature $(FEATURE),) $(if $(NO_CACHE),--no-cache,) $(if $(WORKERS),--workers $(WORKERS),)

# Run yearly backtest for a feature
backtest:
ifndef YEAR
	$(error YEAR is required. Usage: make backtest YEAR=2025 FEATURE=bullish_cannon)
endif
ifndef FEATURE
	$(error FEATURE is required. Usage: make backtest YEAR=2025 FEATURE=bullish_cannon)
endif
	uv run python -m src.backtest.cli --year $(YEAR) --feature $(FEATURE) $(if $(HOLDING_DAYS),--holding-days $(HOLDING_DAYS),) $(if $(WORKERS),--workers $(WORKERS),)

# Track feature signal performance
track:
	uv run python -m src.track.cli $(if $(START_DATE),--start-date $(START_DATE),) $(if $(END_DATE),--end-date $(END_DATE),) $(if $(FEATURE),--feature $(FEATURE),) $(if $(ANALYZE),--analyze,)

# Run feature engineering pipeline
feature-eng:
	uv run python -m src.feature_engineering.cli $(if $(FEATURE),--feature $(FEATURE),) $(if $(FULL),--full,) $(if $(START_DATE),--start-date $(START_DATE),) $(if $(END_DATE),--end-date $(END_DATE),) $(if $(STATS),--stats,) $(if $(ANALYZE),--analyze,) $(if $(EXPORT),--export $(EXPORT),)

# Clear all Redis cache keys
redis-clear:
	uv run python -m src.datahub.clear_cache --all

# Clear feature scan cache only
redis-clear-feature:
	uv run python -m src.datahub.clear_cache --feature

# Clear datahub cache only
redis-clear-datahub:
	uv run python -m src.datahub.clear_cache --datahub

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
