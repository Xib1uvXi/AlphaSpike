# AlphaSpike

A Chinese A-share stock screening system that syncs daily bar data from TuShare API and scans for technical analysis signals.

## Features

- **Data Synchronization**: Incremental sync of daily OHLCV data for all A-share stocks via TuShare API
- **Feature Scanning**: Detect candlestick patterns and technical signals across the entire market
- **Redis Caching**: Cache sync status and scan results for fast resumable operations
- **Rich CLI**: Beautiful terminal UI with progress bars and formatted tables

## Technical Indicators

AlphaSpike includes the following built-in feature detectors:

| Feature | Description | Min Data |
|---------|-------------|----------|
| `bbc` | Big Bearish Candle - Gap up followed by large red candle after limit-up day | 1000 days |
| `volume_upper_shadow` | Volume Upper Shadow - High volume with significant upper shadow | 220 days |
| `volume_stagnation` | Volume Stagnation - High volume but price fails to advance | 550 days |
| `high_retracement` | High Retracement - Intraday reversal from highs | 1500 days |
| `consolidation_breakout` | Consolidation Breakout - Volume surge breaking out of tight range | 60 days |

All features use TA-Lib indicators (SMA, ATR, ADX, Bollinger Bands, etc.) and return signals detected in the last 3 trading days.

## Installation

### Prerequisites

- Python 3.10+
- [Poetry](https://python-poetry.org/)
- [TA-Lib](https://ta-lib.org/) C library
- Redis (optional, for caching)
- TuShare Pro account with API token

### Install TA-Lib (macOS)

```bash
brew install ta-lib
```

### Install Dependencies

```bash
make install
# or
poetry install
```

### Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Required environment variables:

```ini
# TuShare API token (get from https://tushare.pro)
TUSHARE_TOKEN=your_token

# SQLite database path
SQLITE_PATH=/path/to/alphaspike.db

# Redis (optional)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
# REDIS_PASSWORD=your_password
```

## Usage

### Sync Daily Bar Data

Sync all stock data up to today (or specified end date):

```bash
make sync

# With specific end date
make sync END_DATE=20251212
```

### Scan for Signals

Scan all symbols for feature signals:

```bash
make scan END_DATE=20251212

# Force rescan (ignore cache)
make scan END_DATE=20251212 NO_CACHE=1
```

Example output:

```
╭─────────────────────────────────────────────────────────────╮
│  AlphaSpike Feature Scanner                                 │
│                                                             │
│  End Date: 2025-12-12  |  Symbols: 4,892  |  Redis: Connected│
╰─────────────────────────────────────────────────────────────╯

⠋ bbc ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:02:15

┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Feature               ┃ Signals ┃ Status                   ┃
┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ bbc                   │      12 │ Scanned (4500 ok, 392 skip)│
│ volume_upper_shadow   │       8 │ Scanned (4700 ok, 192 skip)│
│ consolidation_breakout│      67 │ Cached                   │
└───────────────────────┴─────────┴──────────────────────────┘

Scan completed in 2m 15s | Total signals: 155
```

### Clear Cache

```bash
# Clear all cache
make redis-clear

# Clear feature scan cache only
make redis-clear-feature

# Clear datahub sync cache only
make redis-clear-datahub
```

## Development

### Run Tests

```bash
# All tests
make test

# With coverage
make test-cov

# Single test file
poetry run pytest tests/test_symbol.py -v

# Specific test
poetry run pytest tests/test_symbol.py::test_load_sse_symbols -v
```

### Code Quality

```bash
# Lint
make lint

# Format code
make format

# Check formatting
make check
```

## Architecture

```
src/
├── alphaspike/          # Feature scanner CLI
│   ├── cli.py           # CLI entry point with rich UI
│   ├── scanner.py       # Feature registry and scan logic
│   └── cache.py         # Feature result caching
├── datahub/             # Data acquisition
│   ├── main.py          # Sync entry point
│   ├── symbol.py        # Stock symbol loading
│   ├── daily_bar.py     # Daily bar sync orchestration
│   ├── tushare.py       # TuShare API wrapper
│   ├── db.py            # SQLite operations
│   ├── cache.py         # Sync status caching
│   └── trading_calendar.py
└── feature/             # Signal detection modules
    ├── bbc.py
    ├── volume_upper_shadow.py
    ├── volume_stagnation.py
    ├── high_retracement.py
    └── consolidation_breakout.py
```

## License

MIT
