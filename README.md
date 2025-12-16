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
| `bullish_cannon` | Bullish Cannon - Strong bullish candle + consolidation + breakout | 30 days |

### Detection Criteria

**Volume Upper Shadow (放量上影线)**
- Upper shadow ratio > 2%
- Volume surge: 1.2x to 2x of previous day's 10-day MA volume
- Price quantile < 45% (based on last 200 days)
- Close > MA5 and Close > MA10
- MA3 > MA5 (short-term trend confirmation)
- No limit-up in last 3 days, cumulative gain < 15%

**Volume Stagnation (放量滞涨)**
- Volume surge: vol > vol_ma10 * 1.5
- Price stagnation: -3% < daily change < 3%
- Close > MA10
- MA3 > MA5 (short-term trend confirmation)
- At least 3 consecutive days meeting criteria
- Price quantile 5-45% (based on last 500 days)

**Bullish Cannon (多方炮)**

*First Cannon (day0):*
- Return >= 7% (strong bullish day)
- Volume >= vol_ma5 * 1.8 (volume surge)
- Body/Range >= 40% (solid body, not doji)
- Upper wick/Range <= 50% (limited upper shadow)
- Close > HHV(high, 20) (breaks 20-day high)

*Cannon Body (day1 to dayk, k=1..3):*
- Mean volume <= vol0 * 0.8 (volume contraction)
- Max amplitude <= 8% (limited volatility)
- Min low >= open0 (holds above first cannon's open)

*Second Cannon (day(k+1)):*
- Close > max(high1..k) (breaks body's high)
- Volume >= mean(vol1..k) * 1.0 (volume at least matches body)
- (High - Close) / Range <= 25% (closes near high)

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

# Custom worker count (default: 6)
make scan END_DATE=20251212 WORKERS=4
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

### Backtest Features

Run yearly backtest for a feature signal:

```bash
make backtest YEAR=2025 FEATURE=bullish_cannon

# Custom holding days (default: 5)
make backtest YEAR=2025 FEATURE=bullish_cannon HOLDING_DAYS=10

# Custom worker count (default: 6)
make backtest YEAR=2025 FEATURE=bullish_cannon WORKERS=4
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

## Performance & Memory Usage

### Scan

The scan command uses parallel processing with batch data loading for optimal performance:

- **Memory**: ~2-3GB for full market scan (5000+ stocks)
- **Strategy**: All stock data is loaded into memory in a single database query, then processed in parallel using `ProcessPoolExecutor`
- **Workers**: Default 6 parallel workers (configurable via `WORKERS` parameter)

### Backtest

The backtest command uses stock-level parallelization:

- **Memory**: ~2-3GB for full year backtest
- **Strategy**: Batch load all stock data, then process each stock across all trading days in parallel
- **Data Flow**:
  1. Single database query loads all daily bar data
  2. Extract trading days from loaded data (no calendar dependency)
  3. Parallel process each stock's signals across the year
  4. Aggregate results and calculate statistics

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
    ├── bullish_cannon.py
    ├── volume_upper_shadow.py
    ├── volume_stagnation.py
    ├── high_retracement.py
    └── consolidation_breakout.py
```
