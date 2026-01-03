# AlphaSpike

A Chinese A-share stock screening system that syncs daily bar data from TuShare API and scans for technical analysis signals.

## Features

- **Data Synchronization**: Incremental sync of daily OHLCV data for all A-share stocks via TuShare API
- **Feature Scanning**: Detect candlestick patterns and technical signals across the entire market
- **Hybrid Caching**: Redis hot cache + SQLite persistence for feature scan results
- **Rich CLI**: Beautiful terminal UI with progress bars and formatted tables

## Technical Indicators

AlphaSpike includes the following built-in feature detectors:

| Feature | Description | Min Data |
|---------|-------------|----------|
| `bbc` | Big Bearish Candle - Gap up followed by large red candle after limit-up day | 1000 days |
| `volume_upper_shadow` | Volume Upper Shadow - High volume with significant upper shadow | 220 days |
| `volume_upper_shadow_v2` | Volume Upper Shadow V2 - Optimized version (cross-star + momentum + low position) | 220 days |
| `volume_stagnation` | Volume Stagnation - High volume but price fails to advance | 550 days |
| `high_retracement` | High Retracement - Intraday reversal from highs | 1500 days |
| `consolidation_breakout` | Consolidation Breakout - Volume surge breaking out of tight range | 60 days |
| `bullish_cannon` | Bullish Cannon - Strong bullish candle + consolidation + breakout | 30 days |
| `four_edge` | Four-Edge - ATR volatility + structure pattern filter | 130 days |
| `weak_to_strong` | Weak to Strong - Consecutive limit-ups followed by gap-down weakness | 5 days |

### Detection Criteria

**Volume Upper Shadow (放量上影线)**
- Upper shadow ratio > 2%
- Volume surge: 1.2x to 2x of previous day's 10-day MA volume
- Price quantile < 45% (based on last 200 days)
- Close > MA5 and Close > MA10
- MA3 > MA5 (short-term trend confirmation)
- No limit-up in last 3 days, cumulative gain < 15%

**Volume Upper Shadow V2 (放量上影线优化版)**

Statistically optimized version based on 42,000+ signal analysis. Key insight: the best signals are **cross-star patterns at low positions with prior momentum**, not large bearish candles.

- All conditions from original, plus:
- body_ratio < 0.20 (cross-star pattern, small real body)
- gain_2d > 3% (2-day cumulative gain, momentum confirmation)
- price_quantile < 0.25 (low position, bottom signals)

Performance improvement vs original:
- All Positive ratio: 40% vs 33% (+7pp)
- Avg 3D return: +2.31% vs +0.87% (+165%)
- Signal count: ~1-2/day vs ~17/day (higher precision)

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

**Four-Edge (四层过滤)**

Signal logic: Edge1 AND Edge2 AND Edge3 AND Edge4

*Edge 1 - ATR Volatility:*
- ATR(14) / Close >= 2.5% (sufficient volatility for active trading)
- Recommended range: 2%–4%

*Edge 2 - Structure Patterns (Type 1 OR Type 2 OR Type 3):*

Type 1 - Compression → Expansion:
- Box width: (HHV20 - LLV20) / Close <= 18%
- ATR convergence: ATR14 < SMA(ATR14, 10)
- MA20 slope: abs(MA20(T)/MA20(T-5) - 1) <= 0.8%
- Close to MA: abs(Close/MA20 - 1) <= 3%

Type 2 - Trend Pullback:
- Trend: MA20 > MA60 > MA120
- Pullback distance: Close/MA20 in [0.97, 1.03]
- Volume contraction: SMA(Vol,3) < SMA(Vol,10) or Vol < SMA(Vol,5)
- Support not broken: LLV5 >= MA60 * 0.98

Type 3 - Breakout Retest:
- Breakout occurred 3-10 days ago: Close(T-k) > HHV20(T-k-1), Vol >= Vol_MA5 * 1.5
- Retest holds support: LLV3 >= breakout_level * 0.99
- Volume contraction: SMA(Vol,3) < SMA(Vol,10)
- Retest end signal: Close > Open or Close > MA5

*Edge 3 - Entry Signals (based on Edge 2 structure type):*

Common definitions:
- AR (Amount Ratio): Amount(T) / SMA(Amount, 5)(T)
- CloseStrong: Close >= High - 0.3 * (High - Low)
- BullishCandle: Close > Open AND CloseStrong AND RealBody/Range >= 0.5
- VolUp: AR >= 1.3
- StopDrop: LLV3 >= LLV3_prev (not making new low)

COMPRESS entry:
- Close > HHV20_prev (breakout)
- AR >= 1.3 (amount surge)
- CloseStrong (close in upper 70% of range)

PULLBACK entry:
- Branch 1: Close > MA20 AND AR >= 1.2
- Branch 2: StopDrop AND BullishCandle AND VolUp

RETEST entry:
- HoldBreakout: LLV3 >= breakout_level * 0.99, SMA(Amount,3) < SMA(Amount,10), demand present
- Close > High_prev (today's close > yesterday's high)
- AR >= 1.3 (amount surge)

*Edge 4 - Overheated Rejection Filter:*

Rejects signals when stock has risen too fast (overheated):
- ConsecutiveBullishCandles >= 4 (last 4 days all satisfy BullishCandle)
- Sum(pct_chg, 4) >= 15% (cumulative return >= 15%)

If BOTH conditions are true → Reject signal
Otherwise → Pass

BullishCandle (Edge 4 version, simpler):
- Close > Open
- Close >= High - 0.3 * (High - Low) (CloseStrong)

**Weak to Strong (弱转强)**

Detects stocks showing weakness after consecutive limit-up days:
- T-2: Limit-up day (pct_chg > 9.5% for main board, > 19.2% for ChiNext)
- T-1: Limit-up day (same threshold)
- T: Gap-down open (open < T-1 close)
- T: High stays below T-1 close (no recovery)

All features use TA-Lib indicators (SMA, ATR, ADX, Bollinger Bands, etc.) and return signals detected in the last 1-3 trading days (varies by feature).

## Installation

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
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
uv sync
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

# Redis (optional, for hot cache acceleration)
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

### Track Feature Performance

Analyze stored feature signal performance (1d/2d/3d returns):

```bash
# Track all features
make track

# Track specific feature
make track FEATURE=volume_upper_shadow

# Track specific date
make track END_DATE=20251215
```

#### Analyze Signals

Analyze signal performance with detailed categorization:

```bash
# Analyze all features (shows only all-negative signals)
make track ANALYZE=1

# Analyze specific feature (shows all three categories)
make track ANALYZE=1 FEATURE=four_edge

# Analyze specific date
make track ANALYZE=1 END_DATE=20251215
```

When analyzing **all features**, shows summary and all-negative signals only:

```
                          All-Negative Signal Summary
┏━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━┳━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃ Feature        ┃ Total ┃ Negative ┃ Ratio ┃ Avg 1D ┃ Avg 2D ┃ Avg 3D ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━╇━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│ bullish_cannon │     2 │        1 │ 50.0% │ -4.30% │ -3.45% │ -6.28% │
│ four_edge      │    86 │       35 │ 40.7% │ -2.92% │ -3.97% │ -4.39% │
└────────────────┴───────┴──────────┴───────┴────────┴────────┴────────┘
```

When analyzing a **specific feature**, shows all signals in three categories:

```
four_edge - Total: 86 valid signals

All Positive (1d>0, 2d>0, 3d>0) - 25 signals (29.07%) | Avg: +2.49% / +2.97% / +3.65%
┏━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃ Stock  ┃ Date       ┃     1D ┃     2D ┃     3D ┃
┡━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│ 605389 │ 2025-12-17 │ +0.13% │ +1.49% │ +6.19% │
│ ...    │            │        │        │        │
└────────┴────────────┴────────┴────────┴────────┘

Mixed (some positive, some negative) - 26 signals (30.23%) | Avg: -0.92% / +0.10% / +0.64%
┏━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃ Stock  ┃ Date       ┃     1D ┃     2D ┃     3D ┃
┡━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│ 603759 │ 2025-12-17 │ -0.20% │ +1.00% │ +2.60% │
│ ...    │            │        │        │        │
└────────┴────────────┴────────┴────────┴────────┘

All Negative (1d<0, 2d<0, 3d<0) - 35 signals (40.7%) | Avg: -2.92% / -3.97% / -4.39%
┏━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃ Stock  ┃ Date       ┃     1D ┃     2D ┃     3D ┃
┡━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│ 603155 │ 2025-12-17 │ -0.92% │ -0.23% │ -0.17% │
│ ...    │            │        │        │        │
└────────┴────────────┴────────┴────────┴────────┘
```

The ratio column uses color coding: >30% red, 15-30% yellow, <15% green.

**Current Performance (2024-12-15 ~ 2024-12-18):**

| Feature | Signals | Period | Win Rate | Avg Return | Best | Worst |
|---------|---------|--------|----------|------------|------|-------|
| volume_upper_shadow | 32 | 1D | 78.1% | +2.10% | +7.83% (300779.SZ) | -4.18% (300377.SZ) |
| volume_upper_shadow | 32 | 2D | 88.2% | +2.53% | +12.54% (300782.SZ) | -3.30% (300879.SZ) |
| volume_upper_shadow | 32 | 3D | 77.8% | +2.33% | +9.51% (300782.SZ) | -2.41% (300377.SZ) |
| high_retracement | 7 | 1D | 57.1% | +0.38% | +3.23% (300177.SZ) | -2.98% (601992.SH) |
| high_retracement | 7 | 2D | 50.0% | +0.71% | +4.55% (600661.SH) | -2.38% (601992.SH) |
| high_retracement | 7 | 3D | 75.0% | +0.67% | +2.62% (600661.SH) | -2.98% (601992.SH) |
| four_edge | 102 | 1D | 40.2% | -0.53% | +8.21% (000411.SZ) | -9.58% (002235.SZ) |
| four_edge | 102 | 2D | 45.4% | -0.72% | +7.36% (002638.SZ) | -17.23% (002235.SZ) |
| four_edge | 102 | 3D | 41.9% | -1.65% | +7.00% (002638.SZ) | -18.86% (002235.SZ) |
| bullish_cannon | 2 | 1D | 50.0% | -1.40% | +1.50% (002853.SZ) | -4.30% (301234.SZ) |
| bullish_cannon | 2 | 2D | 50.0% | +1.09% | +5.63% (002853.SZ) | -3.45% (301234.SZ) |
| bullish_cannon | 2 | 3D | 0.0% | -6.28% | -6.28% (301234.SZ) | -6.28% (301234.SZ) |

### Feature Engineering

Extract and analyze feature metrics for ML research:

```bash
# Run feature engineering pipeline
make feature-eng FEATURE=volume_upper_shadow

# Full historical scan (all symbols, all dates)
make feature-eng FEATURE=volume_upper_shadow FULL=1 START_DATE=20240101 END_DATE=20241231

# Show statistics
make feature-eng FEATURE=volume_upper_shadow STATS=1

# Export to CSV
make feature-eng FEATURE=volume_upper_shadow EXPORT=output.csv
```

### Feature Analysis

Analyze feature-return relationships with statistical methods:

```bash
# Run feature analysis (correlation, binning, significance tests)
make feature-eng FEATURE=volume_upper_shadow ANALYZE=1
```

Example output:

```
             Correlation Matrix (Pearson)
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ Feature        ┃ 1D Return ┃ 2D Return ┃ 3D Return ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━┩
│ upper_shadow   │   +0.0493 │   +0.0188 │   +0.0086 │
│ vol_ratio      │   +0.0110 │   +0.0224 │   +0.0247 │
│ body_ratio     │   -0.0022 │   -0.0140 │   -0.0397 │
│ gain_2d        │   +0.0334 │   +0.0504 │   +0.0587 │
└────────────────┴───────────┴───────────┴───────────┘

               body_ratio Bin Analysis
┏━━━━━┳━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┓
┃ Bin ┃ Count ┃ Avg 1D ┃ Avg 2D ┃ Avg 3D ┃ WinRate 1D ┃
┡━━━━━╇━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━┩
│ Q1  │  8774 │ +0.51% │ +0.81% │ +1.22% │      54.7% │
│ Q5  │  8518 │ +0.47% │ +0.58% │ +0.53% │      50.5% │
└─────┴───────┴────────┴────────┴────────┴────────────┘
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
- **Caching**: Hybrid approach with Redis hot cache (14-day TTL) + SQLite persistence. Read path: Redis → SQLite → scan. Write path: writes to both.

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

# Performance benchmarks
make benchmark

# Single test file
uv run pytest tests/test_symbol.py -v

# Specific test
uv run pytest tests/test_symbol.py::test_load_sse_symbols -v
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
│   ├── scanner.py       # Scan logic using feature registry
│   ├── cache.py         # Feature result caching (Redis + SQLite)
│   └── db.py            # SQLite persistence for feature results
├── backtest/            # Backtesting module
│   ├── cli.py           # CLI entry point with rich UI
│   └── backtest.py      # Core backtest logic for evaluating signals
├── track/               # Performance tracking module
│   ├── cli.py           # CLI entry point with rich UI
│   └── tracker.py       # Core tracking logic for signal returns
├── feature_engineering/ # Feature analysis for ML research
│   ├── cli.py           # CLI with --stats, --export, --analyze
│   ├── pipeline.py      # Feature extraction pipeline
│   ├── extractor.py     # Extract signal metrics
│   ├── analysis.py      # Correlation, binning, significance tests
│   └── db.py            # SQLite persistence for feature_data
├── common/              # Shared utilities
│   ├── config.py        # Centralized configuration + feature thresholds
│   ├── cli_utils.py     # Shared CLI utilities (progress bars, formatting)
│   ├── returns.py       # Return calculation for backtest/tracking
│   ├── redis.py         # Unified Redis client
│   └── logging.py       # Logging configuration
├── datahub/             # Data acquisition
│   ├── main.py          # Sync entry point
│   ├── symbol.py        # Stock symbol loading
│   ├── daily_bar.py     # Daily bar sync orchestration
│   ├── tushare.py       # TuShare API wrapper
│   ├── db.py            # SQLite operations
│   ├── cache.py         # Sync status caching
│   └── trading_calendar.py
└── feature/             # Signal detection modules
    ├── registry.py      # Central feature registry
    ├── utils.py         # Shared utilities
    ├── bbc.py
    ├── bullish_cannon.py
    ├── volume_upper_shadow.py
    ├── volume_upper_shadow_v2.py  # Optimized version
    ├── volume_stagnation.py
    ├── high_retracement.py
    ├── consolidation_breakout.py
    ├── weak_to_strong.py
    └── four_edge/       # Four-Edge feature package
        ├── __init__.py  # Main four_edge() function
        ├── helpers.py   # Shared helpers and precompute
        ├── edge1.py     # ATR volatility condition
        ├── edge2.py     # Structure patterns
        ├── edge3.py     # Entry signals
        └── edge4.py     # Overheated rejection filter
```
