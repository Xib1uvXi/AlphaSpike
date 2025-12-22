"""AlphaSpike CLI - Feature Scanner."""

import argparse
import sys
import time
from dataclasses import dataclass

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

load_dotenv()

from src.alphaspike.cache import get_redis_client
from src.alphaspike.scanner import FEATURES, ScanResult, scan_feature
from src.common.cli_utils import create_progress_bar, format_duration
from src.datahub.daily_bar import batch_load_daily_bars
from src.datahub.symbol import get_ts_codes
from src.feature.registry import (
    FeatureConfig,
    get_all_feature_names,
    get_feature_by_name,
)


def display_header(console: Console, end_date: str, total_symbols: int, redis_available: bool) -> None:
    """Display the header panel."""
    redis_status = "[green]Connected[/green]" if redis_available else "[yellow]Unavailable[/yellow]"
    header_text = (
        f"[bold cyan]AlphaSpike Feature Scanner[/bold cyan]\n\n"
        f"End Date: [bold]{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}[/bold]  |  "
        f"Symbols: [bold]{total_symbols:,}[/bold]  |  "
        f"Redis: {redis_status}"
    )
    console.print(Panel(header_text, border_style="cyan"))
    console.print()


def display_results_table(console: Console, results: list[ScanResult]) -> None:
    """Display the results summary table."""
    table = Table(title="Scan Results", border_style="cyan")
    table.add_column("Feature", style="cyan", no_wrap=True)
    table.add_column("Signals", justify="right", style="green")
    table.add_column("Status", style="white")

    for result in results:
        if result.from_cache:
            status = "[yellow]Cached[/yellow]"
        else:
            status = f"[green]Scanned[/green] ({result.scanned} ok, {result.skipped} skip, {result.errors} err)"

        table.add_row(
            result.feature_name,
            str(len(result.signals)),
            status,
        )

    console.print(table)
    console.print()


def display_feature_signals(console: Console, result: ScanResult) -> None:
    """Display signals for a single feature."""
    if not result.signals:
        return

    # Strip .SZ/.SH suffix from ts_codes for display
    display_signals = [code.split(".")[0] for code in result.signals]

    console.print(f"[bold cyan]{result.feature_name}[/bold cyan] - {len(display_signals)} signals:")

    # Show all signals for four_edge, truncate others
    if result.feature_name == "four_edge":
        # Display all signals in rows of 10
        for i in range(0, len(display_signals), 10):
            row = display_signals[i : i + 10]
            console.print(f"  {', '.join(row)}")
    else:
        # Truncate to 20 for other features
        signals_str = ", ".join(display_signals[:20])
        if len(display_signals) > 20:
            signals_str += f", ... (+{len(display_signals) - 20} more)"
        console.print(f"  {signals_str}")

    console.print()


@dataclass(frozen=True)
class ScanContext:
    """Shared inputs for scanning features."""

    end_date: str
    ts_codes: list[str]
    use_cache: bool
    redis_client: object | None
    max_workers: int


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="AlphaSpike Feature Scanner - Scan all symbols for trading signals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date for scanning (YYYYMMDD format)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Ignore cache and force rescan all features",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help="Number of parallel workers (default: 6)",
    )
    parser.add_argument(
        "--feature",
        default=None,
        help="Comma-separated feature names to scan (e.g., 'bbc,four_edge')",
    )
    return parser.parse_args()


def resolve_features(console: Console, feature_arg: str | None) -> list[FeatureConfig]:
    """Resolve feature configs to scan from CLI input."""
    if feature_arg is None:
        return FEATURES

    feature_names = [name.strip() for name in feature_arg.split(",")]
    feature_names = [name for name in feature_names if name]
    valid_feature_names = set(get_all_feature_names())
    features_to_scan: list[FeatureConfig] = []

    for name in feature_names:
        if name not in valid_feature_names:
            console.print(f"[yellow]Warning:[/yellow] Unknown feature '{name}', skipping.")
            continue

        feature_config = get_feature_by_name(name)
        if feature_config is None:
            console.print(f"[yellow]Warning:[/yellow] Unknown feature '{name}', skipping.")
            continue

        features_to_scan.append(feature_config)

    return features_to_scan


def load_market_data(
    console: Console,
    ts_codes: list[str],
    end_date: str,
    max_workers: int,
) -> dict[str, object]:
    """Load all market data into cache."""
    console.print("[cyan]Loading market data...[/cyan]")
    load_start = time.time()
    data_cache = batch_load_daily_bars(ts_codes, end_date=end_date)
    load_elapsed = time.time() - load_start
    console.print(
        f"[green]Loaded {len(data_cache):,} symbols[/green] in {format_duration(load_elapsed)} "
        f"(using {max_workers} workers)"
    )
    console.print()
    return data_cache


def scan_features(
    console: Console,
    features_to_scan: list[FeatureConfig],
    data_cache: dict[str, object],
    context: ScanContext,
) -> list[ScanResult]:
    """Scan features with progress updates."""
    results: list[ScanResult] = []

    with create_progress_bar(console) as progress:
        for feature in features_to_scan:
            task_id = progress.add_task(
                f"[cyan]{feature.name}[/cyan]",
                total=len(data_cache),
            )

            def make_progress_callback(tid):
                def callback(current: int, _total: int):
                    progress.update(tid, completed=current)

                return callback

            result = scan_feature(
                feature=feature,
                end_date=context.end_date,
                ts_codes=context.ts_codes,
                use_cache=context.use_cache,
                redis_client=context.redis_client,
                progress_callback=make_progress_callback(task_id),
                data_cache=data_cache,
                max_workers=context.max_workers,
            )

            # If from cache, complete immediately
            if result.from_cache:
                progress.update(
                    task_id, completed=len(data_cache), description=f"[yellow]{feature.name}[/yellow] (cached)"
                )

            results.append(result)

    return results


def main() -> int:  # pylint: disable=too-many-locals
    """Main entry point for CLI."""
    args = parse_args()

    # Validate date format
    if len(args.end_date) != 8 or not args.end_date.isdigit():
        print("Error: --end-date must be in YYYYMMDD format")
        return 1

    console = Console()
    use_cache = not args.no_cache
    max_workers = args.workers

    features_to_scan = resolve_features(console, args.feature)
    if not features_to_scan:
        console.print("[red]Error:[/red] No valid features provided.")
        return 1

    # Initialize
    redis_client = get_redis_client()
    ts_codes = get_ts_codes()
    total_symbols = len(ts_codes)

    # Display header
    display_header(console, args.end_date, total_symbols, redis_client is not None)

    start_time = time.time()

    # Phase 1: Pre-load all market data
    data_cache = load_market_data(console, ts_codes, args.end_date, max_workers)

    # Phase 2: Scan each feature with progress
    context = ScanContext(
        end_date=args.end_date,
        ts_codes=ts_codes,
        use_cache=use_cache,
        redis_client=redis_client,
        max_workers=max_workers,
    )
    results = scan_features(console, features_to_scan, data_cache, context)

    console.print()

    # Display results
    display_results_table(console, results)

    # Display individual feature signals
    for result in results:
        display_feature_signals(console, result)

    # Summary
    elapsed = time.time() - start_time
    total_signals = sum(len(r.signals) for r in results)
    cached_count = sum(1 for r in results if r.from_cache)
    scanned_count = len(results) - cached_count

    console.print(
        f"[bold green]Scan completed[/bold green] in {format_duration(elapsed)} | "
        f"Total signals: {total_signals} | "
        f"Features: {scanned_count} scanned, {cached_count} cached"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
