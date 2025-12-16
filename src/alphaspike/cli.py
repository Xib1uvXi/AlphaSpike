"""AlphaSpike CLI - Feature Scanner."""

import argparse
import sys
import time

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

load_dotenv()

from src.alphaspike.cache import get_redis_client
from src.alphaspike.scanner import FEATURES, ScanResult, scan_feature
from src.datahub.daily_bar import batch_load_daily_bars
from src.datahub.symbol import get_ts_codes


def format_duration(seconds: float) -> str:
    """Format duration in human readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


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

    console.print(f"[bold cyan]{result.feature_name}[/bold cyan] - {len(result.signals)} signals:")

    # Show all signals for four_edge, truncate others
    if result.feature_name == "four_edge":
        # Display all signals in rows of 10
        for i in range(0, len(result.signals), 10):
            row = result.signals[i : i + 10]
            console.print(f"  {', '.join(row)}")
    else:
        # Truncate to 20 for other features
        signals_str = ", ".join(result.signals[:20])
        if len(result.signals) > 20:
            signals_str += f", ... (+{len(result.signals) - 20} more)"
        console.print(f"  {signals_str}")

    console.print()


def main():  # pylint: disable=too-many-locals
    """Main entry point for CLI."""
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
    args = parser.parse_args()

    # Validate date format
    if len(args.end_date) != 8 or not args.end_date.isdigit():
        print("Error: --end-date must be in YYYYMMDD format")
        return 1

    console = Console()
    use_cache = not args.no_cache
    max_workers = args.workers

    # Initialize
    redis_client = get_redis_client()
    ts_codes = get_ts_codes()
    total_symbols = len(ts_codes)

    # Display header
    display_header(console, args.end_date, total_symbols, redis_client is not None)

    results: list[ScanResult] = []
    start_time = time.time()

    # Phase 1: Pre-load all market data
    console.print("[cyan]Loading market data...[/cyan]")
    load_start = time.time()
    data_cache = batch_load_daily_bars(ts_codes, end_date=args.end_date)
    load_elapsed = time.time() - load_start
    console.print(
        f"[green]Loaded {len(data_cache):,} symbols[/green] in {format_duration(load_elapsed)} "
        f"(using {max_workers} workers)"
    )
    console.print()

    # Phase 2: Scan each feature with progress
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        for feature in FEATURES:
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
                end_date=args.end_date,
                ts_codes=ts_codes,
                use_cache=use_cache,
                redis_client=redis_client,
                progress_callback=make_progress_callback(task_id),
                data_cache=data_cache,
                max_workers=max_workers,
            )

            # If from cache, complete immediately
            if result.from_cache:
                progress.update(
                    task_id, completed=len(data_cache), description=f"[yellow]{feature.name}[/yellow] (cached)"
                )

            results.append(result)

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
