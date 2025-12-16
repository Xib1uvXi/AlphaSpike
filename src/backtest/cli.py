"""AlphaSpike Backtest CLI - Feature Backtest Runner."""

import argparse
import sys
import time

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

from src.alphaspike.scanner import FEATURES
from src.backtest.backtest import YearlyBacktestStats, backtest_year
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


def display_header(console: Console, feature_name: str, year: int, holding_days: int) -> None:
    """Display the header panel."""
    header_text = (
        f"[bold cyan]AlphaSpike Backtest CLI[/bold cyan]\n\n"
        f"Feature: [bold]{feature_name}[/bold]  |  "
        f"Year: [bold]{year}[/bold]  |  "
        f"Holding Days: [bold]{holding_days}[/bold]"
    )
    console.print(Panel(header_text, border_style="cyan"))
    console.print()


def display_stats_table(console: Console, stats: YearlyBacktestStats) -> None:
    """Display the backtest statistics table."""
    table = Table(title="Backtest Results", border_style="cyan")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", justify="right", style="white")

    # Format values with colors
    win_rate_style = "green" if stats.win_rate >= 50 else "red"
    max_win_rate_style = "green" if stats.max_win_rate >= 50 else "red"
    avg_return_style = "green" if stats.avg_return > 0 else "red"
    max_return_style = "green" if stats.max_return > 0 else "red"
    min_return_style = "green" if stats.min_return > 0 else "red"
    total_return_style = "green" if stats.total_return_sum > 0 else "red"
    win_return_style = "green" if stats.win_return_sum > 0 else "red"
    loss_return_style = "green" if stats.loss_return_sum > 0 else "red"
    max_return_sum_style = "green" if stats.max_return_sum > 0 else "red"

    table.add_row("Total Signals", f"[bold]{stats.total_signals}[/bold]")
    table.add_row("Win Count", f"[green]{stats.win_count}[/green]")
    table.add_row("Loss Count", f"[red]{stats.loss_count}[/red]")
    table.add_row("Win Rate", f"[{win_rate_style}]{stats.win_rate:.2f}%[/{win_rate_style}]")
    table.add_row("Max Win Count", f"[green]{stats.max_win_count}[/green]")
    table.add_row("Max Win Rate", f"[{max_win_rate_style}]{stats.max_win_rate:.2f}%[/{max_win_rate_style}]")
    table.add_row("Cumulative Return", f"[{total_return_style}]{stats.total_return_sum:.2f}%[/{total_return_style}]")
    table.add_row("Win Return Sum", f"[{win_return_style}]{stats.win_return_sum:.2f}%[/{win_return_style}]")
    table.add_row("Loss Return Sum", f"[{loss_return_style}]{stats.loss_return_sum:.2f}%[/{loss_return_style}]")
    table.add_row("Max Return Sum", f"[{max_return_sum_style}]{stats.max_return_sum:.2f}%[/{max_return_sum_style}]")
    table.add_row("Average Return", f"[{avg_return_style}]{stats.avg_return:.2f}%[/{avg_return_style}]")
    table.add_row("Max Return", f"[{max_return_style}]{stats.max_return:.2f}%[/{max_return_style}]")
    table.add_row("Min Return", f"[{min_return_style}]{stats.min_return:.2f}%[/{min_return_style}]")
    table.add_row("Trading Days", f"{stats.trading_days_count}")

    console.print(table)
    console.print()


def validate_feature_name(feature_name: str) -> bool:
    """Check if feature name is valid."""
    return any(f.name == feature_name for f in FEATURES)


def get_available_features() -> list[str]:
    """Get list of available feature names."""
    return [f.name for f in FEATURES]


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="AlphaSpike Backtest CLI - Run yearly backtest for a feature",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available features: {', '.join(get_available_features())}",
    )
    parser.add_argument(
        "--year",
        required=True,
        type=int,
        help="Year to backtest (e.g., 2025)",
    )
    parser.add_argument(
        "--feature",
        required=True,
        help="Feature name to backtest (e.g., bullish_cannon)",
    )
    parser.add_argument(
        "--holding-days",
        type=int,
        default=5,
        help="Number of holding days (default: 5)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help="Number of parallel workers (default: 6)",
    )
    args = parser.parse_args()

    console = Console()

    # Validate feature name
    if not validate_feature_name(args.feature):
        console.print(f"[red]Error: Unknown feature '{args.feature}'[/red]")
        console.print(f"Available features: {', '.join(get_available_features())}")
        return 1

    # Validate year
    if args.year < 2000 or args.year > 2100:
        console.print(f"[red]Error: Invalid year {args.year}. Must be between 2000 and 2100.[/red]")
        return 1

    # Get stock count for progress bar
    ts_codes = get_ts_codes()
    total_stocks = len(ts_codes)

    # Display header
    display_header(console, args.feature, args.year, args.holding_days)

    start_time = time.time()

    # Run backtest with progress
    console.print(f"[cyan]Running backtest ({total_stocks} stocks)...[/cyan]")
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        refresh_per_second=10,
    ) as progress:
        task_id = progress.add_task(
            "[cyan]Processing stocks[/cyan]",
            total=total_stocks,
        )

        def progress_callback(current: int, _total: int):
            progress.update(task_id, completed=current)

        stats, _ = backtest_year(
            feature_name=args.feature,
            year=args.year,
            holding_days=args.holding_days,
            progress_callback=progress_callback,
            max_workers=args.workers,
        )

    console.print()

    # Display results
    display_stats_table(console, stats)

    # Summary
    elapsed = time.time() - start_time
    console.print(f"[bold green]Backtest completed[/bold green] in {format_duration(elapsed)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
