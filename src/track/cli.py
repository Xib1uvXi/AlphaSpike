"""AlphaSpike Feature Tracker CLI - Analyze stored feature signal performance."""

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

from src.track.tracker import (
    FeaturePerformance,
    get_stored_feature_names,
    track_feature_performance,
)


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


def display_header(console: Console, feature_name: str | None) -> None:
    """Display the header panel."""
    target = feature_name if feature_name else "All Features"
    header_text = (
        f"[bold cyan]AlphaSpike Feature Tracker[/bold cyan]\n\n"
        f"Tracking: [bold]{target}[/bold]  |  "
        f"Periods: [bold]1d, 2d, 3d[/bold]"
    )
    console.print(Panel(header_text, border_style="cyan"))
    console.print()


def _format_win_rate(rate: float) -> str:
    """Format win rate with color."""
    style = "green" if rate >= 50 else "red"
    return f"[{style}]{rate:.1f}%[/{style}]"


def _format_return(ret: float) -> str:
    """Format return with color and sign."""
    style = "green" if ret > 0 else "red"
    sign = "+" if ret > 0 else ""
    return f"[{style}]{sign}{ret:.2f}%[/{style}]"


def _format_return_with_stock(ret: float, stock: str, date: str) -> str:
    """Format return with color, stock code and date."""
    style = "green" if ret > 0 else "red"
    sign = "+" if ret > 0 else ""
    if not stock:
        return f"[{style}]{sign}{ret:.2f}%[/{style}]"
    # Format date as MM-DD
    date_fmt = f"{date[4:6]}-{date[6:8]}" if len(date) == 8 else date
    return f"[{style}]{sign}{ret:.2f}%[/{style}] [dim]({stock} {date_fmt})[/dim]"


def display_performance_table(console: Console, performances: list[FeaturePerformance]) -> None:
    """Display the performance statistics tables for each period."""
    periods = [
        ("1D", lambda p: p.stats_1d),
        ("2D", lambda p: p.stats_2d),
        ("3D", lambda p: p.stats_3d),
    ]

    for period_name, get_stats in periods:
        table = Table(title=f"{period_name} Performance", border_style="cyan")

        table.add_column("Feature", style="cyan", no_wrap=True)
        table.add_column("Signals", justify="right", style="white")
        table.add_column("Win Rate", justify="right")
        table.add_column("Avg Return", justify="right")
        table.add_column("Best", justify="right")
        table.add_column("Worst", justify="right")

        for perf in performances:
            stats = get_stats(perf)
            table.add_row(
                perf.feature_name,
                str(perf.total_signals),
                _format_win_rate(stats.win_rate),
                _format_return(stats.avg_return),
                _format_return_with_stock(stats.max_return, stats.max_stock, stats.max_date),
                _format_return_with_stock(stats.min_return, stats.min_stock, stats.min_date),
            )

        console.print(table)
        console.print()


def main():
    """Main entry point for CLI."""
    stored_features = get_stored_feature_names()

    parser = argparse.ArgumentParser(
        description="AlphaSpike Feature Tracker - Analyze stored feature signal performance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Stored features: {', '.join(stored_features) if stored_features else 'None'}",
    )
    parser.add_argument(
        "--feature",
        help="Feature name to track (optional, tracks all if not specified)",
    )
    args = parser.parse_args()

    console = Console()

    # Check if any stored results exist
    if not stored_features:
        console.print("[red]Error: No stored feature results found.[/red]")
        console.print("Run 'make scan END_DATE=YYYYMMDD' first to generate feature results.")
        return 1

    # Validate feature name if specified
    if args.feature and args.feature not in stored_features:
        console.print(f"[red]Error: No stored results for feature '{args.feature}'[/red]")
        console.print(f"Available features: {', '.join(stored_features)}")
        return 1

    # Display header
    display_header(console, args.feature)

    start_time = time.time()

    # Run tracking with progress
    console.print("[cyan]Calculating signal returns...[/cyan]")
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
            "[cyan]Processing signals[/cyan]",
            total=None,  # Will be set once we know total
        )

        first_update = True

        def progress_callback(current: int, total: int):
            nonlocal first_update
            if first_update:
                progress.update(task_id, total=total)
                first_update = False
            progress.update(task_id, completed=current)

        performances = track_feature_performance(
            feature_name=args.feature,
            progress_callback=progress_callback,
        )

    console.print()

    # Check if we got any results
    if not performances:
        console.print("[yellow]No valid signals found for analysis.[/yellow]")
        return 0

    # Display results
    display_performance_table(console, performances)

    # Summary
    elapsed = time.time() - start_time
    total_signals = sum(p.total_signals for p in performances)
    console.print(
        f"[bold green]Tracking completed[/bold green] in {format_duration(elapsed)} | "
        f"Total signals: {total_signals}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
