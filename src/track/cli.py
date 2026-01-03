"""AlphaSpike Feature Tracker CLI - Analyze stored feature signal performance."""

import argparse
import sys
import time

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.common.cli_utils import create_progress_bar, format_duration
from src.track.tracker import (
    AllNegativeAnalysis,
    FeaturePerformance,
    SignalCategory,
    analyze_all_negative_signals,
    get_stored_feature_names,
    track_feature_performance,
)


def _format_date(date: str | None) -> str:
    """Format date string as YYYY-MM-DD."""
    if not date:
        return ""
    return f"{date[:4]}-{date[4:6]}-{date[6:]}"


def _format_date_range(start_date: str | None, end_date: str | None) -> str:
    """Format date range for display."""
    if start_date and end_date:
        return f"{_format_date(start_date)} ~ {_format_date(end_date)}"
    if start_date:
        return f"{_format_date(start_date)} ~"
    if end_date:
        return f"~ {_format_date(end_date)}"
    return "All Dates"


def display_header(console: Console, feature_name: str | None, start_date: str | None, end_date: str | None) -> None:
    """Display the header panel."""
    target = feature_name if feature_name else "All Features"
    date_info = _format_date_range(start_date, end_date)
    header_text = (
        f"[bold cyan]AlphaSpike Feature Tracker[/bold cyan]\n\n"
        f"Tracking: [bold]{target}[/bold]  |  "
        f"Date: [bold]{date_info}[/bold]  |  "
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


def display_analysis_header(
    console: Console, feature_name: str | None, start_date: str | None, end_date: str | None
) -> None:
    """Display the header panel for analysis mode."""
    target = feature_name if feature_name else "All Features"
    date_info = _format_date_range(start_date, end_date)
    header_text = (
        f"[bold yellow]AlphaSpike All-Negative Signal Analysis[/bold yellow]\n\n"
        f"Analyzing: [bold]{target}[/bold]  |  "
        f"Date: [bold]{date_info}[/bold]  |  "
        f"Condition: [bold]1d < 0 AND 2d < 0 AND 3d < 0[/bold]"
    )
    console.print(Panel(header_text, border_style="yellow"))
    console.print()


def display_analysis_summary_table(console: Console, analyses: list[AllNegativeAnalysis]) -> None:
    """Display the summary table for all-negative signal analysis."""
    table = Table(title="All-Negative Signal Summary", border_style="yellow")

    table.add_column("Feature", style="cyan", no_wrap=True)
    table.add_column("Total", justify="right", style="white")
    table.add_column("Negative", justify="right", style="red")
    table.add_column("Ratio", justify="right")
    table.add_column("Avg 1D", justify="right")
    table.add_column("Avg 2D", justify="right")
    table.add_column("Avg 3D", justify="right")

    for analysis in analyses:
        # Color ratio based on severity: >30% red, 15-30% yellow, <15% green
        if analysis.negative_ratio > 30:
            ratio_style = "red"
        elif analysis.negative_ratio > 15:
            ratio_style = "yellow"
        else:
            ratio_style = "green"
        ratio_str = f"[{ratio_style}]{analysis.negative_ratio:.1f}%[/{ratio_style}]"

        table.add_row(
            analysis.feature_name,
            str(analysis.total_signals),
            str(analysis.negative_count),
            ratio_str,
            _format_return(analysis.avg_loss_1d),
            _format_return(analysis.avg_loss_2d),
            _format_return(analysis.avg_loss_3d),
        )

    console.print(table)
    console.print()


def _display_signal_category_table(
    console: Console,
    category: SignalCategory,
    title: str,
    title_style: str,
    border_style: str,
) -> None:
    """Display a table for a signal category."""
    if not category.signals:
        console.print(f"[{title_style}]{title}[/{title_style}] - 0 signals")
        console.print()
        return

    # Category header with stats
    console.print(
        f"[{title_style}]{title}[/{title_style}] - "
        f"{category.count} signals ({category.ratio}%) | "
        f"Avg: {_format_return(category.avg_1d)} / {_format_return(category.avg_2d)} / {_format_return(category.avg_3d)}"
    )

    # Create table
    table = Table(show_header=True, border_style=border_style)
    table.add_column("Stock", style="cyan", no_wrap=True)
    table.add_column("Date", style="white")
    table.add_column("1D", justify="right")
    table.add_column("2D", justify="right")
    table.add_column("3D", justify="right")

    for signal in category.signals:
        date_fmt = f"{signal.signal_date[:4]}-{signal.signal_date[4:6]}-{signal.signal_date[6:]}"
        stock_code = signal.ts_code.split(".")[0]
        table.add_row(
            stock_code,
            date_fmt,
            _format_return(signal.return_1d),
            _format_return(signal.return_2d),
            _format_return(signal.return_3d),
        )

    console.print(table)
    console.print()


def display_analysis_details(console: Console, analyses: list[AllNegativeAnalysis]) -> None:
    """Display detailed list of signals for each feature."""
    for analysis in analyses:
        # Check if we have detailed categories (single feature mode)
        if analysis.all_positive is not None:
            # Display three categories
            console.print(
                f"[bold cyan]{analysis.feature_name}[/bold cyan] - " f"Total: {analysis.total_signals} valid signals"
            )
            console.print()

            # All Positive (green)
            _display_signal_category_table(
                console,
                analysis.all_positive,
                "All Positive (1d>0, 2d>0, 3d>0)",
                "bold green",
                "green",
            )

            # Mixed (yellow)
            _display_signal_category_table(
                console,
                analysis.mixed,
                "Mixed (some positive, some negative)",
                "bold yellow",
                "yellow",
            )

            # All Negative (red)
            _display_signal_category_table(
                console,
                analysis.all_negative_cat,
                "All Negative (1d<0, 2d<0, 3d<0)",
                "bold red",
                "red",
            )
        else:
            # Original mode: only show all-negative signals
            if not analysis.signals:
                continue

            console.print(
                f"[bold yellow]{analysis.feature_name}[/bold yellow] - "
                f"{analysis.negative_count} all-negative signals:"
            )

            table = Table(show_header=True, border_style="dim")
            table.add_column("Stock", style="cyan", no_wrap=True)
            table.add_column("Date", style="white")
            table.add_column("1D", justify="right")
            table.add_column("2D", justify="right")
            table.add_column("3D", justify="right")

            for signal in analysis.signals:
                date_fmt = f"{signal.signal_date[:4]}-{signal.signal_date[4:6]}-{signal.signal_date[6:]}"
                stock_code = signal.ts_code.split(".")[0]
                table.add_row(
                    stock_code,
                    date_fmt,
                    _format_return(signal.return_1d),
                    _format_return(signal.return_2d),
                    _format_return(signal.return_3d),
                )

            console.print(table)
            console.print()


def run_analysis_mode(console: Console, feature_name: str | None, start_date: str | None, end_date: str | None) -> int:
    """Run the all-negative signal analysis mode."""
    display_analysis_header(console, feature_name, start_date, end_date)

    start_time = time.time()

    console.print("[cyan]Analyzing signals...[/cyan]")
    with create_progress_bar(console) as progress:
        task_id = progress.add_task(
            "[cyan]Processing signals[/cyan]",
            total=None,
        )

        first_update = True

        def progress_callback(current: int, total: int):
            nonlocal first_update
            if first_update:
                progress.update(task_id, total=total)
                first_update = False
            progress.update(task_id, completed=current)

        analyses = analyze_all_negative_signals(
            feature_name=feature_name,
            start_date=start_date,
            end_date=end_date,
            progress_callback=progress_callback,
        )

    console.print()

    if not analyses:
        console.print("[yellow]No signals found for analysis.[/yellow]")
        return 0

    # Display summary table
    display_analysis_summary_table(console, analyses)

    # Display detailed signals
    display_analysis_details(console, analyses)

    # Summary
    elapsed = time.time() - start_time
    total_negative = sum(a.negative_count for a in analyses)
    total_signals = sum(a.total_signals for a in analyses)

    console.print(
        f"[bold yellow]Analysis completed[/bold yellow] in {format_duration(elapsed)} | "
        f"All-negative signals: {total_negative} / {total_signals}"
    )

    return 0


def run_track_mode(console: Console, feature_name: str | None, start_date: str | None, end_date: str | None) -> int:
    """Run the standard tracking mode."""
    display_header(console, feature_name, start_date, end_date)

    start_time = time.time()

    console.print("[cyan]Calculating signal returns...[/cyan]")
    with create_progress_bar(console) as progress:
        task_id = progress.add_task(
            "[cyan]Processing signals[/cyan]",
            total=None,
        )

        first_update = True

        def progress_callback(current: int, total: int):
            nonlocal first_update
            if first_update:
                progress.update(task_id, total=total)
                first_update = False
            progress.update(task_id, completed=current)

        performances = track_feature_performance(
            feature_name=feature_name,
            start_date=start_date,
            end_date=end_date,
            progress_callback=progress_callback,
        )

    console.print()

    if not performances:
        console.print("[yellow]No valid signals found for analysis.[/yellow]")
        return 0

    display_performance_table(console, performances)

    elapsed = time.time() - start_time
    total_signals = sum(p.total_signals for p in performances)
    console.print(
        f"[bold green]Tracking completed[/bold green] in {format_duration(elapsed)} | "
        f"Total signals: {total_signals}"
    )

    return 0


def _is_valid_date(date_str: str | None) -> bool:
    """Check if date string is valid YYYYMMDD format."""
    return date_str is None or (len(date_str) == 8 and date_str.isdigit())


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
    parser.add_argument(
        "--start-date",
        help="Start date in YYYYMMDD format (inclusive, optional)",
    )
    parser.add_argument(
        "--end-date",
        help="End date in YYYYMMDD format (inclusive, optional)",
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Analyze all-negative signals (1d, 2d, 3d all negative)",
    )
    args = parser.parse_args()

    console = Console()

    # Validate inputs and collect errors
    start_date = args.start_date
    end_date = args.end_date
    error_msg = None

    if not _is_valid_date(start_date):
        error_msg = "--start-date must be in YYYYMMDD format"
    elif not _is_valid_date(end_date):
        error_msg = "--end-date must be in YYYYMMDD format"
    elif start_date and end_date and start_date > end_date:
        error_msg = "--start-date must be before or equal to --end-date"
    elif not stored_features:
        error_msg = "No stored feature results found. Run 'make scan END_DATE=YYYYMMDD' first."
    elif args.feature and args.feature not in stored_features:
        error_msg = f"No stored results for feature '{args.feature}'. Available: {', '.join(stored_features)}"

    if error_msg:
        console.print(f"[red]Error: {error_msg}[/red]")
        return 1

    # Branch based on --analyze flag
    if args.analyze:
        return run_analysis_mode(console, args.feature, start_date, end_date)
    return run_track_mode(console, args.feature, start_date, end_date)


if __name__ == "__main__":
    sys.exit(main())
