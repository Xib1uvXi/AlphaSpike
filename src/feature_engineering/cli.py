"""CLI for feature engineering pipeline."""

import argparse

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)
from rich.table import Table

from src.feature_engineering.analysis import (
    analyze_feature_returns,
    export_analysis_to_csv,
    print_analysis_report,
)
from src.feature_engineering.db import get_feature_data_by_feature, init_feature_data_db
from src.feature_engineering.pipeline import (
    get_feature_engineering_stats,
    run_feature_engineering,
    run_feature_engineering_full,
)


def main():  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """Run feature engineering CLI."""
    parser = argparse.ArgumentParser(description="Feature engineering pipeline")
    parser.add_argument(
        "--feature",
        "-f",
        default="volume_upper_shadow",
        help="Feature name to process (default: volume_upper_shadow)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full scan on ALL symbols and ALL trading days",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date filter (YYYYMMDD) for full scan",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date filter (YYYYMMDD) for full scan",
    )
    parser.add_argument(
        "--stats",
        "-s",
        action="store_true",
        help="Show statistics instead of running pipeline",
    )
    parser.add_argument(
        "--export",
        "-e",
        type=str,
        help="Export feature data to CSV file",
    )
    parser.add_argument(
        "--analyze",
        "-a",
        action="store_true",
        help="Run feature analysis (correlation, binning, significance tests)",
    )

    args = parser.parse_args()
    console = Console()

    if args.stats:
        # Show statistics
        stats = get_feature_engineering_stats(args.feature)
        console.print(f"\n[bold]Feature Engineering Stats: {args.feature}[/bold]")
        console.print(f"  Total records: {stats['total_records']}")
        console.print(f"  Records with returns: {stats['records_with_returns']}")
        if stats["date_range"]:
            console.print(f"  Date range: {stats['date_range'][0]} - {stats['date_range'][1]}")
        return

    if args.export:
        # Export to CSV
        console.print(f"\n[bold]Exporting {args.feature} data to {args.export}...[/bold]")
        data = get_feature_data_by_feature(args.feature)
        if not data:
            console.print("[red]No data found[/red]")
            return

        import csv

        with open(args.export, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            # Header
            writer.writerow(
                [
                    "ts_code",
                    "signal_date",
                    "feature_name",
                    "upper_shadow",
                    "vol_ratio",
                    "price_quantile",
                    "pct_chg",
                    "body_ratio",
                    "close_vs_ma20",
                    "prev_vol_ratio",
                    "gain_2d",
                    "return_1d",
                    "return_2d",
                    "return_3d",
                ]
            )
            # Data
            for d in data:
                writer.writerow(
                    [
                        d.ts_code,
                        d.signal_date,
                        d.feature_name,
                        d.upper_shadow,
                        d.vol_ratio,
                        d.price_quantile,
                        d.pct_chg,
                        d.body_ratio,
                        d.close_vs_ma20,
                        d.prev_vol_ratio,
                        d.gain_2d,
                        d.return_1d,
                        d.return_2d,
                        d.return_3d,
                    ]
                )
        console.print(f"[green]Exported {len(data)} records to {args.export}[/green]")
        return

    if args.analyze:
        # Run feature analysis
        console.print(f"\n[bold]Analyzing feature: {args.feature}[/bold]\n")
        report = analyze_feature_returns(args.feature)

        if report.valid_records == 0:
            console.print("[red]No valid data found for analysis[/red]")
            return

        print_analysis_report(report, console)

        # Export if specified
        if args.export:
            export_analysis_to_csv(report, args.export)
            console.print(f"\n[green]Analysis exported to {args.export}[/green]")
        return

    # Run pipeline
    if args.full:
        console.print(f"\n[bold]Running FULL Feature Engineering Pipeline: {args.feature}[/bold]")
        console.print("Scanning ALL symbols for ALL trading days...")
        if args.start_date:
            console.print(f"  Start date: {args.start_date}")
        if args.end_date:
            console.print(f"  End date: {args.end_date}")
        console.print()
    else:
        console.print(f"\n[bold]Running Feature Engineering Pipeline: {args.feature}[/bold]\n")

    # Initialize database
    init_feature_data_db()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Processing...", total=None)

        def update_progress(current: int, total: int, ts_code: str):
            progress.update(task, total=total, completed=current, description=f"Processing {ts_code}...")

        if args.full:
            count = run_feature_engineering_full(
                start_date=args.start_date,
                end_date=args.end_date,
                progress_callback=update_progress,
            )
        else:
            count = run_feature_engineering(args.feature, progress_callback=update_progress)

    console.print(f"\n[green]Saved {count} feature data records[/green]")

    # Show sample data
    data = get_feature_data_by_feature(args.feature)
    if data:
        table = Table(title="Sample Data (last 10 records)")
        table.add_column("Stock", style="cyan")
        table.add_column("Date", style="green")
        table.add_column("UpperShadow", justify="right")
        table.add_column("VolRatio", justify="right")
        table.add_column("PriceQ", justify="right")
        table.add_column("PctChg", justify="right")
        table.add_column("BodyR", justify="right")
        table.add_column("1D", justify="right")
        table.add_column("2D", justify="right")
        table.add_column("3D", justify="right")

        for d in data[-10:]:
            r1d = f"{d.return_1d:.2f}%" if d.return_1d else "-"
            r2d = f"{d.return_2d:.2f}%" if d.return_2d else "-"
            r3d = f"{d.return_3d:.2f}%" if d.return_3d else "-"
            table.add_row(
                d.ts_code,
                d.signal_date,
                f"{d.upper_shadow:.2f}%",
                f"{d.vol_ratio:.2f}",
                f"{d.price_quantile:.2f}",
                f"{d.pct_chg:.2f}%",
                f"{d.body_ratio:.2f}",
                r1d,
                r2d,
                r3d,
            )

        console.print(table)


if __name__ == "__main__":
    main()
