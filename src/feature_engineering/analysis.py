"""Feature analysis module for analyzing feature-return relationships."""

from dataclasses import dataclass

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from scipy import stats

from src.datahub.db import get_connection

# Feature columns to analyze
FEATURE_COLUMNS = [
    "upper_shadow",
    "vol_ratio",
    "price_quantile",
    "pct_chg",
    "body_ratio",
    "close_vs_ma20",
    "prev_vol_ratio",
    "gain_2d",
]

RETURN_COLUMNS = ["return_1d", "return_2d", "return_3d"]


@dataclass
class CorrelationResult:
    """Correlation between a feature and returns."""

    feature: str
    corr_1d: float
    corr_2d: float
    corr_3d: float


@dataclass
class BinStats:
    """Statistics for a single bin of a feature."""

    bin_label: str
    count: int
    avg_1d: float
    avg_2d: float
    avg_3d: float
    win_rate_1d: float
    win_rate_2d: float
    win_rate_3d: float


@dataclass
class SignificanceTest:
    """T-test result comparing Q5 vs Q1."""

    feature: str
    t_stat_1d: float
    p_value_1d: float
    t_stat_2d: float
    p_value_2d: float
    t_stat_3d: float
    p_value_3d: float


@dataclass
class AnalysisReport:
    """Complete analysis report for a feature."""

    feature_name: str
    total_records: int
    valid_records: int
    correlations: list[CorrelationResult]
    bin_stats: dict[str, list[BinStats]]
    significance_tests: list[SignificanceTest]


def load_feature_data(feature_name: str) -> pd.DataFrame:
    """Load feature data from SQLite into a DataFrame."""
    query = """
        SELECT * FROM feature_data
        WHERE feature_name = ?
        AND return_1d IS NOT NULL
        AND return_2d IS NOT NULL
        AND return_3d IS NOT NULL
    """
    with get_connection() as conn:
        df = pd.read_sql_query(query, conn, params=(feature_name,))
    return df


def calculate_correlations(df: pd.DataFrame) -> list[CorrelationResult]:
    """Calculate Pearson correlations between features and returns."""
    results = []
    for feature in FEATURE_COLUMNS:
        if feature not in df.columns:
            continue
        valid_df = df[[feature, "return_1d", "return_2d", "return_3d"]].dropna()
        if len(valid_df) < 10:
            continue

        corr_1d = valid_df[feature].corr(valid_df["return_1d"])
        corr_2d = valid_df[feature].corr(valid_df["return_2d"])
        corr_3d = valid_df[feature].corr(valid_df["return_3d"])

        results.append(
            CorrelationResult(
                feature=feature,
                corr_1d=corr_1d if pd.notna(corr_1d) else 0.0,
                corr_2d=corr_2d if pd.notna(corr_2d) else 0.0,
                corr_3d=corr_3d if pd.notna(corr_3d) else 0.0,
            )
        )
    return results


def calculate_bin_stats(df: pd.DataFrame, feature: str, n_bins: int = 5) -> list[BinStats]:
    """Calculate statistics for each quantile bin of a feature."""
    if feature not in df.columns:
        return []

    valid_df = df[[feature, "return_1d", "return_2d", "return_3d"]].dropna()
    if len(valid_df) < n_bins * 10:
        return []

    # Create quantile bins
    try:
        valid_df["bin"] = pd.qcut(
            valid_df[feature], q=n_bins, labels=[f"Q{i+1}" for i in range(n_bins)], duplicates="drop"
        )
    except ValueError:
        # Not enough unique values for binning
        return []

    results = []
    for bin_label in valid_df["bin"].unique():
        if pd.isna(bin_label):
            continue
        bin_df = valid_df[valid_df["bin"] == bin_label]
        if len(bin_df) == 0:
            continue

        results.append(
            BinStats(
                bin_label=str(bin_label),
                count=len(bin_df),
                avg_1d=bin_df["return_1d"].mean(),
                avg_2d=bin_df["return_2d"].mean(),
                avg_3d=bin_df["return_3d"].mean(),
                win_rate_1d=(bin_df["return_1d"] > 0).mean() * 100,
                win_rate_2d=(bin_df["return_2d"] > 0).mean() * 100,
                win_rate_3d=(bin_df["return_3d"] > 0).mean() * 100,
            )
        )

    # Sort by bin label
    results.sort(key=lambda x: x.bin_label)
    return results


def test_significance(df: pd.DataFrame, feature: str, n_bins: int = 5) -> SignificanceTest | None:
    """Perform t-test comparing Q5 (highest) vs Q1 (lowest) bins."""
    if feature not in df.columns:
        return None

    valid_df = df[[feature, "return_1d", "return_2d", "return_3d"]].dropna()
    if len(valid_df) < n_bins * 10:
        return None

    try:
        valid_df["bin"] = pd.qcut(
            valid_df[feature], q=n_bins, labels=[f"Q{i+1}" for i in range(n_bins)], duplicates="drop"
        )
    except ValueError:
        return None

    q1_df = valid_df[valid_df["bin"] == "Q1"]
    q5_df = valid_df[valid_df["bin"] == f"Q{n_bins}"]

    if len(q1_df) < 5 or len(q5_df) < 5:
        return None

    # T-tests for each return period
    t_1d, p_1d = stats.ttest_ind(q5_df["return_1d"], q1_df["return_1d"])
    t_2d, p_2d = stats.ttest_ind(q5_df["return_2d"], q1_df["return_2d"])
    t_3d, p_3d = stats.ttest_ind(q5_df["return_3d"], q1_df["return_3d"])

    return SignificanceTest(
        feature=feature,
        t_stat_1d=t_1d,
        p_value_1d=p_1d,
        t_stat_2d=t_2d,
        p_value_2d=p_2d,
        t_stat_3d=t_3d,
        p_value_3d=p_3d,
    )


def analyze_feature_returns(feature_name: str) -> AnalysisReport:
    """Run complete analysis on a feature's data."""
    df = load_feature_data(feature_name)
    total_records = len(df)

    # Filter to valid records
    valid_df = df.dropna(subset=RETURN_COLUMNS)
    valid_records = len(valid_df)

    # Run analyses
    correlations = calculate_correlations(valid_df)
    bin_stats = {feature: calculate_bin_stats(valid_df, feature) for feature in FEATURE_COLUMNS}
    significance_tests = [test_significance(valid_df, feature) for feature in FEATURE_COLUMNS]
    significance_tests = [t for t in significance_tests if t is not None]

    return AnalysisReport(
        feature_name=feature_name,
        total_records=total_records,
        valid_records=valid_records,
        correlations=correlations,
        bin_stats=bin_stats,
        significance_tests=significance_tests,
    )


def print_analysis_report(report: AnalysisReport, console: Console) -> None:
    """Print analysis report to console using Rich tables."""
    # Header panel
    console.print(
        Panel(
            f"[bold]Feature:[/bold] {report.feature_name}\n"
            f"[bold]Total Records:[/bold] {report.total_records:,}\n"
            f"[bold]Valid Records:[/bold] {report.valid_records:,}",
            title="Feature Analysis Report",
            border_style="blue",
        )
    )
    console.print()

    # 1. Correlation Matrix
    corr_table = Table(title="Correlation Matrix (Pearson)", show_header=True, header_style="bold cyan")
    corr_table.add_column("Feature", style="cyan")
    corr_table.add_column("1D Return", justify="right")
    corr_table.add_column("2D Return", justify="right")
    corr_table.add_column("3D Return", justify="right")

    for corr in report.correlations:
        corr_table.add_row(
            corr.feature,
            _format_correlation(corr.corr_1d),
            _format_correlation(corr.corr_2d),
            _format_correlation(corr.corr_3d),
        )
    console.print(corr_table)
    console.print()

    # 2. Bin Analysis for each feature
    for feature, bins in report.bin_stats.items():
        if not bins:
            continue

        bin_table = Table(title=f"{feature} Bin Analysis", show_header=True, header_style="bold green")
        bin_table.add_column("Bin", style="green")
        bin_table.add_column("Count", justify="right")
        bin_table.add_column("Avg 1D", justify="right")
        bin_table.add_column("Avg 2D", justify="right")
        bin_table.add_column("Avg 3D", justify="right")
        bin_table.add_column("WinRate 1D", justify="right")

        for bin_stat in bins:
            bin_table.add_row(
                bin_stat.bin_label,
                str(bin_stat.count),
                _format_return(bin_stat.avg_1d),
                _format_return(bin_stat.avg_2d),
                _format_return(bin_stat.avg_3d),
                f"{bin_stat.win_rate_1d:.1f}%",
            )
        console.print(bin_table)
        console.print()

    # 3. Significance Tests
    if report.significance_tests:
        sig_table = Table(title="Significance Tests (Q5 vs Q1)", show_header=True, header_style="bold yellow")
        sig_table.add_column("Feature", style="yellow")
        sig_table.add_column("t-stat 1D", justify="right")
        sig_table.add_column("p-value 1D", justify="right")
        sig_table.add_column("Sig 1D", justify="center")
        sig_table.add_column("p-value 2D", justify="right")
        sig_table.add_column("Sig 2D", justify="center")
        sig_table.add_column("p-value 3D", justify="right")
        sig_table.add_column("Sig 3D", justify="center")

        for test in report.significance_tests:
            sig_table.add_row(
                test.feature,
                f"{test.t_stat_1d:.2f}",
                _format_pvalue(test.p_value_1d),
                _format_significance(test.p_value_1d),
                _format_pvalue(test.p_value_2d),
                _format_significance(test.p_value_2d),
                _format_pvalue(test.p_value_3d),
                _format_significance(test.p_value_3d),
            )
        console.print(sig_table)


def export_analysis_to_csv(report: AnalysisReport, filepath: str) -> None:
    """Export analysis results to CSV file."""
    rows = []

    # Export correlations
    for corr in report.correlations:
        rows.append(
            {
                "type": "correlation",
                "feature": corr.feature,
                "metric": "corr_1d",
                "value": corr.corr_1d,
            }
        )
        rows.append(
            {
                "type": "correlation",
                "feature": corr.feature,
                "metric": "corr_2d",
                "value": corr.corr_2d,
            }
        )
        rows.append(
            {
                "type": "correlation",
                "feature": corr.feature,
                "metric": "corr_3d",
                "value": corr.corr_3d,
            }
        )

    # Export bin stats
    for feature, bins in report.bin_stats.items():
        for bin_stat in bins:
            rows.append(
                {
                    "type": "bin_stats",
                    "feature": feature,
                    "bin": bin_stat.bin_label,
                    "count": bin_stat.count,
                    "avg_1d": bin_stat.avg_1d,
                    "avg_2d": bin_stat.avg_2d,
                    "avg_3d": bin_stat.avg_3d,
                    "win_rate_1d": bin_stat.win_rate_1d,
                    "win_rate_2d": bin_stat.win_rate_2d,
                    "win_rate_3d": bin_stat.win_rate_3d,
                }
            )

    # Export significance tests
    for test in report.significance_tests:
        rows.append(
            {
                "type": "significance",
                "feature": test.feature,
                "t_stat_1d": test.t_stat_1d,
                "p_value_1d": test.p_value_1d,
                "t_stat_2d": test.t_stat_2d,
                "p_value_2d": test.p_value_2d,
                "t_stat_3d": test.t_stat_3d,
                "p_value_3d": test.p_value_3d,
            }
        )

    df = pd.DataFrame(rows)
    df.to_csv(filepath, index=False)


def _format_correlation(value: float) -> str:
    """Format correlation value with color."""
    if value > 0.1:
        return f"[green]{value:+.4f}[/green]"
    if value < -0.1:
        return f"[red]{value:+.4f}[/red]"
    return f"{value:+.4f}"


def _format_return(value: float) -> str:
    """Format return value with color."""
    if value > 0:
        return f"[green]{value:+.2f}%[/green]"
    if value < 0:
        return f"[red]{value:+.2f}%[/red]"
    return f"{value:+.2f}%"


def _format_pvalue(value: float) -> str:
    """Format p-value."""
    if value < 0.001:
        return "<0.001"
    return f"{value:.4f}"


def _format_significance(p_value: float) -> str:
    """Format significance indicator."""
    if p_value < 0.01:
        return "[bold green]**[/bold green]"
    if p_value < 0.05:
        return "[green]*[/green]"
    return ""
