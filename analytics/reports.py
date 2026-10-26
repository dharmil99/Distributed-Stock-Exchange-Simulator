"""
Report Generation and Formatting for Exchange Analytics.

Provides clean console output formatting and structured JSON serialization
for the Apache Spark distributed analytics pipeline (Lab 7).
"""

import json
from typing import Optional
from analytics.trade_statistics import TradeStatisticsReport


def format_console_report(report: TradeStatisticsReport) -> str:
    """Format TradeStatisticsReport into a clean, human-readable terminal dashboard."""
    lines = []
    bar = "=" * 80
    sub_bar = "-" * 80

    lines.append(bar)
    lines.append("     DISTRIBUTED STOCK EXCHANGE SIMULATOR — SPARK ANALYTICS (LAB 7)")
    lines.append(bar)

    lines.append("\n[1] MARKET EXECUTION OVERVIEW:")
    lines.append(f"  Total Trades:        {report.total_trades:,}")
    lines.append(f"  Total Traded Volume: {report.total_volume:,} shares")
    lines.append(f"  Total Traded Value:  ${report.total_traded_value:,.2f}")
    lines.append(f"  Average Trade Price: ${report.average_trade_price:,.2f}")

    lines.append("\n[2] MOST TRADED STOCKS (RANKED BY VOLUME):")
    if not report.most_traded_symbols:
        lines.append("  (No trade volume recorded)")
    else:
        for idx, item in enumerate(report.most_traded_symbols, 1):
            lines.append(
                f"  {idx}. {item['symbol']:<6} | Volume: {item['total_volume']:>8,} shares "
                f"| Trades: {item['trade_count']:>4} | Avg Price: ${item['avg_price']:>8.2f}"
            )

    lines.append("\n[3] HIGHEST VALUE STOCKS (RANKED BY TOTAL TRADED VALUE):")
    if not report.highest_value_symbols:
        lines.append("  (No trade value recorded)")
    else:
        for idx, item in enumerate(report.highest_value_symbols, 1):
            lines.append(
                f"  {idx}. {item['symbol']:<6} | Value: ${item['total_value']:>12,.2f} "
                f"| Volume: {item['total_volume']:>8,} shares | Avg Price: ${item['avg_price']:>8.2f}"
            )

    lines.append("\n[4] SYMBOL BREAKDOWN:")
    lines.append(
        f"  {'Symbol':<8} {'Trades':<8} {'Volume':<12} {'Total Value':<16} {'Avg Price':<12} {'Min Price':<10} {'Max Price':<10}"
    )
    lines.append("  " + "-" * 76)
    if not report.symbol_statistics:
        lines.append("  (No symbol trade data)")
    else:
        for s in report.symbol_statistics:
            lines.append(
                f"  {s['symbol']:<8} {s['trade_count']:<8} {s['total_volume']:<12,} "
                f"${s['total_value']:<15,.2f} ${s['avg_price']:<11.2f} ${s['min_price']:<9.2f} ${s['max_price']:<9.2f}"
            )

    if report.time_statistics:
        lines.append("\n[5] TIME-BASED WINDOW ACTIVITY:")
        lines.append(f"  {'Time Window':<20} {'Trades':<10} {'Volume':<12} {'Traded Value':<16}")
        lines.append("  " + "-" * 58)
        for t in report.time_statistics:
            lines.append(
                f"  {t['time_window']:<20} {t['trade_count']:<10} {t['volume']:<12,} ${t['traded_value']:<15,.2f}"
            )

    lines.append("\n" + bar)
    return "\n".join(lines)


def save_report_json(report: TradeStatisticsReport, output_path: str):
    """Serialize the report to a JSON file."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, indent=2)
