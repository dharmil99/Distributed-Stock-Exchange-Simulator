"""
Distributed Stock Exchange Simulator — Analytics Module (Lab 7).
"""

from analytics.trade_statistics import SymbolStatistics, TradeStatisticsReport
from analytics.reports import format_console_report, save_report_json

def __getattr__(name):
    if name == "SparkAnalytics":
        from analytics.spark_analytics import SparkAnalytics
        return SparkAnalytics
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "SymbolStatistics",
    "TradeStatisticsReport",
    "format_console_report",
    "save_report_json",
    "SparkAnalytics",
]
