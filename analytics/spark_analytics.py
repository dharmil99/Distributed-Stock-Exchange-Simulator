"""
Apache Spark Distributed Analytics Pipeline (Lab 7).

Consumes real trade execution records produced by the Distributed Stock Exchange Simulator,
using genuine PySpark DataFrame operations to compute:
  A. Total trading volume by symbol
  B. Number of trades by symbol
  C. Average execution price by symbol
  D. Total traded value by symbol (quantity * execution_price)
  E. Most traded stocks (ranked by volume)
  F. Highest-value stocks (ranked by total value)
  G. Aggregate market trade statistics
  H. Time-based trade and volume statistics

Primary computation remains fully distributed within Spark DataFrames and Spark SQL.
Pandas is NOT used for core distributed aggregations.
"""

import os
import sys
import argparse
from typing import Optional, Dict, Any, List

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType, LongType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from analytics.trade_statistics import SymbolStatistics, TradeStatisticsReport


TRADE_SCHEMA = StructType([
    StructField("trade_id", LongType(), True),
    StructField("buy_order_id", LongType(), True),
    StructField("sell_order_id", LongType(), True),
    StructField("buyer_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("lamport_ts", LongType(), True),
])


class SparkAnalytics:
    """
    Spark Distributed Analytics Engine for the Stock Exchange Simulator.
    """

    def __init__(
        self,
        app_name: str = "DistributedStockExchangeAnalytics",
        master: str = "local[*]",
        spark_session: Optional["SparkSession"] = None,
    ):
        if not PYSPARK_AVAILABLE:
            raise RuntimeError(
                "pyspark is not installed. Please install pyspark to run Spark analytics: "
                "pip install pyspark>=4.2.0"
            )
        self.app_name = app_name
        self.master = master
        self._spark = spark_session

    def get_spark_session(self) -> "SparkSession":
        """Get or lazily initialize the SparkSession in local mode."""
        if self._spark is None or self._spark._jsparkSession is None:
            builder = (
                SparkSession.builder
                .appName(self.app_name)
                .master(self.master)
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
            )
            self._spark = builder.getOrCreate()
            self._spark.sparkContext.setLogLevel("ERROR")
        return self._spark

    def load_trades_dataframe(self, filepath: str):
        """
        Load JSONL trade records into a typed Spark DataFrame.
        """
        spark = self.get_spark_session()
        if not os.path.isabs(filepath):
            filepath = os.path.abspath(filepath)

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Trades data file not found: {filepath}")

        # Spark reads JSON lines automatically with .read.json()
        df = spark.read.schema(TRADE_SCHEMA).json(filepath)
        return df

    def compute_symbol_statistics(self, df):
        """
        Calculates symbol-level aggregations using Spark DataFrame operations:
        - Total volume
        - Trade count
        - Average price
        - Total traded value (quantity * price)
        - Min price
        - Max price
        """
        # Traded value per row: quantity * price
        with_value = df.withColumn("traded_value", F.col("quantity") * F.col("price"))

        symbol_agg = with_value.groupBy("symbol").agg(
            F.sum("quantity").alias("total_volume"),
            F.count("trade_id").alias("trade_count"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.sum("traded_value"), 2).alias("total_value"),
            F.round(F.min("price"), 2).alias("min_price"),
            F.round(F.max("price"), 2).alias("max_price"),
        ).orderBy(F.col("total_volume").desc())

        return symbol_agg

    def compute_most_traded(self, symbol_df, top_n: int = 5):
        """Rank symbols by total trading volume."""
        return symbol_df.orderBy(F.col("total_volume").desc()).limit(top_n)

    def compute_highest_value(self, symbol_df, top_n: int = 5):
        """Rank symbols by total traded monetary value."""
        return symbol_df.orderBy(F.col("total_value").desc()).limit(top_n)

    def compute_global_statistics(self, df) -> Dict[str, Any]:
        """
        Calculate total market trade statistics via Spark aggregate actions.
        """
        with_value = df.withColumn("traded_value", F.col("quantity") * F.col("price"))

        agg_row = with_value.agg(
            F.count("trade_id").alias("total_trades"),
            F.sum("quantity").alias("total_volume"),
            F.round(F.sum("traded_value"), 2).alias("total_traded_value"),
            F.round(F.avg("price"), 2).alias("average_trade_price"),
        ).collect()[0]

        return {
            "total_trades": int(agg_row["total_trades"] or 0),
            "total_volume": int(agg_row["total_volume"] or 0),
            "total_traded_value": float(agg_row["total_traded_value"] or 0.0),
            "average_trade_price": float(agg_row["average_trade_price"] or 0.0),
        }

    def compute_time_statistics(self, df):
        """
        Aggregate trades by time interval (e.g. hourly window based on ISO timestamp).
        """
        # Extract date/hour prefix: YYYY-MM-DDTHH
        hourly_df = (
            df.withColumn("traded_value", F.col("quantity") * F.col("price"))
            .withColumn("time_window", F.substring(F.col("timestamp"), 1, 13))
            .groupBy("time_window")
            .agg(
                F.count("trade_id").alias("trade_count"),
                F.sum("quantity").alias("volume"),
                F.round(F.sum("traded_value"), 2).alias("traded_value"),
            )
            .orderBy("time_window")
        )
        return hourly_df

    def run_full_analysis(self, filepath: str) -> TradeStatisticsReport:
        """
        Execute full distributed analytics pipeline on exported trade records.
        Returns a structured TradeStatisticsReport.
        """
        df = self.load_trades_dataframe(filepath)

        # Check if dataset is empty
        trade_count = df.count()
        if trade_count == 0:
            return TradeStatisticsReport(
                total_trades=0,
                total_volume=0,
                total_traded_value=0.0,
                average_trade_price=0.0,
                metadata={"source_file": filepath, "empty": True},
            )

        # 1. Global statistics
        global_stats = self.compute_global_statistics(df)

        # 2. Symbol statistics
        symbol_df = self.compute_symbol_statistics(df)
        symbol_rows = symbol_df.collect()
        symbol_stats_list = [
            {
                "symbol": r["symbol"],
                "total_volume": int(r["total_volume"] or 0),
                "trade_count": int(r["trade_count"] or 0),
                "avg_price": float(r["avg_price"] or 0.0),
                "total_value": float(r["total_value"] or 0.0),
                "min_price": float(r["min_price"] or 0.0),
                "max_price": float(r["max_price"] or 0.0),
            }
            for r in symbol_rows
        ]

        # 3. Top Traded Stocks (by Volume)
        most_traded_rows = self.compute_most_traded(symbol_df, top_n=5).collect()
        most_traded_list = [
            {
                "symbol": r["symbol"],
                "total_volume": int(r["total_volume"] or 0),
                "trade_count": int(r["trade_count"] or 0),
                "avg_price": float(r["avg_price"] or 0.0),
            }
            for r in most_traded_rows
        ]

        # 4. Highest Value Stocks (by Traded Value)
        highest_val_rows = self.compute_highest_value(symbol_df, top_n=5).collect()
        highest_val_list = [
            {
                "symbol": r["symbol"],
                "total_value": float(r["total_value"] or 0.0),
                "total_volume": int(r["total_volume"] or 0),
                "avg_price": float(r["avg_price"] or 0.0),
            }
            for r in highest_val_rows
        ]

        # 5. Time-based statistics
        time_df = self.compute_time_statistics(df)
        time_rows = time_df.collect()
        time_stats_list = [
            {
                "time_window": str(r["time_window"]),
                "trade_count": int(r["trade_count"] or 0),
                "volume": int(r["volume"] or 0),
                "traded_value": float(r["traded_value"] or 0.0),
            }
            for r in time_rows
        ]

        report = TradeStatisticsReport(
            total_trades=global_stats["total_trades"],
            total_volume=global_stats["total_volume"],
            total_traded_value=global_stats["total_traded_value"],
            average_trade_price=global_stats["average_trade_price"],
            most_traded_symbols=most_traded_list,
            highest_value_symbols=highest_val_list,
            symbol_statistics=symbol_stats_list,
            time_statistics=time_stats_list,
            metadata={
                "source_file": filepath,
                "engine": "Apache Spark (PySpark)",
                "master": self.master,
                "app_name": self.app_name,
            },
        )
        return report

    def stop(self):
        """Stop active SparkSession."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None


def main():
    parser = argparse.ArgumentParser(description="Distributed Stock Exchange Spark Analytics (Lab 7)")
    parser.add_argument(
        "--input",
        type=str,
        default="data/trades.jsonl",
        help="Path to JSONL trades export file (default: data/trades.jsonl)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional path to save JSON analytics report",
    )
    args = parser.parse_args()

    # Import report formatter
    from analytics.reports import format_console_report, save_report_json

    if not os.path.exists(args.input):
        print(f"Error: Input trades file not found: {args.input}")
        print("Tip: Run trading sessions or export trade records from SQLite first.")
        sys.exit(1)

    print(f"[*] Initializing Apache Spark analytics engine on {args.input}...")
    analytics = SparkAnalytics()
    try:
        report = analytics.run_full_analysis(args.input)
        print(format_console_report(report))
        if args.output:
            save_report_json(report, args.output)
            print(f"[+] Saved structured analytics report to: {args.output}")
    finally:
        analytics.stop()


if __name__ == "__main__":
    main()
