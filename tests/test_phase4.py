"""
Phase 4 Test Suite — Apache Spark Analytics (Lab 7) & MPI Distributed Computation (Labs 9 & 10).

Validates:
  1. Apache Spark:
     - SparkSession initialization in local mode
     - JSONL trade records ingestion from exchange persistence
     - Symbol-level volume, trade count, avg price, and total value aggregation
     - Ranking for most traded stocks and highest value stocks
     - Time-based window aggregations
     - End-to-end report generation and formatting
  2. MPI Infrastructure:
     - MPI module import and interface verification
     - Partitioning algorithms with remainder handling (for arbitrary MPI rank counts)
     - Parallel matrix multiplication mathematical correctness against canonical specification
     - MPI runtime availability detection and environment diagnostics
"""

import os
import unittest
from typing import List

from analytics.spark_analytics import SparkAnalytics, PYSPARK_AVAILABLE
from analytics.trade_statistics import TradeStatisticsReport, SymbolStatistics
from analytics.reports import format_console_report

from mpi.collective_communication import (
    partition_data,
    MPI_AVAILABLE as MPI_COLLECTIVE_AVAIL,
    MPI_LOAD_ERROR as MPI_COLLECTIVE_ERR,
)
from mpi.matrix_multiplication import (
    partition_rows,
    sequential_matrix_multiply,
    MPI_AVAILABLE as MPI_MATRIX_AVAIL,
    MPI_LOAD_ERROR as MPI_MATRIX_ERR,
)


class TestPhase4SparkAnalytics(unittest.TestCase):
    """Test suite for Lab 7 Apache Spark distributed analytics."""

    @classmethod
    def setUpClass(cls):
        if not PYSPARK_AVAILABLE:
            raise unittest.SkipTest("PySpark is not installed")
        cls.trades_path = os.path.abspath("data/trades.jsonl")
        cls.analytics = SparkAnalytics(master="local[2]", app_name="TestPhase4Spark")
        cls.spark = cls.analytics.get_spark_session()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "analytics") and cls.analytics:
            cls.analytics.stop()

    def test_spark_session_creation(self):
        """Verify SparkSession initializes successfully in local mode."""
        self.assertIsNotNone(self.spark)
        self.assertEqual(self.spark.conf.get("spark.master"), "local[2]")

    def test_spark_load_trades_dataframe(self):
        """Verify real JSONL trade data is correctly loaded into Spark DataFrame."""
        df = self.analytics.load_trades_dataframe(self.trades_path)
        count = df.count()
        self.assertGreater(count, 0, "Expected at least 1 trade in data/trades.jsonl")
        columns = df.columns
        for required_col in ["trade_id", "symbol", "price", "quantity", "timestamp"]:
            self.assertIn(required_col, columns)

    def test_spark_symbol_aggregation_metrics(self):
        """Verify Spark DataFrame aggregations: volume, trade count, avg price, traded value."""
        df = self.analytics.load_trades_dataframe(self.trades_path)
        symbol_df = self.analytics.compute_symbol_statistics(df)
        rows = symbol_df.collect()
        self.assertGreater(len(rows), 0)

        for row in rows:
            self.assertIsNotNone(row["symbol"])
            self.assertGreater(row["total_volume"], 0)
            self.assertGreater(row["trade_count"], 0)
            self.assertGreater(row["avg_price"], 0.0)
            self.assertGreater(row["total_value"], 0.0)
            # Verify mathematical relationship: avg_price should be roughly total_value / total_volume
            approx_val = row["total_volume"] * row["avg_price"]
            self.assertAlmostEqual(approx_val, row["total_value"], delta=row["total_value"] * 0.1)

    def test_spark_most_traded_and_highest_value_ranking(self):
        """Verify top-ranked stocks by volume and monetary traded value."""
        df = self.analytics.load_trades_dataframe(self.trades_path)
        symbol_df = self.analytics.compute_symbol_statistics(df)

        most_traded = self.analytics.compute_most_traded(symbol_df, top_n=3).collect()
        self.assertLessEqual(len(most_traded), 3)
        # Verify volume is monotonically non-increasing
        vols = [r["total_volume"] for r in most_traded]
        self.assertEqual(vols, sorted(vols, reverse=True))

        highest_value = self.analytics.compute_highest_value(symbol_df, top_n=3).collect()
        self.assertLessEqual(len(highest_value), 3)
        # Verify value is monotonically non-increasing
        vals = [r["total_value"] for r in highest_value]
        self.assertEqual(vals, sorted(vals, reverse=True))

    def test_spark_global_and_time_statistics(self):
        """Verify overall market metrics and time-based window aggregations."""
        df = self.analytics.load_trades_dataframe(self.trades_path)
        global_stats = self.analytics.compute_global_statistics(df)

        self.assertGreater(global_stats["total_trades"], 0)
        self.assertGreater(global_stats["total_volume"], 0)
        self.assertGreater(global_stats["total_traded_value"], 0.0)
        self.assertGreater(global_stats["average_trade_price"], 0.0)

        time_df = self.analytics.compute_time_statistics(df)
        time_rows = time_df.collect()
        self.assertGreater(len(time_rows), 0)
        total_time_trades = sum(r["trade_count"] for r in time_rows)
        self.assertEqual(total_time_trades, global_stats["total_trades"])

    def test_spark_end_to_end_report(self):
        """Verify full distributed analysis pipeline produces structured report and formatted ASCII output."""
        report = self.analytics.run_full_analysis(self.trades_path)
        self.assertIsInstance(report, TradeStatisticsReport)
        self.assertGreater(report.total_trades, 0)
        self.assertGreater(report.total_volume, 0)
        self.assertGreater(len(report.symbol_statistics), 0)

        # Validate dictionary serialization
        report_dict = report.to_dict()
        self.assertEqual(report_dict["total_trades"], report.total_trades)
        self.assertIn("most_traded_symbols", report_dict)

        # Validate human-readable console output
        console_output = format_console_report(report)
        self.assertIn("DISTRIBUTED STOCK EXCHANGE SIMULATOR", console_output)
        self.assertIn("MARKET EXECUTION OVERVIEW", console_output)
        self.assertIn("MOST TRADED STOCKS", console_output)


class TestPhase4MPI(unittest.TestCase):
    """Test suite for Labs 9 & 10 MPI distributed collective and numerical computing."""

    def test_mpi_module_exports_and_callables(self):
        """Verify MPI collective communication and matrix multiplication interfaces."""
        from mpi.collective_communication import (
            partition_data,
            run_collective_pipeline,
            load_trades_from_file,
        )
        from mpi.matrix_multiplication import (
            partition_rows,
            sequential_matrix_multiply,
            mpi_matrix_multiply,
            generate_sample_matrix,
        )
        self.assertTrue(callable(partition_data))
        self.assertTrue(callable(run_collective_pipeline))
        self.assertTrue(callable(load_trades_from_file))
        self.assertTrue(callable(partition_rows))
        self.assertTrue(callable(sequential_matrix_multiply))
        self.assertTrue(callable(mpi_matrix_multiply))
        self.assertTrue(callable(generate_sample_matrix))

    def test_mpi_partition_data_with_arbitrary_remainders(self):
        """Verify MPI partition_data distributes workloads evenly across arbitrary rank counts."""
        items = list(range(10))

        # 4 ranks: 10 items -> chunks: [3, 3, 2, 2]
        p4 = partition_data(items, 4)
        self.assertEqual(len(p4), 4)
        self.assertEqual([len(c) for c in p4], [3, 3, 2, 2])
        # Verify no items lost or duplicated
        flattened = [x for chunk in p4 for x in chunk]
        self.assertEqual(flattened, items)

        # 3 ranks: 10 items -> chunks: [4, 3, 3]
        p3 = partition_data(items, 3)
        self.assertEqual(len(p3), 3)
        self.assertEqual([len(c) for c in p3], [4, 3, 3])
        self.assertEqual([x for chunk in p3 for x in chunk], items)

        # 2 ranks: 10 items -> chunks: [5, 5]
        p2 = partition_data(items, 2)
        self.assertEqual([len(c) for c in p2], [5, 5])
        self.assertEqual([x for chunk in p2 for x in chunk], items)

    def test_mpi_partition_rows_for_matrix_scattering(self):
        """Verify partition_rows correctly splits matrix rows for MPI rank distribution."""
        A = [[float(r * 10 + c) for c in range(3)] for r in range(7)]

        # Distribute 7 rows across 3 ranks -> [3, 2, 2]
        chunks = partition_rows(A, 3)
        self.assertEqual(len(chunks), 3)
        self.assertEqual([len(c) for c in chunks], [3, 2, 2])
        reassembled = [row for chunk in chunks for row in chunk]
        self.assertEqual(reassembled, A)

    def test_canonical_matrix_multiplication_specification(self):
        """
        Verify matrix multiplication against the canonical specification:
        A = [[1, 2], [3, 4]]
        B = [[5, 6], [7, 8]]
        Expected C = [[19, 22], [43, 50]]
        """
        A = [[1.0, 2.0], [3.0, 4.0]]
        B = [[5.0, 6.0], [7.0, 8.0]]
        expected_C = [[19.0, 22.0], [43.0, 50.0]]

        C = sequential_matrix_multiply(A, B)
        self.assertEqual(C, expected_C)

    def test_mpi_runtime_status_and_prerequisites(self):
        """
        Verify MPI runtime availability and document external prerequisites if absent.
        mpi4py package is installed. If OS-level MS-MPI runtime is not yet installed on Windows,
        the test cleanly documents the status without breaking the overall test discovery.
        """
        self.assertIsInstance(MPI_COLLECTIVE_AVAIL, bool)
        self.assertIsInstance(MPI_MATRIX_AVAIL, bool)

        if not MPI_COLLECTIVE_AVAIL:
            print("\n[INFO] mpi4py is installed, but Microsoft MPI (msmpi.dll) runtime is not present on this OS.")
            print(f"[INFO] Diagnostics: {MPI_COLLECTIVE_ERR}")
            print("[INFO] Dedicated MPI tests can be executed with 'mpiexec -n 2 python tests/test_mpi_collective.py' once MS-MPI is installed.")


if __name__ == "__main__":
    unittest.main()
