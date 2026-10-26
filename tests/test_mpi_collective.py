"""
Standalone MPI Collective Communication Test (Lab 9).

Designed to be executed with mpiexec:
  mpiexec -n 2 python tests/test_mpi_collective.py
  mpiexec -n 4 python tests/test_mpi_collective.py
"""

import sys
import os

# Ensure project root in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from mpi.collective_communication import MPI_AVAILABLE, MPI_LOAD_ERROR, run_collective_pipeline

def main():
    if not MPI_AVAILABLE:
        print("[MPI ERROR] Cannot execute MPI test: mpi4py runtime is not available.")
        print(f"Details: {MPI_LOAD_ERROR}")
        print("Please install Microsoft MPI on Windows or MPICH/OpenMPI on Linux/macOS.")
        sys.exit(1)

    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Define test trades
    test_trades = [
        {"trade_id": 1, "symbol": "AAPL", "price": 150.0, "quantity": 100},
        {"trade_id": 2, "symbol": "TSLA", "price": 200.0, "quantity": 50},
        {"trade_id": 3, "symbol": "AAPL", "price": 152.0, "quantity": 75},
        {"trade_id": 4, "symbol": "MSFT", "price": 310.0, "quantity": 40},
    ]

    result = run_collective_pipeline(comm=comm, trades=test_trades)

    if rank == 0:
        assert result is not None, "Rank 0 must produce a consolidated result"
        assert result["total_trades"] == 4, f"Expected 4 trades, got {result['total_trades']}"
        assert result["total_volume"] == 265, f"Expected 265 volume, got {result['total_volume']}"
        expected_val = 150*100 + 200*50 + 152*75 + 310*40  # 15000 + 10000 + 11400 + 12400 = 48800.0
        assert abs(result["total_traded_value"] - expected_val) < 1e-2, f"Value mismatch: {result['total_traded_value']} vs {expected_val}"
        print(f"[TEST PASSED] MPI Collective Communication verified across {size} ranks.")

if __name__ == "__main__":
    main()
