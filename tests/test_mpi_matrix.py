"""
Standalone MPI Parallel Matrix Multiplication Test (Lab 10).

Designed to be executed with mpiexec:
  mpiexec -n 2 python tests/test_mpi_matrix.py
  mpiexec -n 4 python tests/test_mpi_matrix.py
"""

import sys
import os

# Ensure project root in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from mpi.matrix_multiplication import (
    MPI_AVAILABLE,
    MPI_LOAD_ERROR,
    mpi_matrix_multiply,
    sequential_matrix_multiply,
)

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

    # Exact canonical specification test matrices:
    # A = [[1, 2], [3, 4]]
    # B = [[5, 6], [7, 8]]
    # Expected C = [[19, 22], [43, 50]]
    A = [[1.0, 2.0], [3.0, 4.0]]
    B = [[5.0, 6.0], [7.0, 8.0]]
    expected_C = [[19.0, 22.0], [43.0, 50.0]]

    C = mpi_matrix_multiply(A, B, comm=comm)

    if rank == 0:
        assert C is not None, "Rank 0 must produce the final matrix C"
        assert C == expected_C, f"Expected {expected_C}, got {C}"
        print(f"[TEST PASSED] Parallel Matrix Multiplication verified across {size} ranks.")

if __name__ == "__main__":
    main()
