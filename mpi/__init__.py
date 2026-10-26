"""
Distributed Stock Exchange Simulator — MPI Module (Labs 9 & 10).

Provides high-performance distributed computing capabilities using genuine mpi4py:
- Lab 9: Collective Communication (MPI_Bcast, MPI_Scatter, MPI_Gather)
- Lab 10: Parallel Matrix Multiplication
"""

from mpi.collective_communication import (
    MPI_AVAILABLE as MPI_COLLECTIVE_AVAILABLE,
    MPI_LOAD_ERROR as MPI_COLLECTIVE_ERROR,
    run_collective_pipeline,
)
from mpi.matrix_multiplication import (
    MPI_AVAILABLE as MPI_MATRIX_AVAILABLE,
    MPI_LOAD_ERROR as MPI_MATRIX_ERROR,
    mpi_matrix_multiply,
)

MPI_AVAILABLE = MPI_COLLECTIVE_AVAILABLE and MPI_MATRIX_AVAILABLE

__all__ = [
    "MPI_AVAILABLE",
    "MPI_COLLECTIVE_AVAILABLE",
    "MPI_COLLECTIVE_ERROR",
    "MPI_MATRIX_AVAILABLE",
    "MPI_MATRIX_ERROR",
    "run_collective_pipeline",
    "mpi_matrix_multiply",
]
