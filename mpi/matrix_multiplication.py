"""
Parallel Matrix Multiplication using MPI (Lab 10).

Demonstrates parallel numerical computation in distributed systems:
  Computes C = A × B
  where:
    A is an M × K matrix (e.g. Portfolio asset holding weights across portfolios)
    B is a K × N matrix (e.g. Asset covariance / risk factor loadings)
    C is an M × N matrix (e.g. Portfolio risk exposure profiles)

Distributed Computation Flow:
  1. Rank 0 initializes or loads input matrices A and B.
  2. Dimensions (M, K, N) and Matrix B are broadcast to all ranks via MPI_Bcast.
  3. Rows of Matrix A are partitioned and distributed to worker ranks via MPI_Scatter.
  4. Each rank independently computes its assigned rows via pure distributed row-column dot products.
     (NumPy is NOT used for the distributed multiplication; workers perform the actual arithmetic).
  5. Computed row slices are gathered to Rank 0 via MPI_Gather.
  6. Rank 0 reconstructs final Matrix C and validates correctness against sequential / NumPy reference.

Requires genuine mpi4py (`from mpi4py import MPI`).
Must be executed via an MPI launcher such as:
  mpiexec -n 4 python -m mpi.matrix_multiplication --rows 8 --cols 8
"""

import sys
import argparse
from typing import List, Tuple, Optional, Any

try:
    from mpi4py import MPI
    MPI_AVAILABLE = True
    MPI_LOAD_ERROR = None
except Exception as _err:
    MPI = None
    MPI_AVAILABLE = False
    MPI_LOAD_ERROR = str(_err)

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False


def partition_rows(matrix_a: List[List[float]], num_chunks: int) -> List[List[List[float]]]:
    """
    Partition the rows of matrix A across num_chunks ranks.
    Evenly distributes remaining rows when len(matrix_a) % num_chunks != 0.
    """
    if num_chunks <= 0:
        return []
    total_rows = len(matrix_a)
    base_rows = total_rows // num_chunks
    remainder = total_rows % num_chunks

    chunks = []
    curr = 0
    for rank in range(num_chunks):
        chunk_len = base_rows + (1 if rank < remainder else 0)
        chunks.append(matrix_a[curr:curr + chunk_len])
        curr += chunk_len
    return chunks


def sequential_matrix_multiply(
    A: List[List[float]], B: List[List[float]]
) -> List[List[float]]:
    """
    Reference sequential implementation for correctness validation: C = A x B.
    """
    M = len(A)
    K = len(A[0]) if M > 0 else 0
    N = len(B[0]) if len(B) > 0 else 0

    C = [[0.0 for _ in range(N)] for _ in range(M)]
    for i in range(M):
        for j in range(N):
            val = 0.0
            for k in range(K):
                val += A[i][k] * B[k][j]
            C[i][j] = val
    return C


def mpi_matrix_multiply(
    A: Optional[List[List[float]]] = None,
    B: Optional[List[List[float]]] = None,
    comm=None,
) -> Optional[List[List[float]]]:
    """
    Perform parallel matrix multiplication across MPI ranks.

    Returns:
      The resulting matrix C on Rank 0, None on worker ranks.
    """
    if comm is None:
        if not MPI_AVAILABLE:
            raise RuntimeError(
                f"mpi4py MPI runtime is unavailable: {MPI_LOAD_ERROR}\n"
                "Prerequisite: Install Microsoft MPI v10+ (msmpisetup.exe) on Windows, "
                "or MPICH / OpenMPI on Linux/macOS."
            )
        comm = MPI.COMM_WORLD

    rank = comm.Get_rank()
    size = comm.Get_size()

    # Step 1: Dimensions and Validation on Root (Rank 0)
    dims = None
    if rank == 0:
        if A is None or B is None:
            # Default test matrices matching the specification:
            # A = [[1, 2], [3, 4]], B = [[5, 6], [7, 8]] -> C = [[19, 22], [43, 50]]
            A = [[1.0, 2.0], [3.0, 4.0]]
            B = [[5.0, 6.0], [7.0, 8.0]]

        M = len(A)
        K = len(A[0]) if M > 0 else 0
        K_b = len(B)
        N = len(B[0]) if K_b > 0 else 0

        if K != K_b:
            raise ValueError(f"Incompatible matrix dimensions: A is {M}x{K}, B is {K_b}x{N}")

        dims = (M, K, N)
        print(f"[Rank 0/{size}] Initiating MPI Matrix Multiplication: A({M}x{K}) × B({K}x{N}) = C({M}x{N})")

    # Step 2: MPI_Bcast — Broadcast matrix dimensions and matrix B to all ranks
    dims = comm.bcast(dims, root=0)
    M, K, N = dims
    B = comm.bcast(B, root=0)

    # Step 3: MPI_Scatter — Partition and scatter rows of A across all ranks
    row_chunks = None
    if rank == 0:
        row_chunks = partition_rows(A, size)

    local_A_rows = comm.scatter(row_chunks, root=0)
    assigned_count = len(local_A_rows)
    print(f"[Rank {rank}/{size}] Received {assigned_count} rows of matrix A to compute.")

    # Step 4: Worker Computation — Distributed row-column dot products
    # Pure worker calculation without NumPy doing the distributed multiplication
    local_C_rows = []
    for a_row in local_A_rows:
        c_row = []
        for j in range(N):
            val = 0.0
            for k in range(K):
                val += a_row[k] * B[k][j]
            c_row.append(val)
        local_C_rows.append(c_row)

    # Step 5: MPI_Gather — Gather computed row slices back to Rank 0
    gathered_chunks = comm.gather(local_C_rows, root=0)

    # Step 6: Reconstruction and Correctness Verification on Rank 0
    if rank == 0:
        final_C = []
        for chunk in gathered_chunks:
            final_C.extend(chunk)

        # Validate against sequential reference
        expected_C = sequential_matrix_multiply(A, B)
        matches = True
        for i in range(M):
            for j in range(N):
                if abs(final_C[i][j] - expected_C[i][j]) > 1e-6:
                    matches = False
                    break

        print("\n" + "=" * 70)
        print("     PARALLEL MATRIX MULTIPLICATION (LAB 10) — VERIFICATION")
        print("=" * 70)
        print(f"Matrix A ({M}x{K}):")
        for row in A[:4]:
            print("  " + str([round(x, 2) for x in row]))
        if M > 4:
            print("  ...")

        print(f"\nMatrix B ({K}x{N}):")
        for row in B[:4]:
            print("  " + str([round(x, 2) for x in row]))
        if K > 4:
            print("  ...")

        print(f"\nComputed Matrix C = A x B ({M}x{N}):")
        for row in final_C[:4]:
            print("  " + str([round(x, 2) for x in row]))
        if M > 4:
            print("  ...")

        print(f"\nCorrectness Check Against Sequential Product: {'PASSED (EXACT MATCH)' if matches else 'FAILED'}")
        if NUMPY_AVAILABLE:
            np_check = np.allclose(np.array(final_C), np.dot(np.array(A), np.array(B)))
            print(f"NumPy Reference Validation:                   {'PASSED (np.allclose)' if np_check else 'FAILED'}")
        print("=" * 70 + "\n")

        return final_C

    return None


def generate_sample_matrix(rows: int, cols: int, seed: int = 42) -> List[List[float]]:
    """Generate a reproducible float matrix for demonstration."""
    mat = []
    val = 1.0
    for r in range(rows):
        row = []
        for c in range(cols):
            row.append(round(((r + 1) * 2.5 + (c + 1) * 1.5), 1))
        mat.append(row)
    return mat


def main():
    parser = argparse.ArgumentParser(
        description="Parallel Matrix Multiplication using MPI (Lab 10)"
    )
    parser.add_argument("--rows", type=int, default=4, help="Rows of Matrix A (default: 4)")
    parser.add_argument("--cols", type=int, default=4, help="Cols of Matrix B (default: 4)")
    parser.add_argument("--inner", type=int, default=4, help="Inner dimension K (default: 4)")
    parser.add_argument(
        "--demo-small",
        action="store_true",
        help="Run the canonical 2x2 specification test matrix ([[1,2],[3,4]] x [[5,6],[7,8]])",
    )
    args = parser.parse_args()

    if not MPI_AVAILABLE:
        print("\n[MPI RUNTIME NOTICE]")
        print(f"Error loading MPI library: {MPI_LOAD_ERROR}")
        print("\nPrerequisite: An external MPI runtime must be installed on your operating system:")
        print("  - Windows: Microsoft MPI v10+ (Download msmpisetup.exe and msmpisdk.msi)")
        print("    Link: https://www.microsoft.com/en-us/download/details.aspx?id=100593")
        print("  - Linux: sudo apt-get install mpich libmpich-dev (or openmpi-bin)")
        print("  - macOS: brew install mpich (or open-mpi)")
        print("\nOnce installed, execute this script with an MPI process launcher:")
        print(f"  mpiexec -n 4 python -m mpi.matrix_multiplication --rows {args.rows} --cols {args.cols}\n")
        sys.exit(1)

    if args.demo_small:
        A = [[1.0, 2.0], [3.0, 4.0]]
        B = [[5.0, 6.0], [7.0, 8.0]]
    else:
        A = generate_sample_matrix(args.rows, args.inner)
        B = generate_sample_matrix(args.inner, args.cols)

    mpi_matrix_multiply(A, B)


if __name__ == "__main__":
    main()
