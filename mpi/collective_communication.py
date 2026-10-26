"""
MPI Collective Communication for Distributed Stock Exchange Analytics (Lab 9).

Implements genuine MPI collective communication patterns:
  1. MPI_Bcast: Root rank (Rank 0) distributes market configuration / symbol catalog to all processes.
  2. MPI_Scatter: Root rank partitions trade execution records among all worker processes.
  3. Worker Processing: Each rank independently calculates local volume, trade count, and traded value.
  4. MPI_Gather: Root rank gathers local statistics from all ranks and consolidates the global result.

Requires genuine mpi4py (`from mpi4py import MPI`).
Must be executed via an MPI launcher such as:
  mpiexec -n 4 python -m mpi.collective_communication
"""

import json
import os
import sys
import argparse
from typing import List, Dict, Any, Optional

try:
    from mpi4py import MPI
    MPI_AVAILABLE = True
    MPI_LOAD_ERROR = None
except Exception as _err:
    MPI = None
    MPI_AVAILABLE = False
    MPI_LOAD_ERROR = str(_err)


def partition_data(items: List[Any], num_chunks: int) -> List[List[Any]]:
    """
    Evenly partition a list of items across num_chunks ranks.
    Correctly distributes the remainder when len(items) % num_chunks != 0.
    """
    if num_chunks <= 0:
        return []
    total = len(items)
    base_size = total // num_chunks
    remainder = total % num_chunks

    chunks = []
    current_idx = 0
    for rank in range(num_chunks):
        chunk_len = base_size + (1 if rank < remainder else 0)
        chunks.append(items[current_idx:current_idx + chunk_len])
        current_idx += chunk_len
    return chunks


def load_trades_from_file(filepath: str) -> List[Dict[str, Any]]:
    """Load JSONL trade records from file."""
    if not os.path.exists(filepath):
        return []
    trades = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    trades.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return trades


def run_collective_pipeline(
    comm=None,
    trades: Optional[List[Dict[str, Any]]] = None,
    trades_file: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Executes the MPI collective communication pipeline across all participating ranks.

    Returns:
      Consolidated global statistics dictionary on Rank 0, None on worker ranks.
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

    # =========================================================================
    # STEP 1: MPI_Bcast — Root broadcasts market configuration to all processes
    # =========================================================================
    if rank == 0:
        if config is None:
            config = {
                "market_name": "DISTRIBUTED_STOCK_EXCHANGE",
                "batch_id": "BATCH-MPI-001",
                "active_symbols": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
                "currency": "USD",
                "engine_version": "Phase 4 - Lab 9",
            }
        print(f"[Rank 0/{size}] Broadcaster: Broadcasting exchange configuration via MPI_Bcast...")
    else:
        config = None

    # Broadcast exchange config from root rank 0 to all ranks
    config = comm.bcast(config, root=0)

    print(f"[Rank {rank}/{size}] Received broadcast configuration: market={config['market_name']}, batch={config['batch_id']}")

    # =========================================================================
    # STEP 2: MPI_Scatter — Root partitions trade records and scatters to workers
    # =========================================================================
    chunks = None
    if rank == 0:
        if trades is None and trades_file:
            trades = load_trades_from_file(trades_file)

        if not trades:
            # Fallback sample exchange trades if no file was loaded
            trades = [
                {"trade_id": 1, "symbol": "AAPL", "price": 150.0, "quantity": 100},
                {"trade_id": 2, "symbol": "TSLA", "price": 200.0, "quantity": 50},
                {"trade_id": 3, "symbol": "AAPL", "price": 152.0, "quantity": 75},
                {"trade_id": 4, "symbol": "MSFT", "price": 310.0, "quantity": 40},
                {"trade_id": 5, "symbol": "GOOGL", "price": 140.0, "quantity": 60},
                {"trade_id": 6, "symbol": "TSLA", "price": 198.0, "quantity": 80},
                {"trade_id": 7, "symbol": "AMZN", "price": 175.0, "quantity": 90},
                {"trade_id": 8, "symbol": "AAPL", "price": 151.0, "quantity": 120},
            ]

        print(f"[Rank 0/{size}] Scatterer: Partitioning {len(trades)} trade records across {size} MPI ranks...")
        chunks = partition_data(trades, size)

    # Scatter assigned trades chunk to each rank
    local_trades = comm.scatter(chunks, root=0)

    # =========================================================================
    # STEP 3: Local Processing — Each rank calculates local partition metrics
    # =========================================================================
    local_count = len(local_trades)
    local_volume = sum(int(t.get("quantity", 0)) for t in local_trades)
    local_value = sum(int(t.get("quantity", 0)) * float(t.get("price", 0.0)) for t in local_trades)

    local_symbol_vols: Dict[str, int] = {}
    for t in local_trades:
        sym = str(t.get("symbol", "UNKNOWN")).upper()
        local_symbol_vols[sym] = local_symbol_vols.get(sym, 0) + int(t.get("quantity", 0))

    local_stats = {
        "rank": rank,
        "trade_count": local_count,
        "volume": local_volume,
        "traded_value": local_value,
        "symbol_volumes": local_symbol_vols,
    }

    print(
        f"[Rank {rank}/{size}] Processed {local_count} trades | "
        f"Local Volume: {local_volume:,} | Local Value: ${local_value:,.2f}"
    )

    # =========================================================================
    # STEP 4: MPI_Gather — Root collects local results and aggregates globally
    # =========================================================================
    gathered_stats = comm.gather(local_stats, root=0)

    if rank == 0:
        print(f"[Rank 0/{size}] Gatherer: Collected local statistics from all {size} ranks. Aggregating...")
        global_trades = sum(s["trade_count"] for s in gathered_stats)
        global_volume = sum(s["volume"] for s in gathered_stats)
        global_value = sum(s["traded_value"] for s in gathered_stats)
        avg_price = (global_value / global_volume) if global_volume > 0 else 0.0

        global_symbol_vols: Dict[str, int] = {}
        for s in gathered_stats:
            for sym, vol in s["symbol_volumes"].items():
                global_symbol_vols[sym] = global_symbol_vols.get(sym, 0) + vol

        global_result = {
            "total_ranks": size,
            "total_trades": global_trades,
            "total_volume": global_volume,
            "total_traded_value": round(global_value, 2),
            "average_trade_price": round(avg_price, 2),
            "symbol_volumes": global_symbol_vols,
            "rank_breakdown": gathered_stats,
        }

        print("\n" + "=" * 70)
        print("     MPI COLLECTIVE COMMUNICATION (LAB 9) — CONSOLIDATED SUMMARY")
        print("=" * 70)
        print(f"Total Participating Ranks: {size}")
        print(f"Global Trade Count:        {global_trades:,}")
        print(f"Global Traded Volume:      {global_volume:,} shares")
        print(f"Global Traded Value:       ${global_value:,.2f}")
        print(f"Global Average Price:      ${avg_price:,.2f}")
        print("\nVolume by Symbol:")
        for sym, vol in sorted(global_symbol_vols.items(), key=lambda x: x[1], reverse=True):
            print(f"  {sym:<6}: {vol:>8,} shares")
        print("=" * 70 + "\n")
        return global_result

    return None


def main():
    parser = argparse.ArgumentParser(
        description="MPI Collective Communication for Stock Exchange Analytics (Lab 9)"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/trades.jsonl",
        help="Path to trades JSONL file (default: data/trades.jsonl)",
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
        print(f"  mpiexec -n 4 python -m mpi.collective_communication --input {args.input}\n")
        sys.exit(1)

    trades_file = args.input if os.path.exists(args.input) else None
    run_collective_pipeline(trades_file=trades_file)


if __name__ == "__main__":
    main()
