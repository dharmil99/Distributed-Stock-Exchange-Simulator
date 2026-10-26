# Distributed Stock Exchange Simulator

An enterprise-grade, academically unified Distributed Computing laboratory project implementing all **10 Distributed Systems experiments** as an integrated, multi-node stock exchange simulator.

---

## 1. System Architecture

```
                                  TRADER CLIENTS
                    +-------------------+-------------------+
                    |   Trader: Alice   |    Trader: Bob    |
                    +-------------------+-------------------+
                                          | XML-RPC
                                          v
                               +---------------------+
                               |    LOAD BALANCER    |
                               | (Round-Robin Proxy) |
                               |      Port 8000      |
                               +---------------------+
                                          |
                +-------------------------+-------------------------+
                |                         |                         |
                v                         v                         v
     +--------------------+    +--------------------+    +--------------------+
     |   TRADING NODE 1   |    |   TRADING NODE 2   |    |   TRADING NODE 3   |
     |     Port 8001      |    |     Port 8002      |    |     Port 8003      |
     |    Priority: 1     |    |    Priority: 2     |    |    Priority: 3     |
     |   Role: BACKUP     |    |   Role: BACKUP     |    |   Role: LEADER     |
     +--------------------+    +--------------------+    +--------------------+
     | * Lamport Clock    |    | * Lamport Clock    |    | * Lamport Clock    |
     | * Matching Engine  |    | * Matching Engine  |    | * Matching Engine  |
     | * Replicated State |    | * Replicated State |    | * Primary Matcher  |
     | * SQLite Storage   |    | * SQLite Storage   |    | * SQLite Storage   |
     +--------------------+    +--------------------+    +--------------------+
                ^                         ^                         ^
                +=========================+=========================+
                      COORDINATION: Bully / Ring Election & Heartbeats
                                          |
                                          | State Persistence (data/exchange.db)
                                          v
                                  DATA EXPORT (JSONL)
                                  (data/trades.jsonl)
                                          |
                        +-----------------+-----------------+
                        |                                   |
                        v                                   v
             +---------------------+             +---------------------+
             |    APACHE SPARK     |             |      MPI (mpi4py)   |
             |     ANALYTICS       |             |     HIGH PERFORMANCE|
             |     (LAB 7)         |             |   (LABS 9 & 10)     |
             | * Volume Aggregation|             | * MPI_Bcast Config  |
             | * Top-Traded Stocks |             | * MPI_Scatter Trades|
             | * Traded Value Math |             | * Parallel Workers  |
             | * Time-Window Stats |             | * MPI_Gather Results|
             | * Spark DataFrames  |             | * Matrix Multiply   |
             +---------------------+             +---------------------+
                        |                                   |
                        +-----------------+-----------------+
                                          v
                               CONSOLIDATED MARKET REPORTS
```

---

## 2. All 10 Laboratory Experiments Mapping

| Lab | Concept / Domain | Implementation Module(s) | Description |
|:---|:---|:---|:---|
| **Lab 1** | **Client-Server Communication** | `communication/rpc_server.py`, `communication/rpc_client.py`, `communication/protocol.py` | Threaded XML-RPC server and client with standardized serialization, error codes, and remote order execution. |
| **Lab 2** | **Multithreading** | `communication/rpc_server.py` (`ThreadingMixIn`), `concurrency/thread_pool.py` | Concurrent handling of incoming RPC requests from multiple trader clients across worker threads. |
| **Lab 3** | **Clock Synchronization** | `core/clock.py` (`LamportClock`), `core/models.py` | Thread-safe Lamport logical clock maintaining LC1 (local tick) and LC2 (max receipt + 1) event ordering. |
| **Lab 4** | **Election Algorithms** | `election/bully.py`, `election/ring.py`, `election/coordinator.py` | Both Bully Algorithm (priority-based) and Ring Algorithm (token traversal) for automatic Leader election. |
| **Lab 5** | **Consistency & Replication** | `replication/primary.py`, `replication/backup.py`, `replication/replication_manager.py` | Primary-Backup replication supporting both Strong Quorum and Eventual consistency modes. |
| **Lab 6** | **Load Balancing** | `load_balancing/round_robin.py`, `load_balancing/node_health.py`, `load_balancing/load_balancer.py` | Reverse proxy distributing client order requests round-robin across healthy nodes with auto-pruning. |
| **Lab 7** | **Apache Spark Distributed Analytics** | `analytics/spark_analytics.py`, `analytics/trade_statistics.py`, `analytics/reports.py` | Distributed Spark DataFrame aggregations computing volumes, trade counts, average prices, and rankings. |
| **Lab 8** | **Fault Tolerance & Failover** | `fault_tolerance/heartbeat.py`, `fault_tolerance/failure_detector.py`, `fault_tolerance/recovery.py` | Background heartbeat daemon, threshold-based crash detection, and automated failover election. |
| **Lab 9** | **MPI Collective Communication** | `mpi/collective_communication.py` | Genuine `mpi4py` collective operations: `MPI_Bcast` (catalog), `MPI_Scatter` (trades), `MPI_Gather` (local stats). |
| **Lab 10** | **MPI Parallel Matrix Multiplication** | `mpi/matrix_multiplication.py` | Parallel matrix product $C = A \times B$ (portfolio risk exposure) distributing row dot-products across MPI workers. |

---

## 3. Directory Structure

```
Distributed Stock engine/
├── analytics/                      # Lab 7: Apache Spark Distributed Analytics
│   ├── __init__.py
│   ├── reports.py                  # Terminal dashboard and JSON report generation
│   ├── spark_analytics.py          # PySpark DataFrame pipeline
│   └── trade_statistics.py         # Data structures and metrics containers
├── client/                         # Client Applications
│   ├── __init__.py
│   └── trader_client.py            # Interactive CLI trader client
├── communication/                  # Lab 1: RPC Networking Layer
│   ├── __init__.py
│   ├── protocol.py                 # RPC method signatures and constants
│   ├── rpc_client.py               # TradingClient proxy
│   └── rpc_server.py               # Threaded XML-RPC server (ThreadingMixIn)
├── concurrency/                    # Lab 2: Multithreading Utilities
│   ├── __init__.py
│   └── thread_pool.py              # Worker thread pool helper
├── core/                           # Phase 1: Core Foundation Models & Engine
│   ├── __init__.py
│   ├── clock.py                    # Lab 3: Thread-safe Lamport logical clock
│   ├── config.py                   # Central cluster network configuration
│   ├── matching_engine.py          # Continuous double-auction limit order book
│   ├── models.py                   # Canonical Order, Trade, NodeDescriptor dataclasses
│   └── persistence.py              # SQLite storage and JSONL trade exporter
├── data/                           # Real exchange persistent state and JSONL exports
│   ├── exchange.db                 # SQLite database
│   └── trades.jsonl                # Exported transaction history for analytics
├── election/                       # Lab 4: Leader Election Algorithms
│   ├── __init__.py
│   ├── bully.py                    # Priority-based Bully algorithm
│   ├── coordinator.py              # Coordinator interface
│   └── ring.py                     # Logical ring traversal algorithm
├── fault_tolerance/                # Lab 8: Fault Detection and Recovery
│   ├── __init__.py
│   ├── failure_detector.py         # Heartbeat threshold failure monitor
│   ├── heartbeat.py                # Heartbeat probe sender
│   └── recovery.py                 # Failover and promotion manager
├── legacy/                         # Preserved original reference labs (UNTOUCHED)
├── load_balancing/                 # Lab 6: Load Balancer
│   ├── __init__.py
│   ├── load_balancer.py            # Client-facing XML-RPC reverse proxy
│   ├── node_health.py              # Background node health probe
│   └── round_robin.py              # Round-robin distribution algorithm
├── mpi/                            # Labs 9 & 10: High-Performance MPI Distributed Computing
│   ├── __init__.py
│   ├── collective_communication.py # Lab 9: MPI_Bcast, MPI_Scatter, MPI_Gather
│   └── matrix_multiplication.py    # Lab 10: Parallel matrix multiplication (C = A x B)
├── nodes/                          # Distributed Trading Node Servers
│   ├── __init__.py
│   ├── node_server.py              # Trading node CLI daemon entry point
│   └── trading_node.py             # Integrated TradingNode instance
├── scripts/                        # Operational Scripts
│   └── generate_exchange_data.py   # Real trade generator using MatchingEngine + Persistence
├── tests/                          # Automated Verification Test Suite
│   ├── test_phase1.py              # 20 tests: Models, Clock, Engine, Persistence
│   ├── test_phase2.py              # 7 tests: RPC Server/Client, Concurrency, Node
│   ├── test_phase3.py              # 8 tests: Election, Replication, Load Balancer, Failover
│   ├── test_phase4.py              # 11 tests: Spark Analytics, MPI Interfaces, Math
│   ├── test_mpi_collective.py      # Standalone mpiexec collective communication test
│   └── test_mpi_matrix.py          # Standalone mpiexec matrix multiplication test
├── requirements.txt                # Python dependencies
└── README.md                       # Master documentation
```

---

## 4. Installation & Prerequisites

### Python Dependencies
Install required packages using pip:
```bash
pip install -r requirements.txt
```
*Contents:*
- `pyspark>=4.2.0`: Distributed analytics framework
- `mpi4py>=4.1.2`: Python MPI bindings
- `numpy>=1.26.0`: Numerical validation and reference matrices

### External Runtime Prerequisites
1. **Java Runtime (for Apache Spark):**
   - Requires Java 11 or 17 (e.g. OpenJDK 17).
   - Verify: `java -version`
2. **MPI Runtime (for mpi4py):**
   - **Windows:** Install **Microsoft MPI v10+** (Download `msmpisetup.exe` and `msmpisdk.msi` from [Microsoft Download Center](https://www.microsoft.com/en-us/download/details.aspx?id=100593)).
   - **Linux:** `sudo apt-get install mpich libmpich-dev` (or `openmpi-bin libopenmpi-dev`).
   - **macOS:** `brew install mpich` (or `brew install open-mpi`).

---

## 5. Multi-Terminal Cluster Demonstration Guide

### Step 1: Start Trading Nodes (3 Terminals)
Open 3 separate terminal windows:

**Terminal 1 (Node 1 - Backup):**
```bash
python -m nodes.node_server --node-id node-1
```

**Terminal 2 (Node 2 - Backup):**
```bash
python -m nodes.node_server --node-id node-2
```

**Terminal 3 (Node 3 - Initial Leader):**
```bash
python -m nodes.node_server --node-id node-3
```

### Step 2: Start Load Balancer (Terminal 4)
```bash
python -m load_balancing.load_balancer
```
The Load Balancer listens on `http://127.0.0.1:8000/RPC2` and routes client requests round-robin across healthy nodes.

### Step 3: Run Interactive Trader Clients (Terminals 5 & 6)
**Terminal 5 (Trader Alice):**
```bash
python -m client.trader_client --url http://127.0.0.1:8000/RPC2 --trader Alice
```

**Terminal 6 (Trader Bob):**
```bash
python -m client.trader_client --url http://127.0.0.1:8000/RPC2 --trader Bob
```

Submit limit orders in the client prompt:
```
exchange> submit AAPL BUY 150.0 50
exchange> submit AAPL SELL 150.0 50
exchange> book AAPL
exchange> leader
```

### Step 4: Generate Real Exchange Trades for Analytics
You can also run the automated trade generation pipeline that submits orders across multiple stocks through the canonical engine and exports the resulting fills to `data/trades.jsonl`:
```bash
python scripts/generate_exchange_data.py
```

---

## 6. Running Distributed Analytics

### Lab 7 — Apache Spark Analytics
Executes genuine PySpark DataFrame operations on real exchange trade data:
```bash
python -m analytics.spark_analytics --input data/trades.jsonl
```
*Output Summary:*
- Market execution overview (total volume, traded value, average trade price)
- Top-traded stocks ranked by volume
- Highest-value stocks ranked by monetary value
- Symbol-by-symbol volume and price breakdown
- Time-window activity grouping

### Lab 9 — MPI Collective Communication
Executes genuine `from mpi4py import MPI` collective operations across MPI worker ranks:
```bash
mpiexec -n 4 python -m mpi.collective_communication --input data/trades.jsonl
```
*Execution Flow:*
- **MPI_Bcast:** Root (Rank 0) broadcasts exchange configuration to all ranks.
- **MPI_Scatter:** Root partitions trade records and distributes assigned chunks to workers.
- **Workers:** Each rank computes local trade count, volume, and value.
- **MPI_Gather:** Root gathers local results and prints global consolidated metrics.

### Lab 10 — MPI Parallel Matrix Multiplication
Executes parallel matrix multiplication ($C = A \times B$) distributing computation across ranks:
```bash
mpiexec -n 4 python -m mpi.matrix_multiplication --rows 8 --cols 8
```
Or run the canonical $2 \times 2$ specification test matrix:
```bash
mpiexec -n 2 python -m mpi.matrix_multiplication --demo-small
```
*Verification:*
- $A = [[1, 2], [3, 4]], B = [[5, 6], [7, 8]] \implies C = [[19, 22], [43, 50]]$
- Validates distributed computation against sequential reference arithmetic.

---

## 7. Leader Failure and Automated Recovery Demonstration (Lab 8)

1. Start all 3 nodes and load balancer.
2. In Terminal 3 (Node 3, the initial Leader), press `Ctrl+C` to simulate an abrupt crash.
3. Node 1 and Node 2 heartbeat monitors detect the failure after missed heartbeat intervals.
4. A Bully election is initiated.
5. Node 2 (having the highest remaining priority: 2 > 1) wins the election and is promoted to Primary Leader.
6. The Load Balancer detects Node 3 is down, removes it from healthy rotation, and routes new client requests to Node 2.
7. Orders submitted by Alice or Bob continue to match seamlessly.

---

## 8. Test Commands & Verification

### Run Complete Test Discovery (All 46 Tests)
```bash
python -m unittest discover -v
```

### Run Phase-Specific Test Suites
- **Phase 1 (Core Foundation — 20 tests):**
  ```bash
  python -m unittest tests/test_phase1.py -v
  ```
- **Phase 2 (RPC Runtime & Multithreading — 7 tests):**
  ```bash
  python -m unittest tests/test_phase2.py -v
  ```
- **Phase 3 (Election, Replication, Load Balancer, Failover — 8 tests):**
  ```bash
  python -m unittest tests/test_phase3.py -v
  ```
- **Phase 4 (Spark Analytics & MPI Computation — 11 tests):**
  ```bash
  python -m unittest tests/test_phase4.py -v
  ```

### Run Dedicated MPI Tests under mpiexec
```bash
mpiexec -n 2 python tests/test_mpi_collective.py
mpiexec -n 2 python tests/test_mpi_matrix.py
```

### Verify Import Integrity
```bash
python -c "from analytics.spark_analytics import *; print('Spark analytics import OK')"
python -c "from mpi.collective_communication import *; print('MPI collective import OK')"
python -c "from mpi.matrix_multiplication import *; print('MPI matrix multiplication import OK')"
```

---

## 9. Known Limitations

- **MPI OS Runtime:** `mpi4py` requires an operating-system level MPI binary library (`msmpi.dll` on Windows, `libmpich.so` on Linux). If the OS installer is not present, `from mpi4py import MPI` reports an informative diagnostic message rather than terminating the overall application.
- **Spark Local Mode:** Apache Spark runs in local multithreaded mode (`local[*]`). A standalone Hadoop daemon or cluster is not required.
