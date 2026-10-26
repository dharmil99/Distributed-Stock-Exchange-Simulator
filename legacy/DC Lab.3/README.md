# Experiment 3 — Clock Synchronization (Logical & Physical)

Distributed Stock Exchange Simulator — extends Experiment 1 (XML-RPC
client-server) and Experiment 2 (multithreading + locks).

## Aim
To implement clock synchronization — both **logical** (Lamport's algorithm)
and **physical** (Berkeley's algorithm) — in the Distributed Stock Exchange
Simulator, so that events (orders) across independent traders/nodes can be
placed in a consistent, agreed-upon order despite each node having its own
local clock.

## Files

| File | Purpose |
|---|---|
| `order.py` | Builds/formats the order dict; now carries a `lamport_ts` field. |
| `lamport_clock.py` | `LamportClock` class implementing Lamport's logical clock (tick / update). |
| `matching_engine_server.py` | Multithreaded, thread-safe XML-RPC server (from Exp 2) extended with a server-side Lamport clock. Assigns every order a `server_lamport_ts` and returns the order book in a deterministic total order. |
| `trader_client.py` | Single-order trader client; keeps its own `LamportClock`, ticks before sending, updates on receiving the server's reply. |
| `simulate_lamport_traders.py` | Concurrent load-test harness: many trader threads, each with an independent Lamport clock, hammer the server at once. Verifies (a) no duplicate/lost orders (thread-safety, Exp 2) and (b) the returned order book is totally ordered by `server_lamport_ts` (logical clock sync, Exp 3). |
| `berkeley_clock_sync.py` | Standalone simulation of the **Berkeley algorithm** for physical clock synchronization: several nodes with drifted clocks are polled by a coordinator, which computes a fault-tolerant average and pushes back signed corrections. Prints clock spread before/after. |

## Key Concepts Demonstrated
- **Logical clocks (Lamport):** `C = C + 1` before send/local event; `C = max(C, C_received) + 1` on receive. Guarantees "happens-before" consistency without relying on wall-clock time.
- **Total ordering from logical clocks:** ties on the same logical timestamp are broken by a fixed rule (`trader_id`), so every observer computing the same sort agrees on the same final order.
- **Physical clocks (Berkeley algorithm):** a coordinator polls all nodes, delay-compensates the readings, computes an average, and sends each node a signed offset to converge toward — used when there is no external time reference (no GPS/NTP).
- **Why both are needed:** logical clocks order *events*; physical clocks synchronize *wall-clock time* itself. A real distributed matching-engine cluster typically needs both.

## Execution Procedure

### Part A — Logical clock sync (Lamport) over XML-RPC
```
Terminal 1: python3 matching_engine_server.py
Terminal 2: python3 simulate_lamport_traders.py localhost 8000 10 5
```
Expected: 0 duplicate order IDs, order book has exactly (traders × orders) entries, and
`order_book is sorted by server_lamport_ts: True`.

Optional — single client, same as Exp 1/2:
```
python3 trader_client.py localhost 8000 trader1 BUY STOCK_A 101.5 10
```
Each printed order shows `Lc=<client's own Lamport clock>` and
`Ls=<server-assigned Lamport clock>` side by side — note `Lc` values repeat
across different traders (each trader's clock is independent), while `Ls`
values are globally unique and increasing (the server's clock provides the
single agreed order).

### Part B — Physical clock sync (Berkeley algorithm), simulation only
```
python3 berkeley_clock_sync.py
```
Expected: "Max spread between nodes BEFORE sync" is large (clocks were
manually drifted apart in the simulation), and "Max spread ... AFTER sync"
is much smaller (all clocks converged toward the coordinator's computed
average), ending in `PASS`.

## Distributed Computing Concepts Demonstrated
- Concurrency & mutual exclusion (carried over from Exp 2, still required since
  the Lamport clock and order book are shared state).
- Happens-before relation and logical time (Lamport, 1978).
- Total order derived from a partial order via a deterministic tie-break.
- Fault-tolerant averaging and offset-based correction (Berkeley algorithm).
- The distinction between *ordering events* (logical clocks) and
  *synchronizing wall-clock time* (physical clocks).

## Limitations
- Only one matching-engine server instance runs the Lamport clock in this
  demo; a fully replicated setup would need each replica to run its own
  logical clock and exchange timestamps with the others directly.
- The Berkeley algorithm demo is simulated in-process (no real network calls)
  to make the clock drift and correction reproducible; it is not wired into
  the XML-RPC server in this version.
- Vector clocks (which capture full causal dependency between processes,
  not just a single scalar) are not implemented here — a natural extension
  for a future lab.

## References
- L. Lamport, "Time, Clocks, and the Ordering of Events in a Distributed
  System," *Communications of the ACM*, vol. 21, no. 7, pp. 558-565, 1978.
- R. Gusella and S. Zatti, "The Accuracy of the Clock Synchronization
  Achieved by TEMPO in Berkeley UNIX 4.3BSD," *IEEE Transactions on Software
  Engineering*, 1989.
- A. S. Tanenbaum and M. van Steen, *Distributed Systems: Principles and
  Paradigms*, 3rd ed., Pearson, 2017.
- G. Coulouris, J. Dollimore, T. Kindberg, and G. Blair, *Distributed
  Systems: Concepts and Design*, 5th ed., Addison-Wesley, 2011.
