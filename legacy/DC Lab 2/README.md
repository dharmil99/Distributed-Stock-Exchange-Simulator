# Lab 2: Multithreading in a Distributed System
### Project: Distributed Trading / Order Matching Engine

## What this demonstrates
The Lab 1 server handled one client at a time. Lab 2 makes the server
**multithreaded** so it can handle many trader clients concurrently, and
then shows why that introduces a **race condition** on shared state (the
order book / order ID counter) — and how a `threading.Lock` fixes it.

## Files
- `order.py` — unchanged from Lab 1.
- `trader_client.py` — unchanged from Lab 1 (the RPC interface didn't change).
- `matching_engine_server_unsafe.py` — multithreaded server **without** a lock.
  Exists only to demonstrate the bug — do not submit this as your final answer.
- `matching_engine_server.py` — multithreaded server **with** a lock. This is
  the correct/final version.
- `simulate_concurrent_traders.py` — a test harness that fires many orders
  from many threads at once and checks the results for duplicate order IDs
  or lost orders.

## Why a race condition happens here
In the unsafe server, `submit_order()` does roughly:
```python
assigned_id = next_order_id   # read
next_order_id = assigned_id + 1   # write
```
If Thread A reads `next_order_id` (say it's `5`), and before A writes back
`6`, Thread B *also* reads `next_order_id` (still `5`, because A hasn't
written yet) — both threads think they got order ID `5`. This is called a
**race condition**: the result depends on the unpredictable timing/interleaving
of threads.

The fixed server wraps the read-modify-write in:
```python
with order_book_lock:
    assigned_id = next_order_id
    next_order_id += 1
    order["order_id"] = assigned_id
    order_book.append(order)
```
Only one thread can be inside that `with` block at a time — every other
thread has to wait its turn — so the read-modify-write can never be
interrupted halfway.

## How to run

### 1. See the bug (unsafe server)
Terminal 1:
```bash
python3 matching_engine_server_unsafe.py
```
Terminal 2:
```bash
python3 simulate_concurrent_traders.py localhost 8000 10 5
```
Expected output: **duplicate order IDs**, e.g.
```
[TEST] Unique order IDs:       10
[TEST] Duplicate order IDs:    40
[TEST] FAIL -- race condition detected!
```
(With 10 traders × 5 orders = 50 total orders sent, but only ~10 unique IDs
handed out — most orders silently collided.)

### 2. Confirm the fix (safe server)
Stop the unsafe server (Ctrl+C), then Terminal 1:
```bash
python3 matching_engine_server.py
```
Terminal 2 (same command as before):
```bash
python3 simulate_concurrent_traders.py localhost 8000 10 5
```
Expected output: **zero duplicates**, e.g.
```
[TEST] Unique order IDs:       50
[TEST] Duplicate order IDs:    0
[TEST] PASS -- no race condition detected.
```

### 3. Normal client still works
With the safe server running, you can still use `trader_client.py` exactly
like in Lab 1:
```bash
python3 trader_client.py localhost 8000 trader1 BUY STOCK_A 101.5 10
```

## What to point out in your lab report / viva
- **ThreadingMixIn**: makes the RPC server spawn a new thread per incoming
  request instead of processing requests one at a time — this is what
  makes true concurrency possible.
- **Critical section**: the block of code that touches shared state
  (`order_book`, `next_order_id`) and must not be entered by more than one
  thread at once.
- **`threading.Lock`**: a mutual-exclusion primitive — `with lock:` ensures
  only one thread executes the block at a time; others block (wait) until
  the lock is released.
- **Why the bug is timing-dependent**: race conditions don't always show up
  — they depend on how threads happen to interleave. That's why we used a
  concurrent load test (many simultaneous requests) instead of just running
  one client at a time, and why the unsafe server has a small artificial
  `time.sleep()` to make the race reliably reproducible in a short demo.

## Next step (Lab 3)
Add logical/physical clock synchronization, so orders from different nodes
can be correctly and fairly sequenced even if their local clocks (or thread
scheduling) don't perfectly agree on arrival order.
