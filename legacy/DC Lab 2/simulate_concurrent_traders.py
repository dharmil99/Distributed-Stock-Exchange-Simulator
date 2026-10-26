"""
simulate_concurrent_traders.py

Spins up several "trader" threads that ALL submit orders to the server
at (roughly) the same time, then checks the results for correctness:

  - Did every submitted order get a UNIQUE order_id? (no duplicates)
  - Did the server end up with exactly (num_traders * orders_per_trader)
    orders in its order book? (no lost updates)

Run this against matching_engine_server_unsafe.py to SEE the race
condition happen (you should see duplicate order IDs / a mismatched
order book count).

Run it against matching_engine_server.py (the fixed version) to confirm
the lock prevents the problem.

Usage:
    python3 simulate_concurrent_traders.py <host> <port> [num_traders] [orders_per_trader]
    e.g. python3 simulate_concurrent_traders.py localhost 8000 10 5
"""

import sys
import threading
import xmlrpc.client
from order import create_order

# Collected across all threads -- protected by its own lock since this
# script itself uses threads to fire requests concurrently.
results_lock = threading.Lock()
assigned_ids = []


def trader_worker(host, port, trader_id, num_orders):
    # Each thread gets its OWN ServerProxy -- xmlrpc.client's ServerProxy
    # is not guaranteed thread-safe for concurrent calls on the same
    # instance, so sharing one across threads could itself introduce
    # bugs unrelated to the server. One proxy per thread avoids that.
    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}/RPC2", allow_none=True)

    for i in range(num_orders):
        order = create_order(trader_id, "STOCK_A", "BUY", 100 + i, 10)
        response = proxy.submit_order(order)
        # response looks like: "Order accepted. Assigned Order ID = 7"
        assigned_id = int(response.split("=")[-1].strip())
        with results_lock:
            assigned_ids.append(assigned_id)


def main():
    args = sys.argv[1:]
    if len(args) < 2:
        print("Usage: python3 simulate_concurrent_traders.py <host> <port> [num_traders] [orders_per_trader]")
        return

    host, port = args[0], int(args[1])
    num_traders = int(args[2]) if len(args) > 2 else 10
    orders_per_trader = int(args[3]) if len(args) > 3 else 5

    print(f"[TEST] Launching {num_traders} concurrent traders, "
          f"{orders_per_trader} orders each ({num_traders * orders_per_trader} total orders)...")

    threads = []
    for t in range(num_traders):
        trader_id = f"trader{t+1}"
        th = threading.Thread(target=trader_worker, args=(host, port, trader_id, orders_per_trader))
        threads.append(th)

    # Start all threads as close together as possible to maximize the
    # chance of overlapping requests hitting the server at the same time.
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    expected_total = num_traders * orders_per_trader
    actual_total = len(assigned_ids)
    unique_ids = len(set(assigned_ids))
    duplicates = actual_total - unique_ids

    print("\n[TEST] ---- Results ----")
    print(f"[TEST] Orders sent:            {expected_total}")
    print(f"[TEST] Responses received:     {actual_total}")
    print(f"[TEST] Unique order IDs:       {unique_ids}")
    print(f"[TEST] Duplicate order IDs:    {duplicates}")

    # Cross-check against the server's own order book.
    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}/RPC2", allow_none=True)
    book = proxy.get_order_book()
    print(f"[TEST] Orders in server's order book: {len(book)} (expected {expected_total})")

    if duplicates == 0 and len(book) == expected_total:
        print("[TEST] PASS -- no race condition detected. Thread-safety looks correct.")
    else:
        print("[TEST] FAIL -- race condition detected! Duplicate IDs or lost orders found.")


if __name__ == "__main__":
    main()
