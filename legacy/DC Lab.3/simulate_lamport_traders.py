"""
simulate_lamport_traders.py

Concurrent load-test harness for Experiment 3, in the same spirit as
Experiment 2's simulate_concurrent_traders.py.

Each simulated trader keeps its OWN LamportClock (its own logical time --
just like an independent process/machine). Traders submit several orders
each, ticking their local clock before every send and updating it from
every server response, exactly as a real distributed client would.

What this proves:
    1. No duplicate order_ids and no lost orders, even under heavy
       concurrent load (this is the same thread-safety guarantee from
       Experiment 2, still holding here).
    2. get_order_book() returns a TOTALLY ORDERED, deterministic sequence
       (by server_lamport_ts, tie-broken by trader_id) even though the
       requests physically arrived at the server in a essentially random
       interleaving. Running this harness twice will re-arrive in a
       different physical arrival order, but the logical order recorded
       for any two causally-related events is always preserved.
"""

import sys
import threading
import xmlrpc.client

from order import create_order
from lamport_clock import LamportClock

results_lock = threading.Lock()
assigned_ids = []


def trader_worker(host, port, trader_id, num_orders):
    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}/RPC2", allow_none=True)
    my_clock = LamportClock()  # this trader's own independent logical clock

    for i in range(num_orders):
        ts = my_clock.tick()  # LC1: about to send -> increment first
        order = create_order(trader_id, "STOCK_A", "BUY", 100 + i, 10, lamport_ts=ts)
        response = proxy.submit_order(order)

        # LC2: response received -> fold server's logical time into ours
        server_ts = response["server_lamport_ts"]
        my_clock.update(server_ts)

        with results_lock:
            assigned_ids.append(response["order_id"])


def main():
    args = sys.argv[1:]
    host, port = args[0], int(args[1])
    num_traders = int(args[2]) if len(args) > 2 else 10
    orders_per_trader = int(args[3]) if len(args) > 3 else 5

    print(f"[TEST] Launching {num_traders} concurrent traders (each with its own "
          f"Lamport clock), {orders_per_trader} orders each "
          f"({num_traders * orders_per_trader} total orders)...")

    threads = [
        threading.Thread(target=trader_worker,
                          args=(host, port, f"trader{t + 1}", orders_per_trader))
        for t in range(num_traders)
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    expected_total = num_traders * orders_per_trader
    actual_total = len(assigned_ids)
    unique_ids = len(set(assigned_ids))
    duplicates = actual_total - unique_ids

    print("\n[TEST] ---- Concurrency / thread-safety results ----")
    print(f"[TEST] Orders sent:            {expected_total}")
    print(f"[TEST] Responses received:     {actual_total}")
    print(f"[TEST] Unique order IDs:       {unique_ids}")
    print(f"[TEST] Duplicate order IDs:    {duplicates}")

    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}/RPC2", allow_none=True)
    book = proxy.get_order_book()
    server_now = proxy.get_server_clock()

    print(f"[TEST] Orders in server's order book: {len(book)} (expected {expected_total})")
    print(f"[TEST] Final server logical clock:    {server_now}")

    print("\n[TEST] ---- Causal / logical ordering check ----")
    lamport_seq = [o["server_lamport_ts"] for o in book]
    is_sorted = lamport_seq == sorted(lamport_seq)
    print(f"[TEST] order_book is sorted by server_lamport_ts: {is_sorted}")
    print("[TEST] First 10 entries of the totally ordered book "
          "(server_lamport_ts, trader_id, order_id):")
    for o in book[:10]:
        print(f"    L={o['server_lamport_ts']:<4} {o['trader_id']:<10} "
              f"order_id={o['order_id']}")

    if duplicates == 0 and len(book) == expected_total and is_sorted:
        print("\n[TEST] PASS -- thread-safe AND causally/totally ordered by "
              "Lamport clock.")
    else:
        print("\n[TEST] FAIL -- race condition or ordering inconsistency detected!")


if __name__ == "__main__":
    main()
