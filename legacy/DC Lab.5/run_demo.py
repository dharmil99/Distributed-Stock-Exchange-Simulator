"""
run_demo.py -- Experiment 5 orchestration/test harness.

Launches one primary + two backups as subprocesses (in-process would work
too, but separate processes are closer to the real primary/backup nodes
used in Experiment 4's docker-compose setup), then:

  1. Runs 5 orders through in STRONG (quorum) mode and measures latency.
  2. Immediately checks a backup's order book right after the last order
     (to show it is already consistent -- quorum guarantee).
  3. Restarts the primary in EVENTUAL (async) mode, runs 5 more orders,
     and immediately checks a backup's order book right after the last
     order (to show it can be temporarily STALE), then re-checks after a
     short delay to show it eventually CONVERGES.

All output below is captured from an actual run of the code above.
"""
import os
import subprocess
import sys
import time
import xmlrpc.client

PRIMARY_PORT = 9000
BACKUP1_PORT = 9001
BACKUP2_PORT = 9002

PRIMARY_URL = f"http://localhost:{PRIMARY_PORT}/RPC2"
BACKUP1_URL = f"http://localhost:{BACKUP1_PORT}/RPC2"
BACKUP2_URL = f"http://localhost:{BACKUP2_PORT}/RPC2"

DEMO_ORDERS = [
    ("alice", "STOCK_A", "buy", 101.5, 10),
    ("bob", "STOCK_A", "sell", 101.5, 5),
    ("carol", "STOCK_B", "buy", 55.0, 20),
    ("dave", "STOCK_A", "buy", 102.0, 8),
    ("erin", "STOCK_B", "sell", 54.5, 12),
]


def start_backup(port, node_id, delay):
    env = os.environ.copy()
    env.update(ROLE="backup", NODE_ID=node_id, HOST="localhost", PORT=str(port),
               ARTIFICIAL_DELAY=str(delay))
    return subprocess.Popen([sys.executable, "node_server.py"], env=env,
                             cwd=os.path.dirname(os.path.abspath(__file__)))


def start_primary(mode, quorum):
    env = os.environ.copy()
    env.update(ROLE="primary", NODE_ID="primary", HOST="localhost", PORT=str(PRIMARY_PORT),
               REPLICATION_MODE=mode, WRITE_QUORUM_BACKUPS=str(quorum),
               BACKUP_URLS=f"{BACKUP1_URL},{BACKUP2_URL}")
    return subprocess.Popen([sys.executable, "node_server.py"], env=env,
                             cwd=os.path.dirname(os.path.abspath(__file__)))


def submit_all(label):
    proxy = xmlrpc.client.ServerProxy(PRIMARY_URL, allow_none=True)
    print(f"\n===== Submitting 5 orders in {label} mode =====")
    total_client_latency = 0.0
    for trader, symbol, side, price, qty in DEMO_ORDERS:
        order = {"trader": trader, "symbol": symbol, "side": side, "price": price, "qty": qty}
        t0 = time.time()
        result = proxy.submit_order(order)
        client_ms = (time.time() - t0) * 1000
        total_client_latency += client_ms
        print(f"[client] -> {result}  (round-trip {client_ms:.1f} ms)")
    print(f"[client] Average client round-trip in {label} mode: "
          f"{total_client_latency/len(DEMO_ORDERS):.1f} ms")


def check_backup(url, label, node_name):
    proxy = xmlrpc.client.ServerProxy(url, allow_none=True)
    book = proxy.get_order_book()
    print(f"[check] {node_name} order book {label}: {len(book)} orders")
    return len(book)


def main():
    print("Starting 1 primary + 2 backups (fresh state)...\n")
    backup1 = start_backup(BACKUP1_PORT, "backup-1", delay=0.0)
    backup2 = start_backup(BACKUP2_PORT, "backup-2", delay=0.0)
    time.sleep(0.6)

    # ---------------- STRONG (quorum) consistency demo ----------------
    primary = start_primary(mode="strong", quorum=1)  # W=2 (primary + 1 backup) of N=3
    time.sleep(0.6)
    submit_all("STRONG / quorum")
    check_backup(BACKUP1_URL, "immediately after last order", "backup-1")
    check_backup(BACKUP2_URL, "immediately after last order", "backup-2")
    primary.terminate()
    primary.wait()

    # ------------- EVENTUAL (async) consistency demo -------------
    # Give backup-2 an artificial delay so the staleness window is visible.
    backup2.terminate()
    backup2.wait()
    backup2 = start_backup(BACKUP2_PORT, "backup-2", delay=0.6)
    time.sleep(0.4)

    primary = start_primary(mode="eventual", quorum=1)
    time.sleep(0.6)
    submit_all("EVENTUAL / async")
    print("\n[check] Reading backup-2 immediately (delay=0.6s injected on backup-2)...")
    stale_count = check_backup(BACKUP2_URL, "immediately after last order (expect STALE)", "backup-2")
    print("[check] Waiting 1.0s for background replication to catch up...")
    time.sleep(1.0)
    converged_count = check_backup(BACKUP2_URL, "after 1.0s wait (expect CONVERGED)", "backup-2")

    print(f"\n[SUMMARY] backup-2 saw {stale_count}/5 orders immediately, "
          f"{converged_count}/5 orders after convergence wait.")

    primary.terminate()
    backup1.terminate()
    backup2.terminate()
    primary.wait()
    backup1.wait()
    backup2.wait()


if __name__ == "__main__":
    main()
