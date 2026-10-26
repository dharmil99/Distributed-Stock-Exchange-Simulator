"""
node_server.py -- Experiment 5 (Data Consistency & Replication Models)

Extends the Experiment 4 primary-backup setup to THREE replicas (one
primary + two backups) and makes the replication strategy configurable,
so the same code can demonstrate two different consistency models:

  REPLICATION_MODE=strong    -> Synchronous / quorum-based replication.
                                 The primary blocks until it has collected
                                 acknowledgements from a write quorum (W)
                                 of replicas before confirming the order
                                 to the client. With N=3 total replicas
                                 and W=2 (primary + 1 backup), any future
                                 read quorum R=2 is guaranteed to overlap
                                 the write quorum (W + R > N), so a
                                 majority read can never return stale
                                 data -- this is STRONG consistency.

  REPLICATION_MODE=eventual  -> Asynchronous replication. The primary
                                 accepts the order locally, replies to
                                 the client immediately, and replicates
                                 to the backups in the background. Reads
                                 issued against a backup immediately
                                 after may be stale until replication
                                 completes -- this is EVENTUAL
                                 consistency, traded for lower latency.

Both modes reuse the Lamport clock from Experiment 3 to timestamp and
order events, and the accept/replicate/heartbeat vocabulary from
Experiment 4's primary-backup fault tolerance demo.
"""
import os
import sys
import time
import threading
import xmlrpc.client
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

from common import LamportClock

ROLE = os.environ.get("ROLE", "primary")            # "primary" | "backup"
NODE_ID = os.environ.get("NODE_ID", ROLE)
HOST = os.environ.get("HOST", "localhost")
PORT = int(os.environ.get("PORT", "9000"))

# Only meaningful on the primary:
REPLICATION_MODE = os.environ.get("REPLICATION_MODE", "eventual")  # "strong" | "eventual"
BACKUP_URLS = [u for u in os.environ.get("BACKUP_URLS", "").split(",") if u]
WRITE_QUORUM_BACKUPS = int(os.environ.get("WRITE_QUORUM_BACKUPS", "1"))  # backup ACKs needed for W=2 (N=3)

# Only meaningful on a backup: simulates network/processing latency so the
# staleness window of eventual-consistency mode is actually observable.
ARTIFICIAL_DELAY = float(os.environ.get("ARTIFICIAL_DELAY", "0"))

order_book = []
next_order_id = 1
clock = LamportClock()
state_lock = threading.Lock()


def submit_order(order: dict) -> dict:
    """Entry point for clients. Only the primary accepts new orders."""
    global next_order_id
    if ROLE != "primary":
        return {"status": "error", "message": f"{NODE_ID} is not primary"}

    with state_lock:
        ts = clock.tick()
        order["order_id"] = next_order_id
        order["server_lamport_ts"] = ts
        next_order_id += 1
        order_book.append(order)
    print(f"[{NODE_ID}] accepted order #{order['order_id']} @ clock={ts} "
          f"(mode={REPLICATION_MODE})")

    acked = {"count": 0}
    ack_lock = threading.Lock()

    def replicate_to(url):
        try:
            proxy = xmlrpc.client.ServerProxy(url, allow_none=True)
            proxy.replicate_order(order)
            with ack_lock:
                acked["count"] += 1
            print(f"[{NODE_ID}] ACK from backup {url} for order #{order['order_id']}")
        except Exception as e:
            print(f"[{NODE_ID}] replication to {url} FAILED: {e}")

    threads = [threading.Thread(target=replicate_to, args=(u,), daemon=True)
               for u in BACKUP_URLS]

    if REPLICATION_MODE == "strong":
        start = time.time()
        for t in threads:
            t.start()
        # Block until write-quorum of backups have ACKed (or all threads
        # finished, e.g. because a backup is genuinely down).
        while acked["count"] < WRITE_QUORUM_BACKUPS and any(t.is_alive() for t in threads):
            time.sleep(0.002)
        elapsed_ms = round((time.time() - start) * 1000, 1)
        quorum_met = acked["count"] >= WRITE_QUORUM_BACKUPS
        print(f"[{NODE_ID}] STRONG: quorum {acked['count']}/{WRITE_QUORUM_BACKUPS} "
              f"backup ACKs in {elapsed_ms} ms (quorum_met={quorum_met})")
        return {
            "status": "accepted", "order_id": order["order_id"], "mode": "strong",
            "backup_acks": acked["count"], "quorum_required": WRITE_QUORUM_BACKUPS,
            "quorum_met": quorum_met, "latency_ms": elapsed_ms,
        }
    else:
        # Eventual: reply immediately, replicate in the background.
        for t in threads:
            t.start()
        return {
            "status": "accepted", "order_id": order["order_id"], "mode": "eventual",
            "latency_ms": 0.0,
        }


def replicate_order(order: dict) -> dict:
    """Called on a backup by the primary to push a committed order."""
    if ARTIFICIAL_DELAY > 0:
        time.sleep(ARTIFICIAL_DELAY)
    with state_lock:
        clock.update(order["server_lamport_ts"])
        order_book.append(order)
    print(f"[{NODE_ID}] replicated order #{order['order_id']}")
    return {"status": "replicated", "order_id": order["order_id"]}


def get_order_book() -> list:
    with state_lock:
        return list(order_book)


def get_status() -> dict:
    with state_lock:
        return {"role": ROLE, "node_id": NODE_ID, "order_count": len(order_book),
                "clock": clock.value}


class ThreadingXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ("/RPC2",)


def main():
    server = ThreadingXMLRPCServer((HOST, PORT), requestHandler=RequestHandler,
                                    allow_none=True)
    server.register_function(submit_order, "submit_order")
    server.register_function(replicate_order, "replicate_order")
    server.register_function(get_order_book, "get_order_book")
    server.register_function(get_status, "get_status")
    print(f"[{NODE_ID}] node up as {ROLE} on {HOST}:{PORT}"
          + (f" (mode={REPLICATION_MODE}, quorum={WRITE_QUORUM_BACKUPS})" if ROLE == "primary" else ""))
    sys.stdout.flush()
    server.serve_forever()


if __name__ == "__main__":
    main()
