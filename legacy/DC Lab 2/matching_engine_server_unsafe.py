"""
Matching Engine Server (Lab 2 - UNSAFE version, for demonstration only)

This version makes the server multithreaded using ThreadingMixIn, so it
CAN handle multiple clients at the same time -- but it does NOT protect
the shared order_book / next_order_id with a lock.

Run the accompanying simulate_concurrent_traders.py against this server
and you will see the race condition in action: duplicate order IDs,
or orders overwriting each other's assigned ID.

DO NOT use this version for your actual submission -- it exists only to
prove why the lock in matching_engine_server.py (the fixed version) is
necessary. Include this comparison in your lab report if asked to show
"before/after".
"""

import time
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from order import format_order

HOST = "localhost"
PORT = 8000

order_book = []
next_order_id = 1


def submit_order(order: dict) -> str:
    global next_order_id

    # --- THE BUG ---
    # We read next_order_id, then (artificially, to make the race easy to
    # reproduce) pause briefly, THEN write it back. If two threads both
    # read next_order_id before either writes it back, they'll both get
    # the SAME order_id. In real life this pause isn't needed for the bug
    # to exist -- it's just easier to trigger reliably in a short demo.
    assigned_id = next_order_id
    time.sleep(0.01)  # <-- artificially widens the "race window"
    next_order_id = assigned_id + 1

    order["order_id"] = assigned_id
    order_book.append(order)

    print(f"[SERVER-UNSAFE] Received: {format_order(order)}")
    return f"Order accepted. Assigned Order ID = {assigned_id}"


def get_order_book() -> list:
    return order_book


class ThreadingXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    # daemon_threads = True lets the process exit even if a request thread
    # is still running (useful during dev/testing).
    daemon_threads = True


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ("/RPC2",)


def main():
    server = ThreadingXMLRPCServer((HOST, PORT), requestHandler=RequestHandler, allow_none=True)
    server.register_function(submit_order, "submit_order")
    server.register_function(get_order_book, "get_order_book")

    print("[SERVER-UNSAFE] Matching engine server (NO LOCK) is up.")
    print(f"[SERVER-UNSAFE] Listening on {HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
