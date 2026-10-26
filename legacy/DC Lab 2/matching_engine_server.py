"""
Matching Engine Server (Lab 2 - thread-safe version)

Two changes from Lab 1:

1. ThreadingMixIn: makes the server handle each incoming client request
   on its own thread, instead of one at a time. This is what lets
   multiple trader clients be served concurrently.

2. threading.Lock: protects the shared order_book list and the
   next_order_id counter. Only one thread at a time is allowed inside
   the "critical section" (between acquiring and releasing the lock),
   which prevents two threads from both grabbing the same order_id or
   corrupting the order_book list.

Run this first:
    python3 matching_engine_server.py

Then run multiple trader_client.py instances (or simulate_concurrent_traders.py)
against it from separate terminals -- they will now be served concurrently
and safely.
"""

import threading
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from order import format_order

HOST = "localhost"
PORT = 8000

# Shared state -- accessed by many threads at once, so it must be
# protected by a lock whenever it's read or written.
order_book = []
next_order_id = 1

# One lock shared by every request thread. Whoever calls lock.acquire()
# (via the "with lock:" block below) first gets exclusive access; every
# other thread that tries to enter the block has to wait its turn.
order_book_lock = threading.Lock()


def submit_order(order: dict) -> str:
    global next_order_id

    # Everything inside this "with" block is now a critical section --
    # only one thread can be executing it at any given moment, even
    # though many threads may be running submit_order() concurrently.
    with order_book_lock:
        assigned_id = next_order_id
        next_order_id += 1
        order["order_id"] = assigned_id
        order_book.append(order)

    # Logging can safely happen outside the lock -- it doesn't touch
    # shared state, so there's no need to block other threads for it.
    print(f"[SERVER] Received (thread={threading.current_thread().name}): {format_order(order)}")

    return f"Order accepted. Assigned Order ID = {assigned_id}"


def get_order_book() -> list:
    # Reads should also be protected, so a reader never sees the list
    # mid-update (e.g. while append() is partially executing).
    with order_book_lock:
        return list(order_book)


class ThreadingXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ("/RPC2",)


def main():
    server = ThreadingXMLRPCServer((HOST, PORT), requestHandler=RequestHandler, allow_none=True)
    server.register_introspection_functions()
    server.register_function(submit_order, "submit_order")
    server.register_function(get_order_book, "get_order_book")

    print("[SERVER] Matching engine server (multithreaded, thread-safe) is up.")
    print(f"[SERVER] Listening on {HOST}:{PORT}")
    print("[SERVER] Waiting for trader clients to connect...")

    server.serve_forever()


if __name__ == "__main__":
    main()
