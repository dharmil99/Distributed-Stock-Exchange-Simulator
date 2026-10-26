"""
matching_engine_server.py -- Experiment 3

Extends the Experiment 2 thread-safe, multithreaded Matching Engine Server
with LOGICAL CLOCK SYNCHRONIZATION (Lamport clocks).

Why this is needed:
    Experiment 2 assigned order_id sequentially in server arrival order.
    That is fine when there is a single server, but it does not reflect a
    real distributed exchange, where several matching-engine replicas (or
    several traders whose requests race across the network) may disagree
    about which order "really" happened first. Wall-clock timestamps are
    unreliable for this because client machine clocks drift and are not
    synchronized (this is exactly why Experiment 3 also studies physical
    clock sync via the Berkeley algorithm -- see berkeley_time_server.py).

    Lamport's logical clock instead gives every event (submit_order call)
    a logical timestamp that respects "happens-before": if a trader saw
    the effect of order A before sending order B, then L(A) < L(B),
    regardless of any machine's physical clock.

What changed from Experiment 2:
    1. The server keeps its own LamportClock (in addition to next_order_id).
    2. On every incoming submit_order(order) call:
         - LC2: server clock is updated using the client's lamport_ts.
         - The resulting server-side Lamport value is stored on the order
           as "server_lamport_ts" and also returned to the caller, so the
           caller can update its own clock (LC2 on the response message).
    3. The in-memory order_book is always returned SORTED by
       (server_lamport_ts, trader_id) -- a full, deterministic total order
       that every node computing the same sort would agree on, instead of
       "whichever thread happened to acquire the lock first".
    4. The order_book_lock from Experiment 2 is retained: it still protects
       the read-modify-write of next_order_id / order_book AND now also
       the read-modify-write of the server's Lamport clock, since these
       are all shared state touched by many request threads at once.
"""

import threading
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

from order import format_order
from lamport_clock import LamportClock

HOST = "localhost"
PORT = 8000

# ---- Shared state, protected by order_book_lock ----
order_book = []
next_order_id = 1
server_clock = LamportClock()
order_book_lock = threading.Lock()


def submit_order(order: dict) -> dict:
    global next_order_id

    client_ts = order.get("lamport_ts")

    with order_book_lock:
        # LC2: incoming message -> update server clock using client's ts.
        # If a legacy client (Exp1/Exp2 style) sends no lamport_ts, just tick.
        if client_ts is not None:
            server_ts = server_clock.update(client_ts)
        else:
            server_ts = server_clock.tick()

        assigned_id = next_order_id
        next_order_id += 1

        order["order_id"] = assigned_id
        order["server_lamport_ts"] = server_ts
        order_book.append(order)

    print(f"[SERVER] (thread={threading.current_thread().name}) "
          f"Received: {format_order(order)} | server_clock={server_ts}")

    # The response itself is a message being sent back to the client, so it
    # carries the server's current Lamport value -- the client applies LC2
    # to it on arrival (see trader_client.py).
    return {
        "message": f"Order accepted. Assigned Order ID = {assigned_id}",
        "order_id": assigned_id,
        "server_lamport_ts": server_ts,
    }


def get_order_book() -> list:
    with order_book_lock:
        book_copy = list(order_book)
    # Deterministic total order: sort by (logical_ts, trader_id). Any
    # tie on logical_ts (concurrent, causally-unrelated events) is broken
    # consistently using trader_id, so every replica/observer computing
    # this sort over the same events reaches the SAME final ordering.
    book_copy.sort(key=lambda o: (o.get("server_lamport_ts", 0), o["trader_id"]))
    return book_copy


def get_server_clock() -> int:
    """Lets a client/test-harness peek at the server's current logical time."""
    return server_clock.value


class ThreadingXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ("/RPC2",)


def main():
    server = ThreadingXMLRPCServer((HOST, PORT), requestHandler=RequestHandler,
                                    allow_none=True)
    server.register_introspection_functions()
    server.register_function(submit_order, "submit_order")
    server.register_function(get_order_book, "get_order_book")
    server.register_function(get_server_clock, "get_server_clock")
    print("[SERVER] Matching engine server (Lamport-clock synchronized) is up.")
    print(f"[SERVER] Listening on {HOST}:{PORT}")
    print("[SERVER] Waiting for trader clients to connect...")
    server.serve_forever()


if __name__ == "__main__":
    main()
