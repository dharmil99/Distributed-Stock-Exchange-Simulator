"""
Matching Engine Server (Lab 1 - Python version)

Uses Python's built-in xmlrpc.server, which is the closest Python
equivalent to Java RMI: the client calls a method that *looks* local,
but the call, arguments, and return value are actually sent over the
network as XML-RPC messages.

Run this first:
    python3 matching_engine_server.py

Then run trader_client.py in one or more other terminals.

NOTE: Just like in the Java version, Lab 1 does NOT implement order
matching or thread-safety yet -- it only proves the client can reach
the server. Matching logic and concurrency safety are added in later
labs.
"""

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from order import format_order

HOST = "localhost"
PORT = 8000

# In-memory order book: every order the server has received.
order_book = []
next_order_id = 1


def submit_order(order: dict) -> str:
    """Remote method: client calls this to submit a new order.

    XML-RPC automatically exposes this function to remote clients once
    we register it with the server below.
    """
    global next_order_id

    order["order_id"] = next_order_id
    next_order_id += 1

    order_book.append(order)

    print(f"[SERVER] Received: {format_order(order)}")

    return f"Order accepted. Assigned Order ID = {order['order_id']}"


def get_order_book() -> list:
    """Remote method: client calls this to view all orders on the server."""
    return order_book


class RequestHandler(SimpleXMLRPCRequestHandler):
    # Restrict serving to the RPC path only (basic hygiene).
    rpc_paths = ("/RPC2",)


def main():
    server = SimpleXMLRPCServer((HOST, PORT), requestHandler=RequestHandler, allow_none=True)
    server.register_introspection_functions()

    server.register_function(submit_order, "submit_order")
    server.register_function(get_order_book, "get_order_book")

    print("[SERVER] Matching engine server is up.")
    print(f"[SERVER] Listening on {HOST}:{PORT}")
    print("[SERVER] Waiting for trader clients to connect...")

    server.serve_forever()


if __name__ == "__main__":
    main()
