from datetime import datetime


def create_order(trader_id: str, stock: str, side: str, price: float, quantity: int,
                  lamport_ts: int = None) -> dict:
    """
    Builds the order dictionary that is sent to the matching engine server over
    XML-RPC. Kept as a plain dict (not a class) so it serializes cleanly through
    xmlrpc.client / xmlrpc.server.

    New in Experiment 3: each order now optionally carries the trader's current
    Lamport logical-clock value (lamport_ts) at the moment the order was sent.
    The server uses this, together with its own clock, to establish a causally
    consistent total order of events across the distributed system -- something
    wall-clock timestamps alone cannot guarantee (Experiment 1 & 2 relied only
    on server arrival order).
    """
    return {
        "trader_id": trader_id,
        "stock": stock.upper(),
        "side": side.upper(),  # "BUY" or "SELL"
        "price": float(price),
        "quantity": int(quantity),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "lamport_ts": lamport_ts,  # set by the client's LamportClock before sending
        # order_id is intentionally NOT set here -- it is assigned by
        # the server, inside the critical section, once the order
        # is actually accepted.
    }


def format_order(order: dict) -> str:
    """
    Human-readable one-line summary of an order, used for logging.

    Shows BOTH clocks side by side so the difference is obvious:
      Lc = the CLIENT's own local Lamport value at send time (only
           meaningful relative to that one client's own events).
      Ls = the SERVER's Lamport value assigned to this event (LC2:
           max(server_clock, Lc)+1) -- this is the value used to build
           the single, globally agreed total order across ALL clients.
    """
    oid = order.get("order_id", "PENDING")
    lc = order.get("lamport_ts", "-")
    ls = order.get("server_lamport_ts", "-")
    return (f"Order#{oid} | Lc={lc} Ls={ls} | {order['trader_id']} | {order['side']} "
            f"{order['quantity']} {order['stock']} @ {order['price']}")
