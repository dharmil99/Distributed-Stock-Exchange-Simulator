"""
Shared order helpers for the trading engine.

XML-RPC (Python's built-in RPC library) can only send simple types over
the wire: numbers, strings, booleans, lists, and dicts. It can NOT send
custom Python objects directly. So instead of a class, we represent an
order as a plain dict and provide helper functions to create/format it.
"""

from datetime import datetime


def create_order(trader_id: str, symbol: str, order_type: str, price: float, quantity: int) -> dict:
    """Build an order dict on the CLIENT side, before sending it to the server."""
    order_type = order_type.upper()
    if order_type not in ("BUY", "SELL"):
        raise ValueError("order_type must be 'BUY' or 'SELL'")

    return {
        "order_id": None,
        "trader_id": trader_id,
        "symbol": symbol,
        "type": order_type,
        "price": float(price),
        "quantity": int(quantity),
        "timestamp": datetime.now().isoformat(),
    }


def format_order(order: dict) -> str:
    """Human-readable representation, used for logging/printing."""
    return (
        f"Order#{order['order_id']} [{order['trader_id']}] "
        f"{order['type']} {order['quantity']} {order['symbol']} "
        f"@ {order['price']:.2f} (received {order['timestamp']})"
    )
