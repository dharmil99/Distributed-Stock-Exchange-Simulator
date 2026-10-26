import sys
import xmlrpc.client

from order import create_order, format_order
from lamport_clock import LamportClock

# Every trader process keeps its own logical clock. It is local state --
# it is NOT shared with the server directly, it is only ever sent as a
# number inside the order payload (LC1: increment-then-send).
clock = LamportClock()


def main():
    args = sys.argv[1:]
    if len(args) < 6:
        print("Usage: python trader_client.py <host> <port> <trader_id> "
              "<BUY|SELL> <stock> <price> [quantity]")
        sys.exit(1)

    host, port, trader_id, side, stock, price = args[:6]
    quantity = int(args[6]) if len(args) > 6 else 10

    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}/RPC2", allow_none=True)

    # LC1: this client is about to send a message -> tick the clock first.
    ts = clock.tick()
    order = create_order(trader_id, stock, side, float(price), quantity, lamport_ts=ts)

    print(f"[CLIENT:{trader_id}] Local Lamport clock -> {ts}")
    print(f"[CLIENT:{trader_id}] Connecting to matching engine at {host}:{port}")
    print(f"[CLIENT:{trader_id}] Submitting: {format_order(order)}")

    response = proxy.submit_order(order)

    # LC2: the RPC response is itself an incoming message carrying the
    # server's Lamport timestamp -- update our clock so causality is
    # preserved for whatever this trader does next.
    server_ts = response.get("server_lamport_ts")
    if server_ts is not None:
        new_val = clock.update(server_ts)
        print(f"[CLIENT:{trader_id}] Received server L={server_ts} -> "
              f"clock updated to {new_val}")

    print(f"[CLIENT:{trader_id}] Server response: {response}")
    print(f"[CLIENT:{trader_id}] Current order book (causally ordered):")
    for o in proxy.get_order_book():
        print(f"    {format_order(o)}")


if __name__ == "__main__":
    main()
