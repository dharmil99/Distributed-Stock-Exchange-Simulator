"""
Trader Client (unchanged from Lab 1)

Usage (single order):
    python3 trader_client.py <host> <port> <trader_id> <BUY|SELL> <symbol> <price> <qty>

Usage (interactive mode):
    python3 trader_client.py <host> <port> <trader_id>
"""

import sys
import xmlrpc.client
from order import create_order, format_order


def connect(host: str, port: int):
    url = f"http://{host}:{port}/RPC2"
    proxy = xmlrpc.client.ServerProxy(url, allow_none=True)
    print(f"[CLIENT] Connected to matching engine at {url}")
    return proxy


def send_order(proxy, trader_id, order_type, symbol, price, quantity):
    order = create_order(trader_id, symbol, order_type, price, quantity)
    response = proxy.submit_order(order)
    print(f"[CLIENT] Server response: {response}")


def print_order_book(proxy):
    book = proxy.get_order_book()
    print(f"[CLIENT] Current order book on server ({len(book)} orders):")
    for order in book:
        print("    " + format_order(order))


def interactive_loop(proxy, trader_id):
    print("Type orders as: <BUY|SELL> <symbol> <price> <qty>   (or 'exit' to quit, 'book' to view order book)")
    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if line.lower() == "exit":
            break
        if line.lower() == "book":
            print_order_book(proxy)
            continue
        parts = line.split()
        if len(parts) != 4:
            print("Invalid format. Expected: <BUY|SELL> <symbol> <price> <qty>")
            continue
        try:
            order_type, symbol, price, qty = parts
            send_order(proxy, trader_id, order_type, symbol, float(price), int(qty))
        except Exception as e:
            print(f"Could not parse order: {e}")


def main():
    args = sys.argv[1:]

    if len(args) == 7:
        host, port, trader_id, order_type, symbol, price, qty = args
        proxy = connect(host, int(port))
        send_order(proxy, trader_id, order_type, symbol, float(price), int(qty))
        print_order_book(proxy)

    elif len(args) == 3:
        host, port, trader_id = args
        proxy = connect(host, int(port))
        interactive_loop(proxy, trader_id)

    else:
        print("Usage:")
        print("  Single order:  python3 trader_client.py <host> <port> <trader_id> <BUY|SELL> <symbol> <price> <qty>")
        print("  Interactive:   python3 trader_client.py <host> <port> <trader_id>")


if __name__ == "__main__":
    main()
