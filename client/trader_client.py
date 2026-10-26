"""
Trader CLI Client for Distributed Stock Exchange Simulator.

Connects to a TradingNode over XML-RPC.
Supports single-order command submission and interactive session REPL.

Usage (Interactive):
  python -m client.trader_client
  python -m client.trader_client --url http://127.0.0.1:8001/RPC2 --trader Alice

Usage (Single Order):
  python -m client.trader_client http://127.0.0.1:8001/RPC2 Alice BUY AAPL 150 100
"""

import argparse
import sys
from typing import List

from communication.rpc_client import TradingClient


def format_order_line(o: dict) -> str:
    """Format single order line for display."""
    oid = o.get("order_id", "-")
    trader = o.get("trader_id", "-")
    side = o.get("side", "-")
    qty = o.get("quantity", 0)
    rem = o.get("remaining_quantity", qty)
    sym = o.get("symbol", "-")
    price = o.get("price", 0.0)
    status = o.get("status", "-")
    return f"Order#{oid:<3} [{trader:<8}] {side:<4} {rem}/{qty} {sym:<5} @ ${price:<7.2f} [{status}]"


def format_trade_line(t: dict) -> str:
    """Format single trade execution line for display."""
    tid = t.get("trade_id", "-")
    sym = t.get("symbol", "-")
    qty = t.get("quantity", 0)
    price = t.get("price", 0.0)
    b = t.get("buyer_id", "-")
    s = t.get("seller_id", "-")
    return f"Trade#{tid:<3} {qty} {sym} @ ${price:.2f} (Buyer: {b} | Seller: {s})"


def run_interactive(client: TradingClient, default_trader: str):
    print("=" * 65)
    print("  DISTRIBUTED STOCK EXCHANGE — TRADER TERMINAL")
    print(f"  Connected to: {client.node_url}")
    print(f"  Trader ID:    {default_trader}")
    print("=" * 65)
    print("Commands:")
    print("  <BUY|SELL> <SYMBOL> <PRICE> <QUANTITY>   Submit limit order")
    print("  book <SYMBOL>                            View active order book")
    print("  depth <SYMBOL>                           View price-level depth")
    print("  orders                                   View all orders on node")
    print("  trades                                   View all executed trades")
    print("  cancel <SYMBOL> <ORDER_ID>               Cancel resting order")
    print("  info                                     View node info")
    print("  time                                     View node logical time")
    print("  exit                                     Quit")
    print("=" * 65)

    while True:
        try:
            line = input(f"[{default_trader}] > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not line:
            continue
        if line.lower() in ("exit", "quit"):
            print("Session ended.")
            break

        parts = line.split()
        cmd = parts[0].upper()

        try:
            if cmd in ("BUY", "SELL"):
                if len(parts) != 4:
                    print("Usage: <BUY|SELL> <SYMBOL> <PRICE> <QUANTITY>")
                    continue
                side, symbol, price, qty = parts[0].upper(), parts[1].upper(), float(parts[2]), int(parts[3])
                res = client.submit_order(default_trader, symbol, side, price, qty)
                
                order_info = res.get("order", {})
                server_ts = res.get("server_lamport_ts")
                print(f"[ORDER ACCEPTED] {format_order_line(order_info)} (server_lamport_ts={server_ts})")
                
                trades = res.get("trades", [])
                if trades:
                    print(f"*** {len(trades)} TRADE(S) EXECUTED ***")
                    for t in trades:
                        print("  -> " + format_trade_line(t))
                else:
                    print("  (Order is resting in order book)")

            elif cmd == "BOOK":
                symbol = parts[1].upper() if len(parts) > 1 else "AAPL"
                res = client.get_order_book(symbol)
                book = res.get("order_book", {})
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                print(f"\n--- Order Book for {symbol} ({len(bids)} bids, {len(asks)} asks) ---")
                print("ASKS (Sellers):")
                for a in asks:
                    print("  " + format_order_line(a))
                if not asks:
                    print("  (None)")
                print("BIDS (Buyers):")
                for b in bids:
                    print("  " + format_order_line(b))
                if not bids:
                    print("  (None)")
                print("-" * 50)

            elif cmd == "DEPTH":
                symbol = parts[1].upper() if len(parts) > 1 else "AAPL"
                res = client.get_depth(symbol)
                depth = res.get("depth", {})
                print(f"\n--- Market Depth for {symbol} ---")
                print("Price Levels (Asks):")
                for a in depth.get("asks", []):
                    print(f"  ${a['price']:.2f} : {a['quantity']} shares")
                print("Price Levels (Bids):")
                for b in depth.get("bids", []):
                    print(f"  ${b['price']:.2f} : {b['quantity']} shares")
                print("-" * 50)

            elif cmd == "ORDERS":
                symbol = parts[1].upper() if len(parts) > 1 else None
                res = client.get_all_orders(symbol)
                orders = res.get("orders", [])
                print(f"\n--- Historical Orders on Node ({len(orders)} total) ---")
                for o in orders:
                    print("  " + format_order_line(o))
                print("-" * 50)

            elif cmd == "TRADES":
                symbol = parts[1].upper() if len(parts) > 1 else None
                res = client.get_all_trades(symbol)
                trades = res.get("trades", [])
                print(f"\n--- Executed Trades on Node ({len(trades)} total) ---")
                for t in trades:
                    print("  " + format_trade_line(t))
                print("-" * 50)

            elif cmd == "CANCEL":
                if len(parts) != 3:
                    print("Usage: cancel <SYMBOL> <ORDER_ID>")
                    continue
                symbol, oid = parts[1].upper(), int(parts[2])
                res = client.cancel_order(symbol, oid)
                if res.get("status") == "success":
                    print(f"[CANCELLED] Order #{oid} cancelled.")
                else:
                    print(f"[FAILED] {res.get('error', 'Could not cancel order')}")

            elif cmd == "INFO":
                res = client.get_node_info()
                node = res.get("node", {})
                print(f"[NODE INFO] ID={node.get('node_id')} Role={node.get('role')} "
                      f"Endpoint=http://{node.get('host')}:{node.get('port')}/RPC2 "
                      f"Clock={res.get('server_lamport_ts')}")

            elif cmd == "TIME":
                res = client.get_lamport_time()
                print(f"[CLOCK] Server Lamport time = {res.get('lamport_ts')}, "
                      f"Client Lamport time = {client.clock.read()}")

            elif cmd == "LEADER":
                res = client.get_current_leader()
                leader = res.get("leader", {})
                if leader:
                    print(f"[CURRENT LEADER] ID={leader.get('node_id')} Priority={leader.get('priority')} Port={leader.get('port')}")
                else:
                    print(f"[LEADER] {res.get('error', 'No active leader discovered.')}")

            elif cmd == "CLUSTER":
                res = client.get_cluster_status()
                cluster = res.get("cluster", {})
                print("\n--- Cluster Status ---")
                for nid, info in cluster.items():
                    status_str = "ALIVE" if info.get("is_alive") else "DOWN"
                    print(f"  Node {nid:<8}: [{status_str:<5}] Role={info.get('role'):<7} Port={info.get('port')}")
                print("-" * 50)

            elif cmd == "ELECT":
                print("[ELECTION] Initiating leader election...")
                res = client.start_election()
                print(f"[ELECTION RESULT] -> {res}")

            else:
                print(f"Unknown command: '{parts[0]}'. Type 'exit' to quit.")

        except Exception as e:
            print(f"[ERROR] Request failed: {e}")


def main():
    args = sys.argv[1:]

    # Check for direct order arguments: <url> <trader> <BUY|SELL> <symbol> <price> <qty>
    if len(args) == 6:
        url, trader, side, symbol, price, qty = args
        client = TradingClient(url)
        res = client.submit_order(trader, symbol, side, float(price), int(qty))
        print(f"[RESPONSE] {res}")
        return

    parser = argparse.ArgumentParser(description="Trader CLI Client")
    parser.add_argument("--url", default="http://127.0.0.1:8001/RPC2", help="Node RPC URL")
    parser.add_argument("--trader", default="trader1", help="Trader Identifier")
    parsed = parser.parse_args(args)

    client = TradingClient(parsed.url)
    run_interactive(client, parsed.trader)


if __name__ == "__main__":
    main()
