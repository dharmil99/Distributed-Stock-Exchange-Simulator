"""
Generate Real Exchange Trades for Analytics (Lab 7, 9, 10).

Executes genuine order submissions and continuous double-auction matching
using canonical core.matching_engine, core.models, and core.persistence.
Exports matched trades to data/trades.jsonl for Spark and MPI analytics.
"""

import os
import sys
from datetime import datetime, timezone, timedelta

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.models import Order, OrderSide
from core.matching_engine import MatchingEngine
from core.persistence import SQLitePersistence
from core.clock import LamportClock


def generate_trades(output_jsonl: str = "data/trades.jsonl", db_path: str = "data/exchange.db"):
    os.makedirs(os.path.dirname(os.path.abspath(output_jsonl)), exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    persistence = SQLitePersistence(db_path)
    engine = MatchingEngine()
    clock = LamportClock()

    base_time = datetime(2026, 10, 26, 9, 30, 0, tzinfo=timezone.utc)

    # Realistic order sequences across 5 major stocks
    raw_orders = [
        # AAPL trading session
        ("Alice", "AAPL", OrderSide.SELL, 150.0, 100, 0),
        ("Bob", "AAPL", OrderSide.BUY, 150.0, 50, 2),
        ("Charlie", "AAPL", OrderSide.BUY, 151.0, 70, 5),
        ("Diana", "AAPL", OrderSide.SELL, 151.0, 40, 8),
        ("Eve", "AAPL", OrderSide.BUY, 152.0, 120, 12),
        ("Alice", "AAPL", OrderSide.SELL, 152.0, 80, 15),
        ("Bob", "AAPL", OrderSide.SELL, 153.0, 100, 20),
        ("Charlie", "AAPL", OrderSide.BUY, 153.0, 60, 25),

        # TSLA trading session
        ("Bob", "TSLA", OrderSide.SELL, 210.0, 80, 30),
        ("Diana", "TSLA", OrderSide.BUY, 210.0, 50, 32),
        ("Alice", "TSLA", OrderSide.BUY, 212.0, 60, 35),
        ("Charlie", "TSLA", OrderSide.SELL, 211.0, 40, 38),
        ("Eve", "TSLA", OrderSide.BUY, 215.0, 100, 42),
        ("Bob", "TSLA", OrderSide.SELL, 213.0, 70, 45),

        # MSFT trading session
        ("Charlie", "MSFT", OrderSide.SELL, 320.0, 50, 50),
        ("Alice", "MSFT", OrderSide.BUY, 320.0, 50, 52),
        ("Diana", "MSFT", OrderSide.SELL, 322.0, 60, 55),
        ("Eve", "MSFT", OrderSide.BUY, 325.0, 80, 58),
        ("Bob", "MSFT", OrderSide.SELL, 323.0, 40, 62),

        # GOOGL trading session
        ("Alice", "GOOGL", OrderSide.SELL, 140.0, 100, 70),
        ("Bob", "GOOGL", OrderSide.BUY, 140.0, 80, 72),
        ("Charlie", "GOOGL", OrderSide.BUY, 142.0, 50, 75),
        ("Diana", "GOOGL", OrderSide.SELL, 141.0, 30, 80),
        ("Eve", "GOOGL", OrderSide.BUY, 143.0, 90, 85),
        ("Bob", "GOOGL", OrderSide.SELL, 142.5, 60, 90),

        # AMZN trading session
        ("Diana", "AMZN", OrderSide.SELL, 175.0, 70, 100),
        ("Alice", "AMZN", OrderSide.BUY, 175.0, 40, 105),
        ("Eve", "AMZN", OrderSide.BUY, 176.0, 50, 110),
        ("Charlie", "AMZN", OrderSide.SELL, 176.0, 60, 115),
        ("Bob", "AMZN", OrderSide.BUY, 178.0, 80, 120),
        ("Diana", "AMZN", OrderSide.SELL, 177.0, 50, 125),
    ]

    total_trades_count = 0
    total_volume_count = 0
    total_value_count = 0.0

    print(f"[*] Processing {len(raw_orders)} orders through canonical MatchingEngine...")

    for trader, symbol, side, price, qty, offset_secs in raw_orders:
        ts_str = (base_time + timedelta(seconds=offset_secs)).isoformat()
        lamport_ts = clock.increment()

        order = Order(
            trader_id=trader,
            symbol=symbol,
            side=side,
            price=price,
            quantity=qty,
            timestamp=ts_str,
            client_lamport_ts=lamport_ts,
            server_lamport_ts=lamport_ts,
        )

        processed_order, trades = engine.submit_order(order, lamport_ts=lamport_ts)
        persistence.save_order(processed_order)

        for trade in trades:
            persistence.save_trade(trade)
            total_trades_count += 1
            total_volume_count += trade.quantity
            total_value_count += trade.quantity * trade.price

            # Save updated resting counterparty
            counterparty_id = (
                trade.sell_order_id if processed_order.side == "BUY"
                else trade.buy_order_id
            )
            cp = engine.get_order(counterparty_id)
            if cp:
                persistence.save_order(cp)

    exported_count = persistence.export_trades_jsonl(output_jsonl)
    persistence.close()

    print(f"[+] Execution complete: {total_trades_count} real trades generated.")
    print(f"[+] Total volume: {total_volume_count:,} shares across AAPL, TSLA, MSFT, GOOGL, AMZN.")
    print(f"[+] Total value: ${total_value_count:,.2f}.")
    print(f"[+] Exported {exported_count} trade records to: {output_jsonl}")
    return exported_count


if __name__ == "__main__":
    generate_trades()
