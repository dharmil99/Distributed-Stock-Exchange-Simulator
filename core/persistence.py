"""
SQLite Persistence Layer for Distributed Stock Exchange Simulator.

Provides thread-safe persistence for:
  - Orders: Records full lifecycle (PENDING, PARTIALLY_FILLED, FILLED, CANCELLED)
  - Trades: Immutable transaction fill history

Also provides export utilities (JSONL/CSV) so downstream analytics engines
(such as Apache Spark in Lab 7) can easily ingest historical market data.
"""

import json
import sqlite3
import threading
from typing import List, Optional

from core.models import Order, Trade


class SQLitePersistence:
    """
    Thread-safe SQLite storage for Orders and Trades.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        with self._lock:
            cur = self._conn.cursor()
            if self.db_path != ":memory:":
                cur.execute("PRAGMA journal_mode = WAL;")
            cur.execute("PRAGMA synchronous = NORMAL;")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY,
                    trader_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    quantity INTEGER NOT NULL,
                    filled_quantity INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    client_lamport_ts INTEGER,
                    server_lamport_ts INTEGER
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id INTEGER PRIMARY KEY,
                    buy_order_id INTEGER NOT NULL,
                    sell_order_id INTEGER NOT NULL,
                    buyer_id TEXT NOT NULL,
                    seller_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL,
                    quantity INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    lamport_ts INTEGER NOT NULL
                );
            """)

            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);")
            self._conn.commit()

    def save_order(self, order: Order) -> None:
        """Insert or replace an Order record."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO orders (
                    order_id, trader_id, symbol, side, price,
                    quantity, filled_quantity, status, timestamp,
                    client_lamport_ts, server_lamport_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                order.order_id,
                order.trader_id,
                order.symbol,
                order.side,
                float(order.price),
                int(order.quantity),
                int(order.filled_quantity),
                order.status,
                order.timestamp,
                order.client_lamport_ts,
                order.server_lamport_ts,
            ))
            self._conn.commit()

    def save_trade(self, trade: Trade) -> None:
        """Insert or replace an immutable Trade record."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO trades (
                    trade_id, buy_order_id, sell_order_id, buyer_id, seller_id,
                    symbol, price, quantity, timestamp, lamport_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                trade.trade_id,
                trade.buy_order_id,
                trade.sell_order_id,
                trade.buyer_id,
                trade.seller_id,
                trade.symbol,
                float(trade.price),
                int(trade.quantity),
                trade.timestamp,
                int(trade.lamport_ts),
            ))
            self._conn.commit()

    def get_order(self, order_id: int) -> Optional[Order]:
        """Fetch single order by order_id."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM orders WHERE order_id = ?;", (order_id,))
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_order(row)

    def get_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[Order]:
        """Query orders with optional symbol and status filters."""
        with self._lock:
            cur = self._conn.cursor()
            query = "SELECT * FROM orders WHERE 1=1"
            params = []
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol.strip().upper())
            if status:
                query += " AND status = ?"
                params.append(status.strip().upper())
            query += " ORDER BY order_id ASC;"
            cur.execute(query, params)
            return [self._row_to_order(row) for row in cur.fetchall()]

    def get_trade(self, trade_id: int) -> Optional[Trade]:
        """Fetch single trade by trade_id."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM trades WHERE trade_id = ?;", (trade_id,))
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_trade(row)

    def get_trades(self, symbol: Optional[str] = None) -> List[Trade]:
        """Query trades with optional symbol filter."""
        with self._lock:
            cur = self._conn.cursor()
            query = "SELECT * FROM trades WHERE 1=1"
            params = []
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol.strip().upper())
            query += " ORDER BY trade_id ASC;"
            cur.execute(query, params)
            return [self._row_to_trade(row) for row in cur.fetchall()]

    def export_trades_jsonl(self, filepath: str, symbol: Optional[str] = None) -> int:
        """Export trades to a JSONL file suitable for batch Spark/Hadoop ingestion."""
        trades = self.get_trades(symbol=symbol)
        with open(filepath, "w", encoding="utf-8") as f:
            for t in trades:
                f.write(json.dumps(t.to_dict()) + "\n")
        return len(trades)

    def _row_to_order(self, row: sqlite3.Row) -> Order:
        return Order(
            order_id=row["order_id"],
            trader_id=row["trader_id"],
            symbol=row["symbol"],
            side=row["side"],
            price=row["price"],
            quantity=row["quantity"],
            filled_quantity=row["filled_quantity"],
            status=row["status"],
            timestamp=row["timestamp"],
            client_lamport_ts=row["client_lamport_ts"],
            server_lamport_ts=row["server_lamport_ts"],
        )

    def _row_to_trade(self, row: sqlite3.Row) -> Trade:
        return Trade(
            trade_id=row["trade_id"],
            buy_order_id=row["buy_order_id"],
            sell_order_id=row["sell_order_id"],
            buyer_id=row["buyer_id"],
            seller_id=row["seller_id"],
            symbol=row["symbol"],
            price=row["price"],
            quantity=row["quantity"],
            timestamp=row["timestamp"],
            lamport_ts=row["lamport_ts"],
        )

    def close(self):
        """Close database connection."""
        with self._lock:
            self._conn.close()
