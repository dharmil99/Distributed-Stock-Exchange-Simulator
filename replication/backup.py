"""
Backup Replication Receiver for Distributed Stock Exchange Simulator.

Receives replication events from the Primary and applies state updates to
the local MatchingEngine and SQLitePersistence so backups remain hot and synchronized.
"""

import threading
from typing import Any, Dict, Optional

from core.clock import LamportClock
from core.matching_engine import MatchingEngine
from core.models import Order, OrderSide, OrderStatus, Trade
from core.persistence import SQLitePersistence


class BackupReplicationReceiver:
    """
    Applies replicated orders, trades, and cancellations to the local node's state.
    """

    def __init__(
        self,
        node_id: str,
        matching_engine: MatchingEngine,
        persistence: SQLitePersistence,
        clock: LamportClock,
    ):
        self.node_id = node_id
        self.matching_engine = matching_engine
        self.persistence = persistence
        self.clock = clock
        self._lock = threading.RLock()
        self.replicated_event_count = 0

    def apply_event(self, event_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process and persist an incoming replication event.
        """
        with self._lock:
            remote_ts = event_payload.get("server_lamport_ts")
            if remote_ts is not None:
                self.clock.update(int(remote_ts))
            else:
                self.clock.increment()

            event_id = event_payload.get("event_id")
            event_type = event_payload.get("event_type")
            data = event_payload.get("data", {})

            if event_type == "ORDER_SUBMITTED":
                self._apply_order_submitted(data)
            elif event_type == "ORDER_CANCELLED":
                self._apply_order_cancelled(data)

            self.replicated_event_count += 1

            return {
                "status": "success",
                "node_id": self.node_id,
                "event_id": event_id,
                "replicated_count": self.replicated_event_count,
            }

    def _apply_order_submitted(self, data: Dict[str, Any]):
        """Reconstruct order submission, trade execution, and resting counterparties."""
        order_dict = data.get("order")
        if not order_dict:
            return

        order = Order.from_dict(order_dict)
        self.persistence.save_order(order)

        # Sync counterparty orders
        for cp_dict in data.get("counterparties", []):
            cp_order = Order.from_dict(cp_dict)
            self.persistence.save_order(cp_order)

            # Update book in matching engine for counterparty
            book = self.matching_engine._get_or_create_book(cp_order.symbol)
            target_list = book.bids if cp_order.side == OrderSide.BUY else book.asks
            for i, existing in enumerate(target_list):
                if existing.order_id == cp_order.order_id:
                    if cp_order.remaining_quantity == 0:
                        target_list.pop(i)
                    else:
                        existing.filled_quantity = cp_order.filled_quantity
                        existing.status = cp_order.status
                    break

        # Save generated trades
        for t_dict in data.get("trades", []):
            trade = Trade.from_dict(t_dict)
            self.persistence.save_trade(trade)
            if not any(t.trade_id == trade.trade_id for t in self.matching_engine._all_trades):
                self.matching_engine._all_trades.append(trade)

        # If incoming order still has remaining quantity, insert into backup book
        if order.remaining_quantity > 0 and order.status != OrderStatus.CANCELLED:
            self.matching_engine.assign_order_id(order)
            book = self.matching_engine._get_or_create_book(order.symbol)
            if order.side == OrderSide.BUY:
                if not any(b.order_id == order.order_id for b in book.bids):
                    book._insert_bid(order)
            else:
                if not any(a.order_id == order.order_id for a in book.asks):
                    book._insert_ask(order)

    def _apply_order_cancelled(self, data: Dict[str, Any]):
        """Reconstruct order cancellation."""
        symbol = data.get("symbol")
        order_id = data.get("order_id")
        if symbol and order_id is not None:
            self.matching_engine.cancel_order(symbol, int(order_id))
            cancelled_order = self.persistence.get_order(int(order_id))
            if cancelled_order:
                cancelled_order.status = OrderStatus.CANCELLED
                self.persistence.save_order(cancelled_order)
