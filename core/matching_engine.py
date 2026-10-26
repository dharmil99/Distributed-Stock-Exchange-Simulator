"""
Continuous Double Auction Matching Engine for Distributed Stock Exchange Simulator.

Implements price-time priority matching:
  - BUY orders: highest price first; ties broken by earliest arrival/sequence.
  - SELL orders: lowest price first; ties broken by earliest arrival/sequence.
  - Match condition: BUY price >= SELL price.
  - Execution price: Price of the resting limit order already on the book.
  - Supports complete fills, partial fills, remaining quantities, multiple symbols,
    and produces immutable Trade records.
"""

import threading
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Any

from core.models import Order, Trade, OrderSide, OrderStatus


class OrderBook:
    """
    Limit Order Book for a single stock symbol.
    Maintains sorted bids (buyers) and asks (sellers) with price-time priority.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.bids: List[Order] = []  # Sorted: highest price first, then earliest sequence
        self.asks: List[Order] = []  # Sorted: lowest price first, then earliest sequence
        self.trades: List[Trade] = []
        self._lock = threading.RLock()
        self._order_sequence = 0

    def match_order(
        self,
        incoming: Order,
        next_trade_id_fn,
        lamport_ts: int = 0
    ) -> List[Trade]:
        """
        Process an incoming limit order against resting orders on the book.
        Executes trades whenever match condition (BUY price >= SELL price) is satisfied.
        Unfilled remainder rests on the book.
        """
        with self._lock:
            generated_trades: List[Trade] = []
            self._order_sequence += 1

            if incoming.side == OrderSide.BUY:
                generated_trades.extend(
                    self._match_buy(incoming, next_trade_id_fn, lamport_ts)
                )
            else:
                generated_trades.extend(
                    self._match_sell(incoming, next_trade_id_fn, lamport_ts)
                )

            self.trades.extend(generated_trades)
            return generated_trades

    def _match_buy(
        self,
        buy: Order,
        next_trade_id_fn,
        lamport_ts: int
    ) -> List[Trade]:
        trades: List[Trade] = []

        # Match against resting asks while match condition holds
        while self.asks and buy.remaining_quantity > 0:
            best_ask = self.asks[0]
            if buy.price < best_ask.price:
                # No match possible; highest willing buy price is below lowest ask
                break

            # Execute trade at resting ask price
            trade_qty = min(buy.remaining_quantity, best_ask.remaining_quantity)
            trade_price = best_ask.price
            trade_id = next_trade_id_fn()

            trade = Trade(
                trade_id=trade_id,
                buy_order_id=buy.order_id if buy.order_id is not None else 0,
                sell_order_id=best_ask.order_id if best_ask.order_id is not None else 0,
                buyer_id=buy.trader_id,
                seller_id=best_ask.trader_id,
                symbol=self.symbol,
                price=trade_price,
                quantity=trade_qty,
                timestamp=datetime.now(timezone.utc).isoformat(),
                lamport_ts=lamport_ts,
            )
            trades.append(trade)

            # Update filled quantities
            buy.filled_quantity += trade_qty
            best_ask.filled_quantity += trade_qty

            # Update statuses
            buy.status = (
                OrderStatus.FILLED if buy.remaining_quantity == 0
                else OrderStatus.PARTIALLY_FILLED
            )
            best_ask.status = (
                OrderStatus.FILLED if best_ask.remaining_quantity == 0
                else OrderStatus.PARTIALLY_FILLED
            )

            # If resting ask is completely filled, remove it from the book
            if best_ask.remaining_quantity == 0:
                self.asks.pop(0)

        # If incoming buy order still has unfilled shares, insert it into bids
        if buy.remaining_quantity > 0:
            self._insert_bid(buy)

        return trades

    def _match_sell(
        self,
        sell: Order,
        next_trade_id_fn,
        lamport_ts: int
    ) -> List[Trade]:
        trades: List[Trade] = []

        # Match against resting bids while match condition holds
        while self.bids and sell.remaining_quantity > 0:
            best_bid = self.bids[0]
            if sell.price > best_bid.price:
                # No match possible; lowest sell price is above highest bid
                break

            # Execute trade at resting bid price
            trade_qty = min(sell.remaining_quantity, best_bid.remaining_quantity)
            trade_price = best_bid.price
            trade_id = next_trade_id_fn()

            trade = Trade(
                trade_id=trade_id,
                buy_order_id=best_bid.order_id if best_bid.order_id is not None else 0,
                sell_order_id=sell.order_id if sell.order_id is not None else 0,
                buyer_id=best_bid.trader_id,
                seller_id=sell.trader_id,
                symbol=self.symbol,
                price=trade_price,
                quantity=trade_qty,
                timestamp=datetime.now(timezone.utc).isoformat(),
                lamport_ts=lamport_ts,
            )
            trades.append(trade)

            # Update filled quantities
            sell.filled_quantity += trade_qty
            best_bid.filled_quantity += trade_qty

            # Update statuses
            sell.status = (
                OrderStatus.FILLED if sell.remaining_quantity == 0
                else OrderStatus.PARTIALLY_FILLED
            )
            best_bid.status = (
                OrderStatus.FILLED if best_bid.remaining_quantity == 0
                else OrderStatus.PARTIALLY_FILLED
            )

            # If resting bid is completely filled, remove it from the book
            if best_bid.remaining_quantity == 0:
                self.bids.pop(0)

        # If incoming sell order still has unfilled shares, insert it into asks
        if sell.remaining_quantity > 0:
            self._insert_ask(sell)

        return trades

    def _insert_bid(self, order: Order) -> None:
        """
        Insert buy order maintaining price-time priority:
        Higher price first. For same price, earlier order comes first.
        """
        for i, existing in enumerate(self.bids):
            if order.price > existing.price:
                self.bids.insert(i, order)
                return
        self.bids.append(order)

    def _insert_ask(self, order: Order) -> None:
        """
        Insert sell order maintaining price-time priority:
        Lower price first. For same price, earlier order comes first.
        """
        for i, existing in enumerate(self.asks):
            if order.price < existing.price:
                self.asks.insert(i, order)
                return
        self.asks.append(order)

    def cancel_order(self, order_id: int) -> Optional[Order]:
        """Cancel an open order on the book by ID."""
        with self._lock:
            for i, order in enumerate(self.bids):
                if order.order_id == order_id:
                    order.status = OrderStatus.CANCELLED
                    return self.bids.pop(i)
            for i, order in enumerate(self.asks):
                if order.order_id == order_id:
                    order.status = OrderStatus.CANCELLED
                    return self.asks.pop(i)
            return None

    def get_snapshot(self) -> Dict[str, Any]:
        """Return full snapshot of the book and depth."""
        with self._lock:
            return {
                "symbol": self.symbol,
                "bids": [b.to_dict() for b in self.bids],
                "asks": [a.to_dict() for a in self.asks],
                "trade_count": len(self.trades),
            }

    def get_depth(self) -> Dict[str, Any]:
        """Aggregate bids and asks by price level."""
        with self._lock:
            bid_levels: Dict[float, int] = {}
            for b in self.bids:
                bid_levels[b.price] = bid_levels.get(b.price, 0) + b.remaining_quantity

            ask_levels: Dict[float, int] = {}
            for a in self.asks:
                ask_levels[a.price] = ask_levels.get(a.price, 0) + a.remaining_quantity

            sorted_bids = [{"price": p, "quantity": q} for p, q in sorted(bid_levels.items(), key=lambda x: -x[0])]
            sorted_asks = [{"price": p, "quantity": q} for p, q in sorted(ask_levels.items(), key=lambda x: x[0])]

            return {
                "symbol": self.symbol,
                "bids": sorted_bids,
                "asks": sorted_asks,
            }


class MatchingEngine:
    """
    Multi-symbol Continuous Double Auction Matching Engine.
    Manages isolated OrderBook instances per stock symbol, generates globally
    unique order and trade identifiers, and preserves thread-safe execution.
    """

    def __init__(self, initial_order_id: int = 1, initial_trade_id: int = 1):
        self._books: Dict[str, OrderBook] = {}
        self._next_order_id = int(initial_order_id)
        self._next_trade_id = int(initial_trade_id)
        self._all_orders: Dict[int, Order] = {}
        self._all_trades: List[Trade] = []
        self._lock = threading.RLock()

    def _get_or_create_book(self, symbol: str) -> OrderBook:
        sym = symbol.strip().upper()
        if sym not in self._books:
            self._books[sym] = OrderBook(sym)
        return self._books[sym]

    def _generate_trade_id(self) -> int:
        with self._lock:
            tid = self._next_trade_id
            self._next_trade_id += 1
            return tid

    def assign_order_id(self, order: Order) -> int:
        """Assign sequential unique order_id if not already assigned."""
        with self._lock:
            if order.order_id is None:
                order.order_id = self._next_order_id
                self._next_order_id += 1
            elif order.order_id >= self._next_order_id:
                self._next_order_id = order.order_id + 1
            self._all_orders[order.order_id] = order
            return order.order_id

    def submit_order(self, order: Order, lamport_ts: int = 0) -> Tuple[Order, List[Trade]]:
        """
        Submit a limit order to the matching engine.
        Assigns an order ID if needed, processes matches against the symbol's book,
        records generated trades, and returns (order, generated_trades).
        """
        with self._lock:
            self.assign_order_id(order)
            book = self._get_or_create_book(order.symbol)
            trades = book.match_order(order, self._generate_trade_id, lamport_ts=lamport_ts)
            self._all_trades.extend(trades)
            return order, trades

    def cancel_order(self, symbol: str, order_id: int) -> Optional[Order]:
        """Cancel an open order."""
        with self._lock:
            book = self._books.get(symbol.strip().upper())
            if not book:
                return None
            cancelled = book.cancel_order(order_id)
            if cancelled and order_id in self._all_orders:
                self._all_orders[order_id].status = OrderStatus.CANCELLED
            return cancelled

    def get_order(self, order_id: int) -> Optional[Order]:
        """Retrieve an order by ID."""
        with self._lock:
            return self._all_orders.get(order_id)

    def get_order_book(self, symbol: str) -> Dict[str, Any]:
        """Return snapshot of the order book for the given symbol."""
        with self._lock:
            book = self._get_or_create_book(symbol)
            return book.get_snapshot()

    def get_depth(self, symbol: str) -> Dict[str, Any]:
        """Return aggregate depth for the given symbol."""
        with self._lock:
            book = self._get_or_create_book(symbol)
            return book.get_depth()

    def get_all_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Return copy of all registered orders, optionally filtered by symbol."""
        with self._lock:
            orders = list(self._all_orders.values())
            if symbol:
                s = symbol.strip().upper()
                orders = [o for o in orders if o.symbol == s]
            return orders

    def get_all_trades(self, symbol: Optional[str] = None) -> List[Trade]:
        """Return copy of all executed trades, optionally filtered by symbol."""
        with self._lock:
            if symbol:
                s = symbol.strip().upper()
                return [t for t in self._all_trades if t.symbol == s]
            return list(self._all_trades)
