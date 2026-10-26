"""
Unit and Integration Tests for Phase 1: Core Foundation.

Verifies:
  A. Order serialization (to_dict, from_dict, legacy aliases)
  B. Trade serialization (to_dict, from_dict, immutability)
  C. NodeDescriptor serialization (to_dict, from_dict, roles)
  D. Lamport clock increment (LC1)
  E. Lamport clock receive/update (LC2)
  F. Lamport monotonicity
  G. BUY/SELL matching
  H. Partial fill
  I. Complete fill
  J. Price priority (bids & asks)
  K. Time priority (FIFO at same price)
  L. Multiple symbols isolation
  M. Persistence (orders and trades in SQLite)
  N. End-to-end integration test
"""

import os
import tempfile
import threading
import unittest
from dataclasses import FrozenInstanceError

from core.models import Order, Trade, NodeDescriptor, OrderSide, OrderStatus, NodeRole
from core.clock import LamportClock
from core.matching_engine import MatchingEngine, OrderBook
from core.persistence import SQLitePersistence
from core.config import ClusterConfig, DEFAULT_HOST, LOAD_BALANCER_PORT, NODE_CONFIGS


class TestPhase1Models(unittest.TestCase):
    """A, B, C: Tests for Canonical Models and Serialization."""

    def test_order_serialization(self):
        """Test Order to_dict and from_dict canonical serialization."""
        order = Order(
            order_id=42,
            trader_id="trader_alpha",
            symbol="aapl",  # Should be normalized to AAPL
            side="buy",     # Should be normalized to BUY
            price=150.25,
            quantity=100,
            timestamp="2026-10-26T12:00:00Z",
            client_lamport_ts=5,
            server_lamport_ts=6,
            status=OrderStatus.PENDING,
            filled_quantity=0,
        )

        self.assertEqual(order.symbol, "AAPL")
        self.assertEqual(order.side, "BUY")
        self.assertEqual(order.remaining_quantity, 100)
        self.assertFalse(order.is_filled)

        d = order.to_dict()
        self.assertEqual(d["order_id"], 42)
        self.assertEqual(d["symbol"], "AAPL")
        self.assertEqual(d["side"], "BUY")
        self.assertEqual(d["price"], 150.25)
        self.assertEqual(d["quantity"], 100)
        self.assertEqual(d["remaining_quantity"], 100)

        restored = Order.from_dict(d)
        self.assertEqual(restored.order_id, 42)
        self.assertEqual(restored.trader_id, "trader_alpha")
        self.assertEqual(restored.symbol, "AAPL")
        self.assertEqual(restored.side, "BUY")
        self.assertEqual(restored.price, 150.25)
        self.assertEqual(restored.quantity, 100)
        self.assertEqual(restored.client_lamport_ts, 5)
        self.assertEqual(restored.server_lamport_ts, 6)

    def test_order_legacy_aliases(self):
        """Test backward-compatible parsing of legacy lab dicts."""
        # Lab 1/2 format: type, symbol
        legacy_lab1 = {
            "trader_id": "trader1",
            "symbol": "STOCK_A",
            "type": "BUY",
            "price": 101.5,
            "quantity": 10,
        }
        o1 = Order.from_dict(legacy_lab1)
        self.assertEqual(o1.side, "BUY")
        self.assertEqual(o1.symbol, "STOCK_A")

        # Lab 3 format: stock, side, lamport_ts
        legacy_lab3 = {
            "trader_id": "trader2",
            "stock": "STOCK_B",
            "side": "SELL",
            "price": 55.25,
            "quantity": 20,
            "lamport_ts": 8,
        }
        o2 = Order.from_dict(legacy_lab3)
        self.assertEqual(o2.symbol, "STOCK_B")
        self.assertEqual(o2.client_lamport_ts, 8)

        # Lab 5/8 format: trader, qty, id
        legacy_lab5 = {
            "id": 99,
            "trader": "alice",
            "symbol": "STOCK_C",
            "side": "buy",
            "price": 200.0,
            "qty": 50,
        }
        o3 = Order.from_dict(legacy_lab5)
        self.assertEqual(o3.order_id, 99)
        self.assertEqual(o3.trader_id, "alice")
        self.assertEqual(o3.quantity, 50)
        self.assertEqual(o3.side, "BUY")

    def test_order_validation(self):
        """Test validation of price, quantity, and side."""
        with self.assertRaises(ValueError):
            Order(trader_id="t1", symbol="AAPL", side="INVALID", price=10.0, quantity=5)

        with self.assertRaises(ValueError):
            Order(trader_id="t1", symbol="AAPL", side="BUY", price=-1.0, quantity=5)

        with self.assertRaises(ValueError):
            Order(trader_id="t1", symbol="AAPL", side="BUY", price=10.0, quantity=0)

    def test_trade_serialization_and_immutability(self):
        """Test Trade to_dict, from_dict, and immutability (frozen dataclass)."""
        trade = Trade(
            trade_id=1,
            buy_order_id=10,
            sell_order_id=20,
            buyer_id="alice",
            seller_id="bob",
            symbol="msft",
            price=310.50,
            quantity=25,
            timestamp="2026-10-26T12:00:01Z",
            lamport_ts=15,
        )

        self.assertEqual(trade.symbol, "MSFT")
        self.assertEqual(trade.price, 310.50)
        self.assertEqual(trade.quantity, 25)

        # Immutability check
        with self.assertRaises((FrozenInstanceError, AttributeError)):
            trade.price = 320.0  # type: ignore

        d = trade.to_dict()
        restored = Trade.from_dict(d)
        self.assertEqual(restored.trade_id, 1)
        self.assertEqual(restored.buyer_id, "alice")
        self.assertEqual(restored.seller_id, "bob")
        self.assertEqual(restored.symbol, "MSFT")
        self.assertEqual(restored.price, 310.50)
        self.assertEqual(restored.quantity, 25)
        self.assertEqual(restored.lamport_ts, 15)

    def test_node_descriptor_serialization(self):
        """Test NodeDescriptor serialization and role validation."""
        node = NodeDescriptor(
            node_id="node-1",
            host="127.0.0.1",
            port=8001,
            priority=1,
            role=NodeRole.LEADER,
            is_alive=True,
        )
        self.assertEqual(node.url, "http://127.0.0.1:8001/RPC2")

        d = node.to_dict()
        restored = NodeDescriptor.from_dict(d)
        self.assertEqual(restored.node_id, "node-1")
        self.assertEqual(restored.port, 8001)
        self.assertEqual(restored.role, NodeRole.LEADER)

        with self.assertRaises(ValueError):
            NodeDescriptor(node_id="n", host="localhost", port=8000, priority=1, role="UNKNOWN")


class TestPhase1Clock(unittest.TestCase):
    """D, E, F: Tests for Lamport Logical Clock."""

    def test_lamport_clock_increment(self):
        """D: Test LC1 rule: C = C + 1 before event or send."""
        clock = LamportClock(start_value=0) if hasattr(LamportClock, "start_value") else LamportClock(initial_value=0)
        self.assertEqual(clock.read(), 0)
        ts1 = clock.increment()
        self.assertEqual(ts1, 1)
        self.assertEqual(clock.read(), 1)
        ts2 = clock.increment()
        self.assertEqual(ts2, 2)
        # Test backward-compatible aliases
        self.assertEqual(clock.tick(), 3)
        self.assertEqual(clock.value, 3)

    def test_lamport_clock_receive_update(self):
        """E: Test LC2 rule: C = max(C, C_msg) + 1 on message receipt."""
        clock = LamportClock(initial_value=5)

        # Receive timestamp higher than local clock (10 > 5)
        new_ts = clock.update(10)
        self.assertEqual(new_ts, 11)
        self.assertEqual(clock.read(), 11)

        # Receive timestamp lower than local clock (8 < 11)
        new_ts2 = clock.update(8)
        self.assertEqual(new_ts2, 12)
        self.assertEqual(clock.read(), 12)

    def test_lamport_monotonicity_concurrent(self):
        """F: Test clock monotonicity under multi-threaded concurrent access."""
        clock = LamportClock(initial_value=0)
        observed_timestamps = []
        lock = threading.Lock()

        def worker(num_events: int):
            for _ in range(num_events):
                ts = clock.increment()
                with lock:
                    observed_timestamps.append(ts)

        threads = [threading.Thread(target=worker, args=(50,)) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(observed_timestamps), 500)
        # All timestamps must be unique integers from 1 to 500
        self.assertEqual(len(set(observed_timestamps)), 500)
        self.assertEqual(min(observed_timestamps), 1)
        self.assertEqual(max(observed_timestamps), 500)
        self.assertEqual(clock.read(), 500)


class TestPhase1MatchingEngine(unittest.TestCase):
    """G, H, I, J, K, L: Tests for Price-Time Priority Continuous Double Auction."""

    def setUp(self):
        self.engine = MatchingEngine()

    def test_buy_sell_matching(self):
        """G: BUY matches SELL when BUY price >= SELL price."""
        buy = Order(trader_id="t1", symbol="AAPL", side="BUY", price=150.0, quantity=10)
        sell = Order(trader_id="t2", symbol="AAPL", side="SELL", price=150.0, quantity=10)

        _, trades1 = self.engine.submit_order(buy)
        self.assertEqual(len(trades1), 0)  # Resting on book

        _, trades2 = self.engine.submit_order(sell)
        self.assertEqual(len(trades2), 1)

        trade = trades2[0]
        self.assertEqual(trade.symbol, "AAPL")
        self.assertEqual(trade.price, 150.0)
        self.assertEqual(trade.quantity, 10)
        self.assertEqual(trade.buyer_id, "t1")
        self.assertEqual(trade.seller_id, "t2")
        self.assertTrue(buy.is_filled)
        self.assertTrue(sell.is_filled)

    def test_no_match_spread(self):
        """Test that orders do NOT match when BUY price < SELL price."""
        buy = Order(trader_id="t1", symbol="AAPL", side="BUY", price=99.0, quantity=10)
        sell = Order(trader_id="t2", symbol="AAPL", side="SELL", price=100.0, quantity=10)

        _, t1 = self.engine.submit_order(buy)
        _, t2 = self.engine.submit_order(sell)

        self.assertEqual(len(t1), 0)
        self.assertEqual(len(t2), 0)

        book = self.engine.get_order_book("AAPL")
        self.assertEqual(len(book["bids"]), 1)
        self.assertEqual(len(book["asks"]), 1)

    def test_partial_fill(self):
        """H: Partial fill test (prompt example: BUY 100 @ 105, SELL 40 @ 100)."""
        # Resting sell order: SELL 40 @ 100
        sell = Order(trader_id="seller1", symbol="ABC", side="SELL", price=100.0, quantity=40)
        self.engine.submit_order(sell)

        # Incoming buy order: BUY 100 @ 105
        buy = Order(trader_id="buyer1", symbol="ABC", side="BUY", price=105.0, quantity=100)
        _, trades = self.engine.submit_order(buy)

        self.assertEqual(len(trades), 1)
        t = trades[0]
        self.assertEqual(t.quantity, 40)
        self.assertEqual(t.price, 100.0)  # Resting order price
        self.assertEqual(buy.filled_quantity, 40)
        self.assertEqual(buy.remaining_quantity, 60)
        self.assertEqual(buy.status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(sell.filled_quantity, 40)
        self.assertEqual(sell.remaining_quantity, 0)
        self.assertEqual(sell.status, OrderStatus.FILLED)

        # The resting sell is gone, remaining buy is on the book
        book = self.engine.get_order_book("ABC")
        self.assertEqual(len(book["asks"]), 0)
        self.assertEqual(len(book["bids"]), 1)
        self.assertEqual(book["bids"][0]["remaining_quantity"], 60)

    def test_complete_fill(self):
        """I: Complete fill test where both orders are satisfied exactly."""
        buy = Order(trader_id="b1", symbol="GOOG", side="BUY", price=2800.0, quantity=50)
        sell = Order(trader_id="s1", symbol="GOOG", side="SELL", price=2800.0, quantity=50)

        self.engine.submit_order(buy)
        _, trades = self.engine.submit_order(sell)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].quantity, 50)
        self.assertEqual(buy.status, OrderStatus.FILLED)
        self.assertEqual(sell.status, OrderStatus.FILLED)

        book = self.engine.get_order_book("GOOG")
        self.assertEqual(len(book["bids"]), 0)
        self.assertEqual(len(book["asks"]), 0)

    def test_price_priority(self):
        """J: Price priority test (higher bid matches first; lower ask matches first)."""
        # Test 1: Multiple asks resting at different prices
        # Lowest ask (100) should be matched before higher ask (102)
        sell_high = Order(trader_id="s_high", symbol="XYZ", side="SELL", price=102.0, quantity=10)
        sell_low = Order(trader_id="s_low", symbol="XYZ", side="SELL", price=100.0, quantity=10)
        self.engine.submit_order(sell_high)
        self.engine.submit_order(sell_low)

        # Incoming buy for 10 shares @ 105
        buy = Order(trader_id="buyer", symbol="XYZ", side="BUY", price=105.0, quantity=10)
        _, trades = self.engine.submit_order(buy)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].seller_id, "s_low")
        self.assertEqual(trades[0].price, 100.0)

        # Test 2: Multiple bids resting at different prices
        # Highest bid (55) should be matched before lower bid (50)
        engine2 = MatchingEngine()
        buy_low = Order(trader_id="b_low", symbol="XYZ", side="BUY", price=50.0, quantity=10)
        buy_high = Order(trader_id="b_high", symbol="XYZ", side="BUY", price=55.0, quantity=10)
        engine2.submit_order(buy_low)
        engine2.submit_order(buy_high)

        # Incoming sell for 10 shares @ 45
        sell = Order(trader_id="seller", symbol="XYZ", side="SELL", price=45.0, quantity=10)
        _, trades2 = engine2.submit_order(sell)

        self.assertEqual(len(trades2), 1)
        self.assertEqual(trades2[0].buyer_id, "b_high")
        self.assertEqual(trades2[0].price, 55.0)

    def test_time_priority(self):
        """K: Time priority test (FIFO for identical price levels)."""
        # Two sells at the EXACT same price: s1 arrived first, s2 arrived second
        sell1 = Order(trader_id="seller_first", symbol="TICK", side="SELL", price=100.0, quantity=10)
        sell2 = Order(trader_id="seller_second", symbol="TICK", side="SELL", price=100.0, quantity=10)
        self.engine.submit_order(sell1)
        self.engine.submit_order(sell2)

        # Incoming buy takes 10 shares
        buy = Order(trader_id="buyer", symbol="TICK", side="BUY", price=100.0, quantity=10)
        _, trades = self.engine.submit_order(buy)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].seller_id, "seller_first")
        self.assertTrue(sell1.is_filled)
        self.assertEqual(sell2.remaining_quantity, 10)

    def test_multiple_symbols(self):
        """L: Multiple symbols isolation (AAPL orders do not cross with TSLA)."""
        buy_aapl = Order(trader_id="b1", symbol="AAPL", side="BUY", price=150.0, quantity=10)
        sell_tsla = Order(trader_id="s1", symbol="TSLA", side="SELL", price=150.0, quantity=10)

        _, t1 = self.engine.submit_order(buy_aapl)
        _, t2 = self.engine.submit_order(sell_tsla)

        self.assertEqual(len(t1), 0)
        self.assertEqual(len(t2), 0)

        self.assertEqual(len(self.engine.get_order_book("AAPL")["bids"]), 1)
        self.assertEqual(len(self.engine.get_order_book("TSLA")["asks"]), 1)


class TestPhase1Persistence(unittest.TestCase):
    """M: Tests for SQLite Persistence Layer."""

    def setUp(self):
        # Use temporary file so persistence across connections can be tested
        self.temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_file.close()
        self.storage = SQLitePersistence(self.temp_file.name)

    def tearDown(self):
        self.storage.close()
        if os.path.exists(self.temp_file.name):
            try:
                os.remove(self.temp_file.name)
            except OSError:
                pass

    def test_save_and_get_order(self):
        """Save and fetch Order from SQLite."""
        order = Order(
            order_id=101,
            trader_id="trader1",
            symbol="NVDA",
            side="BUY",
            price=450.0,
            quantity=50,
            status=OrderStatus.PENDING,
        )
        self.storage.save_order(order)

        fetched = self.storage.get_order(101)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.order_id, 101)
        self.assertEqual(fetched.trader_id, "trader1")
        self.assertEqual(fetched.symbol, "NVDA")
        self.assertEqual(fetched.side, "BUY")
        self.assertEqual(fetched.price, 450.0)
        self.assertEqual(fetched.quantity, 50)

    def test_save_and_get_trade(self):
        """Save and fetch Trade from SQLite."""
        trade = Trade(
            trade_id=1,
            buy_order_id=101,
            sell_order_id=102,
            buyer_id="trader1",
            seller_id="trader2",
            symbol="NVDA",
            price=450.0,
            quantity=25,
            timestamp="2026-10-26T12:00:00Z",
            lamport_ts=12,
        )
        self.storage.save_trade(trade)

        fetched = self.storage.get_trade(1)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.trade_id, 1)
        self.assertEqual(fetched.buy_order_id, 101)
        self.assertEqual(fetched.sell_order_id, 102)
        self.assertEqual(fetched.price, 450.0)
        self.assertEqual(fetched.quantity, 25)

    def test_export_trades_jsonl(self):
        """Export trades to JSONL file."""
        t1 = Trade(1, 10, 20, "b1", "s1", "AAPL", 150.0, 10, "2026-10-26", 1)
        t2 = Trade(2, 11, 21, "b2", "s2", "GOOG", 2800.0, 5, "2026-10-26", 2)
        self.storage.save_trade(t1)
        self.storage.save_trade(t2)

        export_file = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False)
        export_file.close()
        try:
            count = self.storage.export_trades_jsonl(export_file.name)
            self.assertEqual(count, 2)
            with open(export_file.name, "r", encoding="utf-8") as f:
                lines = f.readlines()
            self.assertEqual(len(lines), 2)
        finally:
            if os.path.exists(export_file.name):
                os.remove(export_file.name)


class TestPhase1Integration(unittest.TestCase):
    """
    End-to-end integration test:
    Submit BUY -> Submit compatible SELL -> MatchingEngine creates Trade
    -> Trade is persisted -> Remaining order quantity is correct
    """

    def test_end_to_end_flow(self):
        storage = SQLitePersistence(":memory:")
        engine = MatchingEngine()
        clock = LamportClock()

        # 1. Trader Alice submits BUY 100 shares of MSFT @ $300.00
        alice_ts = clock.increment()
        buy_order = Order(
            trader_id="Alice",
            symbol="MSFT",
            side=OrderSide.BUY,
            price=300.00,
            quantity=100,
            client_lamport_ts=alice_ts,
        )

        server_ts = clock.increment()
        buy_order.server_lamport_ts = server_ts
        order_res, trades1 = engine.submit_order(buy_order, lamport_ts=server_ts)
        storage.save_order(buy_order)

        self.assertEqual(len(trades1), 0)
        self.assertEqual(buy_order.status, OrderStatus.PENDING)
        self.assertEqual(buy_order.remaining_quantity, 100)

        # 2. Trader Bob submits compatible SELL 60 shares of MSFT @ $295.00
        bob_ts = clock.increment()
        sell_order = Order(
            trader_id="Bob",
            symbol="MSFT",
            side=OrderSide.SELL,
            price=295.00,
            quantity=60,
            client_lamport_ts=bob_ts,
        )

        server_ts2 = clock.increment()
        sell_order.server_lamport_ts = server_ts2
        order_res2, trades2 = engine.submit_order(sell_order, lamport_ts=server_ts2)

        # 3. Verify MatchingEngine generated Trade
        self.assertEqual(len(trades2), 1)
        executed_trade = trades2[0]

        # Trade price equals resting buy order price ($300.00)
        self.assertEqual(executed_trade.price, 300.00)
        self.assertEqual(executed_trade.quantity, 60)
        self.assertEqual(executed_trade.buyer_id, "Alice")
        self.assertEqual(executed_trade.seller_id, "Bob")
        self.assertEqual(executed_trade.symbol, "MSFT")

        # 4. Save updated orders and generated trade in persistence layer
        storage.save_order(buy_order)
        storage.save_order(sell_order)
        storage.save_trade(executed_trade)

        # 5. Verify persisted order quantities and statuses
        stored_buy = storage.get_order(buy_order.order_id)
        stored_sell = storage.get_order(sell_order.order_id)
        stored_trades = storage.get_trades("MSFT")

        self.assertIsNotNone(stored_buy)
        self.assertEqual(stored_buy.remaining_quantity, 40)
        self.assertEqual(stored_buy.filled_quantity, 60)
        self.assertEqual(stored_buy.status, OrderStatus.PARTIALLY_FILLED)

        self.assertIsNotNone(stored_sell)
        self.assertEqual(stored_sell.remaining_quantity, 0)
        self.assertEqual(stored_sell.filled_quantity, 60)
        self.assertEqual(stored_sell.status, OrderStatus.FILLED)

        self.assertEqual(len(stored_trades), 1)
        self.assertEqual(stored_trades[0].quantity, 60)
        self.assertEqual(stored_trades[0].price, 300.00)
        storage.close()


class TestPhase1Config(unittest.TestCase):
    """Verify cluster topology configuration."""

    def test_cluster_config(self):
        lb = ClusterConfig.get_load_balancer()
        self.assertEqual(lb.port, 8000)

        nodes = ClusterConfig.get_all_nodes()
        self.assertEqual(len(nodes), 3)
        self.assertEqual([n.port for n in nodes], [8001, 8002, 8003])
        self.assertEqual([n.priority for n in nodes], [1, 2, 3])

        leader = ClusterConfig.get_default_leader()
        self.assertEqual(leader.node_id, "node-3")
        self.assertEqual(leader.port, 8003)
        self.assertEqual(leader.priority, 3)


if __name__ == "__main__":
    unittest.main()
