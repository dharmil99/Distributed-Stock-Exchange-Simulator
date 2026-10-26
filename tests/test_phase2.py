"""
Unit and Integration Tests for Phase 2: Distributed Runtime Layer.

Verifies:
  1. TradingNode construction and initialization.
  2. Node descriptor and status queries.
  3. Local submit_order execution.
  4. Order persistence in SQLite.
  5. Trade persistence in SQLite.
  6. Lamport clock advancement (LC1 and LC2).
  7. get_order_book snapshot.
  8. get_depth price level aggregation.
  9. cancel_order functionality.
  10. Concurrency: Multi-threaded order submissions with no race conditions or state corruption.
  11. XML-RPC Server-Client communication over network (ephemeral port).
  12. Multi-trader matching over XML-RPC generating trades (BUY 100 @ 150 vs SELL 40 @ 145).
"""

import os
import tempfile
import threading
import time
import unittest
from typing import List

from core.clock import LamportClock
from core.matching_engine import MatchingEngine
from core.models import NodeDescriptor, NodeRole, OrderSide, OrderStatus
from core.persistence import SQLitePersistence
from communication.protocol import STATUS_SUCCESS, STATUS_NOT_FOUND
from communication.rpc_client import TradingClient
from communication.rpc_server import NodeRPCServer
from nodes.trading_node import TradingNode


class TestPhase2TradingNode(unittest.TestCase):
    """Tests 1-9: Unit testing of TradingNode in-process."""

    def setUp(self):
        self.desc = NodeDescriptor(
            node_id="test-node-1",
            host="127.0.0.1",
            port=9999,
            priority=1,
            role=NodeRole.LEADER,
            is_alive=True,
        )
        # Use temporary file database
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db.close()
        self.node = TradingNode(self.desc, db_path=self.temp_db.name)

    def tearDown(self):
        self.node.close()
        if os.path.exists(self.temp_db.name):
            try:
                os.remove(self.temp_db.name)
            except OSError:
                pass

    def test_node_construction(self):
        """1 & 2: Test TradingNode construction and node info retrieval."""
        self.assertEqual(self.node.node_id, "test-node-1")
        info = self.node.get_node_info()
        self.assertEqual(info["status"], STATUS_SUCCESS)
        self.assertEqual(info["node"]["node_id"], "test-node-1")
        self.assertEqual(info["node"]["role"], NodeRole.LEADER)
        self.assertEqual(info["node"]["priority"], 1)

    def test_local_submit_order_and_persistence(self):
        """3, 4, 5, 6: Test submit_order, persistence, and Lamport advancement."""
        order_dict = {
            "trader_id": "alice",
            "symbol": "AAPL",
            "side": "BUY",
            "price": 150.0,
            "quantity": 100,
            "client_lamport_ts": 3,
        }

        # Submit order
        res = self.node.submit_order(order_dict)
        self.assertEqual(res["status"], STATUS_SUCCESS)
        self.assertEqual(res["node_id"], "test-node-1")

        # 6: Lamport LC2 check: server clock should be >= max(0, 3) + 1 = 4
        self.assertGreaterEqual(res["server_lamport_ts"], 4)
        order_data = res["order"]
        self.assertEqual(order_data["trader_id"], "alice")
        self.assertEqual(order_data["symbol"], "AAPL")
        self.assertEqual(order_data["remaining_quantity"], 100)
        self.assertEqual(len(res["trades"]), 0)  # No matching sell yet

        # 4: Verify order persistence in SQLite
        persisted_order = self.node.persistence.get_order(order_data["order_id"])
        self.assertIsNotNone(persisted_order)
        self.assertEqual(persisted_order.trader_id, "alice")
        self.assertEqual(persisted_order.remaining_quantity, 100)

        # 5: Submit matching sell order
        sell_dict = {
            "trader_id": "bob",
            "symbol": "AAPL",
            "side": "SELL",
            "price": 145.0,
            "quantity": 40,
            "client_lamport_ts": 5,
        }
        res2 = self.node.submit_order(sell_dict)
        self.assertEqual(res2["status"], STATUS_SUCCESS)
        self.assertEqual(len(res2["trades"]), 1)

        trade_data = res2["trades"][0]
        self.assertEqual(trade_data["quantity"], 40)
        self.assertEqual(trade_data["price"], 150.0)  # Resting buy price
        self.assertEqual(trade_data["buyer_id"], "alice")
        self.assertEqual(trade_data["seller_id"], "bob")

        # Verify trade persistence
        persisted_trades = self.node.persistence.get_trades("AAPL")
        self.assertEqual(len(persisted_trades), 1)
        self.assertEqual(persisted_trades[0].quantity, 40)
        self.assertEqual(persisted_trades[0].price, 150.0)

        # Verify updated resting order quantity in persistence
        updated_buy = self.node.persistence.get_order(order_data["order_id"])
        self.assertEqual(updated_buy.remaining_quantity, 60)
        self.assertEqual(updated_buy.status, OrderStatus.PARTIALLY_FILLED)

    def test_get_order_book_and_depth(self):
        """7 & 8: Test get_order_book and get_depth queries."""
        self.node.submit_order({"trader_id": "b1", "symbol": "MSFT", "side": "BUY", "price": 300.0, "quantity": 10})
        self.node.submit_order({"trader_id": "b2", "symbol": "MSFT", "side": "BUY", "price": 305.0, "quantity": 15})
        self.node.submit_order({"trader_id": "s1", "symbol": "MSFT", "side": "SELL", "price": 310.0, "quantity": 20})

        # Test order book snapshot
        book_res = self.node.get_order_book("MSFT")
        self.assertEqual(book_res["status"], STATUS_SUCCESS)
        book = book_res["order_book"]
        self.assertEqual(len(book["bids"]), 2)
        self.assertEqual(len(book["asks"]), 1)
        # Price priority: 305 should be before 300
        self.assertEqual(book["bids"][0]["price"], 305.0)

        # Test market depth
        depth_res = self.node.get_depth("MSFT")
        self.assertEqual(depth_res["status"], STATUS_SUCCESS)
        depth = depth_res["depth"]
        self.assertEqual(len(depth["bids"]), 2)
        self.assertEqual(depth["bids"][0]["price"], 305.0)
        self.assertEqual(depth["bids"][0]["quantity"], 15)

    def test_cancel_order(self):
        """9: Test cancel_order functionality."""
        res = self.node.submit_order({"trader_id": "t1", "symbol": "GOOG", "side": "BUY", "price": 100.0, "quantity": 5})
        order_id = res["order"]["order_id"]

        cancel_res = self.node.cancel_order("GOOG", order_id)
        self.assertEqual(cancel_res["status"], STATUS_SUCCESS)
        self.assertEqual(cancel_res["order"]["status"], OrderStatus.CANCELLED)

        # Cancel again should return not_found
        cancel_again = self.node.cancel_order("GOOG", order_id)
        self.assertEqual(cancel_again["status"], STATUS_NOT_FOUND)


class TestPhase2RPCNetwork(unittest.TestCase):
    """Tests 10, 11, 12: Network-level XML-RPC communication and concurrency."""

    @classmethod
    def setUpClass(cls):
        # Bind server to port 0 (OS allocates free port automatically)
        cls.desc = NodeDescriptor(
            node_id="rpc-test-node",
            host="127.0.0.1",
            port=0,
            priority=3,
            role=NodeRole.LEADER,
            is_alive=True,
        )
        cls.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls.temp_db.close()
        cls.node = TradingNode(cls.desc, db_path=cls.temp_db.name)

        cls.server = NodeRPCServer(host="127.0.0.1", port=0)
        cls.server.register_functions(cls.node.get_rpc_methods())
        cls.server.start(block=False)
        time.sleep(0.1)  # Allow background listener to bind

        cls.client = TradingClient(cls.server.url)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.node.close()
        if os.path.exists(cls.temp_db.name):
            try:
                os.remove(cls.temp_db.name)
            except OSError:
                pass

    def test_xmlrpc_basic_communication(self):
        """11: Verify client can query node info, clock, and order book over XML-RPC."""
        info = self.client.get_node_info()
        self.assertEqual(info["status"], STATUS_SUCCESS)
        self.assertEqual(info["node"]["node_id"], "rpc-test-node")

        time_res = self.client.get_lamport_time()
        self.assertEqual(time_res["status"], STATUS_SUCCESS)
        self.assertGreaterEqual(time_res["lamport_ts"], 0)

    def test_xmlrpc_trader_matching_execution(self):
        """12: Complete XML-RPC trading test: BUY 100 @ 150 vs SELL 40 @ 145."""
        client_a = TradingClient(self.server.url)
        client_b = TradingClient(self.server.url)

        # Trader A submits BUY 100 AAPL @ 150
        res_a = client_a.submit_order(
            trader_id="TraderA",
            symbol="AAPL",
            side="BUY",
            price=150.0,
            quantity=100,
        )
        self.assertEqual(res_a["status"], STATUS_SUCCESS)
        self.assertEqual(res_a["order"]["trader_id"], "TraderA")
        self.assertEqual(res_a["order"]["remaining_quantity"], 100)
        self.assertEqual(len(res_a["trades"]), 0)

        # Trader B submits SELL 40 AAPL @ 145
        res_b = client_b.submit_order(
            trader_id="TraderB",
            symbol="AAPL",
            side="SELL",
            price=145.0,
            quantity=40,
        )
        self.assertEqual(res_b["status"], STATUS_SUCCESS)
        self.assertEqual(len(res_b["trades"]), 1)

        trade = res_b["trades"][0]
        self.assertEqual(trade["symbol"], "AAPL")
        self.assertEqual(trade["quantity"], 40)
        self.assertEqual(trade["price"], 150.0)  # Matched at resting order price ($150)
        self.assertEqual(trade["buyer_id"], "TraderA")
        self.assertEqual(trade["seller_id"], "TraderB")

        # Verify book status over RPC
        book_res = client_a.get_order_book("AAPL")
        bids = book_res["order_book"]["bids"]
        self.assertEqual(len(bids), 1)
        self.assertEqual(bids[0]["remaining_quantity"], 60)
        self.assertEqual(len(book_res["order_book"]["asks"]), 0)

    def test_xmlrpc_concurrent_submissions(self):
        """10: Concurrency test: 10 threads submitting 10 orders each over XML-RPC."""
        num_threads = 10
        orders_per_thread = 10
        total_orders = num_threads * orders_per_thread

        assigned_order_ids: List[int] = []
        lock = threading.Lock()

        def worker(thread_idx: int):
            client = TradingClient(self.server.url)
            for i in range(orders_per_thread):
                price = 100.0 + (thread_idx * 10) + i
                res = client.submit_order(
                    trader_id=f"trader_t{thread_idx}",
                    symbol="CONC",
                    side="BUY",
                    price=price,
                    quantity=1,
                )
                if res.get("status") == STATUS_SUCCESS:
                    with lock:
                        assigned_order_ids.append(res["order"]["order_id"])

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify total count and uniqueness
        self.assertEqual(len(assigned_order_ids), total_orders)
        self.assertEqual(len(set(assigned_order_ids)), total_orders, "Duplicate order IDs detected!")

        # Verify against node's persistence
        persisted_orders = self.client.get_all_orders("CONC")
        self.assertEqual(len(persisted_orders["orders"]), total_orders)


if __name__ == "__main__":
    unittest.main()
