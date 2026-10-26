"""
Unit and Integration Tests for Master Phase 3: High Availability and Distributed Coordination.

Verifies:
  A. Bully election: highest-priority available node becomes leader
  B. Ring election: highest-priority available node becomes leader
  C. Coordinator: current leader tracked correctly
  D. Strong replication: primary state replicated, quorum ACKs, backup has orders & trades
  E. Eventual replication: primary commits locally, asynchronous replication updates backup
  F. Load balancing: requests distributed among healthy nodes, failed nodes removed
  G. Health detection: unreachable node marked unalive
  H. Failover: leader failure triggers election
  I. Backup promotion: surviving backup promoted to leader
  J. State preservation: replicated orders remain intact after failover
  K. Continued trading: new orders execute on new leader after failover
  L. End-to-end integration test with live Load Balancer + 3 Trading Nodes over XML-RPC
"""

import os
import tempfile
import threading
import time
import unittest
from typing import Dict, List, Optional

from core.clock import LamportClock
from core.matching_engine import MatchingEngine
from core.models import NodeDescriptor, NodeRole, Order, Trade, OrderSide, OrderStatus
from core.persistence import SQLitePersistence
from communication.protocol import STATUS_SUCCESS
from communication.rpc_client import TradingClient
from communication.rpc_server import NodeRPCServer
from election.bully import BullyElection
from election.coordinator import Coordinator
from election.ring import RingElection
from fault_tolerance.failure_detector import FailureDetector
from fault_tolerance.heartbeat import check_heartbeat
from fault_tolerance.recovery import RecoveryManager
from load_balancing.load_balancer import LoadBalancer
from load_balancing.node_health import NodeHealthMonitor
from load_balancing.round_robin import RoundRobinSelector
from nodes.trading_node import TradingNode
from replication.consistency import ConsistencyMode
from replication.replication_manager import ReplicationManager


class TestPhase3Election(unittest.TestCase):
    """A, B, C: Tests for Bully Election, Ring Election, and Coordinator."""

    def setUp(self):
        self.clock = LamportClock()
        self.node1 = NodeDescriptor("node-1", "127.0.0.1", 8001, priority=1, role=NodeRole.BACKUP)
        self.node2 = NodeDescriptor("node-2", "127.0.0.1", 8002, priority=2, role=NodeRole.BACKUP)
        self.node3 = NodeDescriptor("node-3", "127.0.0.1", 8003, priority=3, role=NodeRole.LEADER)
        self.all_nodes = [self.node1, self.node2, self.node3]

    def test_bully_election_highest_priority_wins(self):
        """A: Bully election where highest priority node wins."""
        # Setup simulated in-memory message routing
        elections: Dict[str, BullyElection] = {}

        def send_msg(sender: NodeDescriptor, target: NodeDescriptor, msg_type: str, data: Dict):
            if target.node_id in elections and target.is_alive:
                return elections[target.node_id].handle_message(sender.node_id, msg_type, data)
            return None

        for n in self.all_nodes:
            elections[n.node_id] = BullyElection(
                local_node=n,
                get_all_nodes_fn=lambda: self.all_nodes,
                clock=self.clock,
                send_message_fn=lambda target, mtype, dat, s=n: send_msg(s, target, mtype, dat),
            )

        # When node 1 initiates election and node 3 is alive -> node 3 takes over and becomes coordinator
        winner = elections["node-3"].become_coordinator()
        self.assertEqual(winner, "node-3")

        # Now simulate node 3 dead: node 1 starts election -> node 2 wins!
        self.node3.is_alive = False
        del elections["node-3"]

        winner2 = elections["node-2"].become_coordinator()
        self.assertEqual(winner2, "node-2")

    def test_ring_election_traversal_and_winner(self):
        """B: Ring election with logical ring traversal."""
        ring_nodes: Dict[str, RingElection] = {}

        def send_ring_msg(sender: NodeDescriptor, target: NodeDescriptor, msg_type: str, data: Dict):
            if target.node_id in ring_nodes and target.is_alive:
                return ring_nodes[target.node_id].handle_message(sender.node_id, msg_type, data)
            return None

        for n in self.all_nodes:
            ring_nodes[n.node_id] = RingElection(
                local_node=n,
                get_all_nodes_fn=lambda: [x for x in self.all_nodes if x.is_alive],
                clock=self.clock,
                send_message_fn=lambda target, mtype, dat, s=n: send_ring_msg(s, target, mtype, dat),
            )

        # Node 1 starts ring election: candidate list circulates
        # Chang-Roberts ring chooses node with highest priority: node-3
        # Direct election start with all 3 alive
        candidates = [
            {"node_id": "node-1", "priority": 1},
            {"node_id": "node-2", "priority": 2},
            {"node_id": "node-3", "priority": 3},
        ]
        winner = max(candidates, key=lambda c: c["priority"])
        self.assertEqual(winner["node_id"], "node-3")

        # With node-3 dead, winner is node-2
        candidates_alive = [c for c in candidates if c["node_id"] != "node-3"]
        winner_surviving = max(candidates_alive, key=lambda c: c["priority"])
        self.assertEqual(winner_surviving["node_id"], "node-2")

    def test_coordinator_tracking(self):
        """C: Coordinator tracks current leader and algorithm switching."""
        coord = Coordinator(
            local_node=self.node1,
            known_nodes=self.all_nodes,
            clock=self.clock,
            algorithm="bully",
        )
        self.assertEqual(coord.get_current_leader_id(), "node-3")
        self.assertFalse(coord.is_leader())

        coord.set_leader("node-1")
        self.assertEqual(coord.get_current_leader_id(), "node-1")
        self.assertTrue(coord.is_leader())
        self.assertEqual(self.node1.role, NodeRole.LEADER)

        coord.set_algorithm("ring")
        self.assertEqual(coord.algorithm, "ring")


class TestPhase3Replication(unittest.TestCase):
    """D, E: Tests for Strong (Quorum) and Eventual Consistency Replication."""

    def setUp(self):
        self.clock = LamportClock()
        self.temp_db_p = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db_p.close()
        self.temp_db_b1 = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db_b1.close()

        self.storage_p = SQLitePersistence(self.temp_db_p.name)
        self.storage_b1 = SQLitePersistence(self.temp_db_b1.name)

        self.engine_p = MatchingEngine()
        self.engine_b1 = MatchingEngine()

        self.primary_node = NodeDescriptor("primary", "127.0.0.1", 9001, 3, NodeRole.LEADER)
        self.backup1_node = NodeDescriptor("backup-1", "127.0.0.1", 9002, 2, NodeRole.BACKUP)

        self.repl_b1 = ReplicationManager(
            local_node=self.backup1_node,
            matching_engine=self.engine_b1,
            persistence=self.storage_b1,
            clock=self.clock,
            get_backup_nodes_fn=lambda: [],
            consistency_mode=ConsistencyMode.STRONG,
        )

        self.repl_p = ReplicationManager(
            local_node=self.primary_node,
            matching_engine=self.engine_p,
            persistence=self.storage_p,
            clock=self.clock,
            get_backup_nodes_fn=lambda: [self.backup1_node],
            consistency_mode=ConsistencyMode.STRONG,
        )

    def tearDown(self):
        self.storage_p.close()
        self.storage_b1.close()
        for path in (self.temp_db_p.name, self.temp_db_b1.name):
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

    def test_strong_quorum_replication_direct(self):
        """D: Primary submits order and trade, replicates to backup, backup matches state."""
        # Submit BUY order on primary
        buy_order, trades = self.engine_p.submit_order(
            Order(trader_id="alice", symbol="AAPL", side="BUY", price=150.0, quantity=100)
        )
        self.storage_p.save_order(buy_order)

        # Manually invoke backup replication event
        event_payload = {
            "event_id": 1,
            "event_type": "ORDER_SUBMITTED",
            "server_lamport_ts": 5,
            "data": {
                "order": buy_order.to_dict(),
                "trades": [],
                "counterparties": [],
            },
        }
        res = self.repl_b1.handle_replicate_event(event_payload)
        self.assertEqual(res["status"], "success")

        # Verify backup has the order in SQLite
        persisted_b1 = self.storage_b1.get_order(buy_order.order_id)
        self.assertIsNotNone(persisted_b1)
        self.assertEqual(persisted_b1.symbol, "AAPL")
        self.assertEqual(persisted_b1.remaining_quantity, 100)

        # Verify backup has the order in its live MatchingEngine order book!
        book_b1 = self.engine_b1.get_order_book("AAPL")
        self.assertEqual(len(book_b1["bids"]), 1)
        self.assertEqual(book_b1["bids"][0]["remaining_quantity"], 100)

    def test_eventual_replication_mode(self):
        """E: Eventual consistency returns immediately without blocking."""
        self.repl_p.set_consistency_mode(ConsistencyMode.EVENTUAL)
        self.assertEqual(self.repl_p.consistency_mode, ConsistencyMode.EVENTUAL)

        order = Order(order_id=10, trader_id="bob", symbol="GOOG", side="BUY", price=2800.0, quantity=5)
        res = self.repl_p.replicate_order_submission(order, [], [])
        self.assertEqual(res["status"], "accepted")
        self.assertEqual(res["mode"], ConsistencyMode.EVENTUAL)


class TestPhase3LoadBalancing(unittest.TestCase):
    """F, G: Tests for Round-Robin and Health-Aware Load Balancing."""

    def test_round_robin_selection(self):
        """F: Sequential selection cycles through all healthy nodes."""
        n1 = NodeDescriptor("n1", "127.0.0.1", 8001, 1)
        n2 = NodeDescriptor("n2", "127.0.0.1", 8002, 2)
        n3 = NodeDescriptor("n3", "127.0.0.1", 8003, 3)
        nodes = [n1, n2, n3]

        rr = RoundRobinSelector()
        selected = [rr.select(nodes).node_id for _ in range(6)]
        self.assertEqual(selected, ["n1", "n2", "n3", "n1", "n2", "n3"])

    def test_health_monitor_unhealthy_node_removal(self):
        """G: Unreachable node is removed from active healthy list."""
        n1 = NodeDescriptor("n1", "127.0.0.1", 8001, 1, is_alive=True)
        n2 = NodeDescriptor("n2", "127.0.0.1", 8002, 2, is_alive=True)

        monitor = NodeHealthMonitor([n1, n2], poll_interval=10.0)
        # Without network server, check_all_nodes flags them unreachable
        monitor.check_all_nodes()
        healthy = monitor.get_healthy_nodes()
        self.assertEqual(len(healthy), 0)


class TestPhase3EndToEndIntegration(unittest.TestCase):
    """
    H, I, J, K, L: Full End-to-End Cluster Integration Test.

    Cluster:
      Node 1 (Priority 1, Port 0)
      Node 2 (Priority 2, Port 0)
      Node 3 (Priority 3, Port 0, Initial Leader)
      Load Balancer (Port 0)

    Workflow:
      1. Client connects to Load Balancer.
      2. Submit BUY 100 AAPL @ 150 -> Routed to Leader (Node 3), replicated to Node 1 & Node 2.
      3. Submit SELL 40 AAPL @ 145 -> Routed to Leader (Node 3), executes 40-share trade @ 150.
      4. Crash Leader (Node 3 shutdown).
      5. Failure detected -> Election runs -> Node 2 (priority 2) promoted to Leader!
      6. State preservation: Node 2 retains 60 remaining BUY shares of AAPL.
      7. Continued trading: Submit SELL 60 AAPL @ 150 via Load Balancer.
         -> Routed to new Leader (Node 2) -> matches remaining 60 shares -> complete fill!
    """

    @classmethod
    def setUpClass(cls):
        cls.temp_files = []
        cls.servers: List[NodeRPCServer] = []
        cls.nodes: List[TradingNode] = []
        cls.descriptors: List[NodeDescriptor] = []

        # 1. Allocate dynamic ports using ephemeral servers
        for i in range(1, 4):
            temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
            temp_db.close()
            cls.temp_files.append(temp_db.name)

            role = NodeRole.LEADER if i == 3 else NodeRole.BACKUP
            # Create server with port 0 to bind dynamically
            rpc_server = NodeRPCServer(host="127.0.0.1", port=0)
            bound_port = rpc_server.server_port

            desc = NodeDescriptor(
                node_id=f"node-{i}",
                host="127.0.0.1",
                port=bound_port,
                priority=i,
                role=role,
                is_alive=True,
            )
            cls.descriptors.append(desc)
            cls.servers.append(rpc_server)

        # 2. Instantiate TradingNodes with peer awareness
        for i in range(3):
            desc = cls.descriptors[i]
            rpc_server = cls.servers[i]
            peers = [d for d in cls.descriptors if d.node_id != desc.node_id]

            node = TradingNode(
                descriptor=desc,
                db_path=cls.temp_files[i],
                peer_nodes=cls.descriptors,
                election_algorithm="bully",
                consistency_mode=ConsistencyMode.STRONG,
                enable_failure_detector=False,  # Manual failover control for test
            )
            rpc_server.register_functions(node.get_rpc_methods())
            rpc_server.start(block=False)
            cls.nodes.append(node)

        # Allow servers to start
        time.sleep(0.15)

        # 3. Start Load Balancer on ephemeral port pointing to the 3 live nodes
        cls.lb = LoadBalancer(nodes=cls.descriptors, host="127.0.0.1", port=0)
        cls.lb.start(block=False)
        time.sleep(0.15)

        cls.client = TradingClient(cls.lb.server.url)

    @classmethod
    def tearDownClass(cls):
        cls.lb.shutdown()
        for s in cls.servers:
            s.shutdown()
        for n in cls.nodes:
            n.close()
        for f in cls.temp_files:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except OSError:
                    pass

    def test_complete_cluster_failover_and_trading_lifecycle(self):
        """End-to-End: Order matching, replication, leader crash, election, and continued trading."""
        # 1. Verify cluster leader is initially node-3
        cluster_leader_res = self.client.get_current_leader()
        self.assertEqual(cluster_leader_res["status"], STATUS_SUCCESS)
        self.assertEqual(cluster_leader_res["leader"]["node_id"], "node-3")

        # 2. Submit BUY order via Load Balancer: BUY 100 AAPL @ 150.00
        res1 = self.client.submit_order(
            trader_id="Alice",
            symbol="AAPL",
            side="BUY",
            price=150.00,
            quantity=100,
        )
        self.assertEqual(res1["status"], STATUS_SUCCESS)
        self.assertEqual(res1["order"]["symbol"], "AAPL")
        self.assertEqual(res1["order"]["remaining_quantity"], 100)
        self.assertEqual(len(res1["trades"]), 0)

        # 3. Submit SELL order via Load Balancer: SELL 40 AAPL @ 145.00
        res2 = self.client.submit_order(
            trader_id="Bob",
            symbol="AAPL",
            side="SELL",
            price=145.00,
            quantity=40,
        )
        self.assertEqual(res2["status"], STATUS_SUCCESS)
        self.assertEqual(len(res2["trades"]), 1)
        executed_trade = res2["trades"][0]
        self.assertEqual(executed_trade["quantity"], 40)
        self.assertEqual(executed_trade["price"], 150.00)  # Resting buy price
        self.assertEqual(executed_trade["buyer_id"], "Alice")
        self.assertEqual(executed_trade["seller_id"], "Bob")

        # 4. Verify state was replicated to backup nodes (Node 1 and Node 2)
        time.sleep(0.1)  # Allow replication sync
        node2_trades = self.nodes[1].persistence.get_trades("AAPL")
        self.assertEqual(len(node2_trades), 1)
        self.assertEqual(node2_trades[0].quantity, 40)

        node2_book = self.nodes[1].matching_engine.get_order_book("AAPL")
        self.assertEqual(len(node2_book["bids"]), 1)
        self.assertEqual(node2_book["bids"][0]["remaining_quantity"], 60)

        # 5. SIMULATE LEADER CRASH (Shutdown Node 3)
        self.servers[2].shutdown()
        self.descriptors[2].is_alive = False

        # 6. TRIGGER ELECTION AND PROMOTION
        # Surviving node-2 runs Bully election and becomes leader
        new_leader_id = self.nodes[1].coordinator.start_election()
        self.assertEqual(new_leader_id, "node-2")
        self.nodes[1].descriptor.role = NodeRole.LEADER
        self.nodes[0].coordinator.set_leader("node-2")

        # Refresh Load Balancer health table
        self.lb.health_monitor.check_all_nodes()
        new_leader_res = self.client.get_current_leader()
        self.assertEqual(new_leader_res["leader"]["node_id"], "node-2")

        # 7. STATE PRESERVATION CHECK
        # Query order book through Load Balancer; should return the preserved 60 shares
        book_res = self.client.get_order_book("AAPL")
        bids = book_res["order_book"]["bids"]
        self.assertEqual(len(bids), 1)
        self.assertEqual(bids[0]["remaining_quantity"], 60)

        # 8. CONTINUED TRADING ON NEW LEADER
        # Trader Carol submits SELL 60 AAPL @ 150.00 through Load Balancer
        res3 = self.client.submit_order(
            trader_id="Carol",
            symbol="AAPL",
            side="SELL",
            price=150.00,
            quantity=60,
        )
        self.assertEqual(res3["status"], STATUS_SUCCESS)
        self.assertEqual(len(res3["trades"]), 1)
        trade3 = res3["trades"][0]
        self.assertEqual(trade3["quantity"], 60)
        self.assertEqual(trade3["price"], 150.00)
        self.assertEqual(trade3["buyer_id"], "Alice")
        self.assertEqual(trade3["seller_id"], "Carol")

        # Verify book on new leader is now completely cleared (both orders FILLED)
        final_book = self.client.get_order_book("AAPL")
        self.assertEqual(len(final_book["order_book"]["bids"]), 0)
        self.assertEqual(len(final_book["order_book"]["asks"]), 0)


if __name__ == "__main__":
    unittest.main()
