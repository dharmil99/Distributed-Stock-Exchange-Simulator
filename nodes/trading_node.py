"""
Trading Node for Distributed Stock Exchange Simulator.

Integrates:
  - Canonical NodeDescriptor (Identity, role, priority, port)
  - Canonical MatchingEngine (Multi-symbol continuous double auction)
  - Canonical LamportClock (Thread-safe logical clock ordering)
  - Canonical SQLitePersistence (Persistent state storage)
  - Election Coordinator (Bully & Ring election algorithms)
  - Replication Manager (Strong Quorum and Eventual consistency)
  - Fault Tolerance & Failure Detector (Heartbeats and automated failover)

Authoritative Leader Rule:
  - Only the current Leader is authoritative for matching and mutating state.
  - If a state-changing request (submit_order, cancel_order) reaches a Backup,
    the backup forwards the request to the active Leader.
  - Read-only queries can be served locally from replicated state.
"""

import os
import threading
import xmlrpc.client
from typing import Any, Callable, Dict, List, Optional

from core.clock import LamportClock
from core.matching_engine import MatchingEngine
from core.models import NodeDescriptor, NodeRole, Order, Trade
from core.persistence import SQLitePersistence
from communication.protocol import (
    SUBMIT_ORDER,
    GET_ORDER_BOOK,
    GET_DEPTH,
    GET_ORDER,
    GET_ALL_ORDERS,
    GET_ALL_TRADES,
    CANCEL_ORDER,
    GET_NODE_INFO,
    GET_LAMPORT_TIME,
    GET_CURRENT_LEADER,
    START_ELECTION,
    ELECTION_MESSAGE,
    HEARTBEAT,
    REPLICATE_EVENT,
    GET_CLUSTER_STATUS,
    STATUS_SUCCESS,
    STATUS_ERROR,
    STATUS_NOT_FOUND,
    make_response,
)
from election.coordinator import Coordinator
from replication.consistency import ConsistencyMode
from replication.replication_manager import ReplicationManager
from fault_tolerance.failure_detector import FailureDetector
from fault_tolerance.recovery import RecoveryManager


class TradingNode:
    """
    An individual Trading Node within the distributed exchange cluster.
    """

    def __init__(
        self,
        descriptor: NodeDescriptor,
        db_path: Optional[str] = None,
        matching_engine: Optional[MatchingEngine] = None,
        clock: Optional[LamportClock] = None,
        peer_nodes: Optional[List[NodeDescriptor]] = None,
        election_algorithm: str = "bully",
        consistency_mode: str = ConsistencyMode.STRONG,
        enable_failure_detector: bool = False,
    ):
        self.descriptor = descriptor
        self.node_id = descriptor.node_id

        # Setup local database file
        if db_path is None:
            data_dir = os.path.join(os.getcwd(), "data")
            os.makedirs(data_dir, exist_ok=True)
            self.db_path = os.path.join(data_dir, f"{self.node_id}.db")
        else:
            self.db_path = db_path
            if self.db_path != ":memory:":
                parent = os.path.dirname(os.path.abspath(self.db_path))
                os.makedirs(parent, exist_ok=True)

        self.persistence = SQLitePersistence(self.db_path)
        self.matching_engine = matching_engine or MatchingEngine()
        self.clock = clock or LamportClock()
        self._lock = threading.RLock()

        # Cluster coordination
        self.coordinator: Optional[Coordinator] = None
        self.replication_manager: Optional[ReplicationManager] = None
        self.failure_detector: Optional[FailureDetector] = None
        self.recovery_manager: Optional[RecoveryManager] = None

        if peer_nodes is not None:
            self.setup_cluster_coordination(
                peer_nodes=peer_nodes,
                election_algorithm=election_algorithm,
                consistency_mode=consistency_mode,
                enable_failure_detector=enable_failure_detector,
            )

    def setup_cluster_coordination(
        self,
        peer_nodes: List[NodeDescriptor],
        election_algorithm: str = "bully",
        consistency_mode: str = ConsistencyMode.STRONG,
        enable_failure_detector: bool = False,
    ):
        """Initialize election, replication, and failure detection with peers."""
        with self._lock:
            self.coordinator = Coordinator(
                local_node=self.descriptor,
                known_nodes=peer_nodes,
                clock=self.clock,
                algorithm=election_algorithm,
                on_leader_changed_fn=self._on_leader_changed,
            )

            self.replication_manager = ReplicationManager(
                local_node=self.descriptor,
                matching_engine=self.matching_engine,
                persistence=self.persistence,
                clock=self.clock,
                get_backup_nodes_fn=self.coordinator.get_all_nodes,
                consistency_mode=consistency_mode,
            )

            self.recovery_manager = RecoveryManager(
                local_node=self.descriptor,
                coordinator=self.coordinator,
                on_promoted_to_leader_fn=self._on_promoted_to_leader,
            )

            if enable_failure_detector:
                self.failure_detector = FailureDetector(
                    local_node_id=self.node_id,
                    get_target_leader_fn=self.coordinator.get_current_leader,
                    on_leader_failed_fn=self.recovery_manager.handle_leader_failure,
                )
                self.failure_detector.start()

    def _on_leader_changed(self, new_leader_id: str):
        """Invoked when coordinator detects or elects a new leader."""
        if new_leader_id == self.node_id:
            self.descriptor.role = NodeRole.LEADER
        else:
            self.descriptor.role = NodeRole.BACKUP

    def _on_promoted_to_leader(self):
        """Invoked when this node wins election and is promoted to Primary."""
        self.descriptor.role = NodeRole.LEADER

    def is_leader(self) -> bool:
        """Check whether this node is currently authoritative Leader."""
        if self.coordinator:
            return self.coordinator.is_leader(self.node_id)
        return self.descriptor.role == NodeRole.LEADER

    # -------------------------------------------------------------------------
    # Core RPC Endpoints
    # -------------------------------------------------------------------------

    def submit_order(self, order_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Submit an order.
        If this node is Backup, forwards the request to the active Leader.
        If this node is Leader, executes matching, persistence, and replication.
        """
        # If not authoritative leader, forward to leader
        if not self.is_leader() and self.coordinator:
            leader = self.coordinator.get_current_leader()
            if leader and leader.node_id != self.node_id:
                try:
                    proxy = xmlrpc.client.ServerProxy(leader.url, allow_none=True)
                    res = proxy.submit_order(order_dict)
                    if isinstance(res, dict):
                        res["forwarded_by"] = self.node_id
                        res["authoritative_leader"] = leader.node_id
                    return res
                except Exception as e:
                    return make_response(
                        status=STATUS_ERROR,
                        error=f"Backup {self.node_id} failed forwarding order to leader {leader.node_id}: {e}",
                        node_id=self.node_id,
                        server_lamport_ts=self.clock.read(),
                    )
            elif not leader:
                return make_response(
                    status=STATUS_ERROR,
                    error=f"Node {self.node_id} is a backup and no leader is currently available.",
                    node_id=self.node_id,
                    server_lamport_ts=self.clock.read(),
                )

        # THIS NODE IS LEADER:
        try:
            order = Order.from_dict(order_dict)
        except Exception as e:
            return make_response(
                status=STATUS_ERROR,
                error=f"Invalid order format: {e}",
                node_id=self.node_id,
                server_lamport_ts=self.clock.read(),
            )

        # Lamport Clock advancement
        if order.client_lamport_ts is not None:
            server_ts = self.clock.update(order.client_lamport_ts)
        else:
            server_ts = self.clock.increment()

        order.server_lamport_ts = server_ts

        # Execute order matching
        processed_order, generated_trades = self.matching_engine.submit_order(
            order, lamport_ts=server_ts
        )

        # Persist the primary submitted order
        self.persistence.save_order(processed_order)

        # Persist generated trades and resting counterparties
        counterparties: List[Order] = []
        for trade in generated_trades:
            self.persistence.save_trade(trade)

            counterparty_id = (
                trade.sell_order_id if processed_order.side == "BUY"
                else trade.buy_order_id
            )
            counterparty = self.matching_engine.get_order(counterparty_id)
            if counterparty:
                self.persistence.save_order(counterparty)
                counterparties.append(counterparty)

        # Replicate state to backups if replication manager configured
        replication_info = None
        if self.replication_manager:
            replication_info = self.replication_manager.replicate_order_submission(
                order=processed_order,
                trades=generated_trades,
                counterparties=counterparties,
            )

        resp_data = {
            "order": processed_order.to_dict(),
            "trades": [t.to_dict() for t in generated_trades],
        }
        if replication_info:
            resp_data["replication"] = replication_info

        return make_response(
            status=STATUS_SUCCESS,
            data=resp_data,
            node_id=self.node_id,
            server_lamport_ts=server_ts,
        )

    def cancel_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        """
        Cancel an open order.
        If this node is Backup, forwards the request to the active Leader.
        """
        if not self.is_leader() and self.coordinator:
            leader = self.coordinator.get_current_leader()
            if leader and leader.node_id != self.node_id:
                try:
                    proxy = xmlrpc.client.ServerProxy(leader.url, allow_none=True)
                    return proxy.cancel_order(symbol, int(order_id))
                except Exception as e:
                    return make_response(
                        status=STATUS_ERROR,
                        error=f"Backup {self.node_id} failed forwarding cancel to leader {leader.node_id}: {e}",
                        node_id=self.node_id,
                    )

        server_ts = self.clock.increment()
        cancelled = self.matching_engine.cancel_order(symbol, int(order_id))
        if cancelled:
            self.persistence.save_order(cancelled)
            if self.replication_manager:
                self.replication_manager.replicate_order_cancellation(symbol, int(order_id))
            return make_response(
                status=STATUS_SUCCESS,
                data={"order": cancelled.to_dict()},
                node_id=self.node_id,
                server_lamport_ts=server_ts,
            )
        return make_response(
            status=STATUS_NOT_FOUND,
            error=f"Order #{order_id} not found or already filled/cancelled",
            node_id=self.node_id,
            server_lamport_ts=server_ts,
        )

    def get_order_book(self, symbol: str) -> Dict[str, Any]:
        """Remote endpoint: Return active order book snapshot for the symbol."""
        server_ts = self.clock.increment()
        snapshot = self.matching_engine.get_order_book(symbol)
        return make_response(
            status=STATUS_SUCCESS,
            data={"order_book": snapshot},
            node_id=self.node_id,
            server_lamport_ts=server_ts,
        )

    def get_depth(self, symbol: str) -> Dict[str, Any]:
        """Remote endpoint: Return aggregate price-level depth for the symbol."""
        server_ts = self.clock.increment()
        depth = self.matching_engine.get_depth(symbol)
        return make_response(
            status=STATUS_SUCCESS,
            data={"depth": depth},
            node_id=self.node_id,
            server_lamport_ts=server_ts,
        )

    def get_order(self, order_id: int) -> Dict[str, Any]:
        """Remote endpoint: Retrieve a specific order by ID."""
        server_ts = self.clock.increment()
        order = self.persistence.get_order(int(order_id))
        if not order:
            order = self.matching_engine.get_order(int(order_id))

        if not order:
            return make_response(
                status=STATUS_NOT_FOUND,
                error=f"Order #{order_id} not found",
                node_id=self.node_id,
                server_lamport_ts=server_ts,
            )

        return make_response(
            status=STATUS_SUCCESS,
            data={"order": order.to_dict()},
            node_id=self.node_id,
            server_lamport_ts=server_ts,
        )

    def get_all_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Remote endpoint: Retrieve all historical orders, optionally filtered by symbol."""
        server_ts = self.clock.increment()
        orders = self.persistence.get_orders(symbol=symbol)
        return make_response(
            status=STATUS_SUCCESS,
            data={"orders": [o.to_dict() for o in orders]},
            node_id=self.node_id,
            server_lamport_ts=server_ts,
        )

    def get_all_trades(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Remote endpoint: Retrieve all executed trades, optionally filtered by symbol."""
        server_ts = self.clock.increment()
        trades = self.persistence.get_trades(symbol=symbol)
        return make_response(
            status=STATUS_SUCCESS,
            data={"trades": [t.to_dict() for t in trades]},
            node_id=self.node_id,
            server_lamport_ts=server_ts,
        )

    def get_node_info(self) -> Dict[str, Any]:
        """Remote endpoint: Return node descriptor, status, and role."""
        node_dict = self.descriptor.to_dict()
        node_dict["is_leader"] = self.is_leader()
        node_dict["current_leader_id"] = (
            self.coordinator.get_current_leader_id()
            if self.coordinator else (self.node_id if self.is_leader() else None)
        )
        return make_response(
            status=STATUS_SUCCESS,
            data={
                "node": node_dict,
                "consistency_mode": (
                    self.replication_manager.consistency_mode
                    if self.replication_manager else "none"
                ),
                "election_algorithm": (
                    self.coordinator.algorithm if self.coordinator else "none"
                ),
            },
            node_id=self.node_id,
            server_lamport_ts=self.clock.read(),
        )

    def get_lamport_time(self) -> Dict[str, Any]:
        """Remote endpoint: Return current logical clock reading."""
        return make_response(
            status=STATUS_SUCCESS,
            data={"lamport_ts": self.clock.read()},
            node_id=self.node_id,
            server_lamport_ts=self.clock.read(),
        )

    # -------------------------------------------------------------------------
    # Cluster Coordination RPC Endpoints
    # -------------------------------------------------------------------------

    def heartbeat(self) -> Dict[str, Any]:
        """Remote endpoint: Heartbeat liveness and role query."""
        return make_response(
            status=STATUS_SUCCESS,
            data={
                "alive": True,
                "role": self.descriptor.role,
                "is_leader": self.is_leader(),
            },
            node_id=self.node_id,
            server_lamport_ts=self.clock.read(),
        )

    def replicate_event(self, event_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Remote endpoint: Receive and apply replicated event on Backup."""
        if self.replication_manager:
            return self.replication_manager.handle_replicate_event(event_payload)
        return make_response(status=STATUS_SUCCESS, node_id=self.node_id)

    def election_message(self, sender_id: str, msg_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Remote endpoint: Handle Bully or Ring election messages."""
        if self.coordinator:
            return self.coordinator.handle_election_message(sender_id, msg_type, data)
        return {"status": "ignored"}

    def start_election(self) -> Dict[str, Any]:
        """Remote endpoint: Trigger an election from outside or peer."""
        if self.coordinator:
            winner = self.coordinator.start_election()
            return make_response(
                status=STATUS_SUCCESS,
                data={"elected_leader": winner},
                node_id=self.node_id,
                server_lamport_ts=self.clock.read(),
            )
        return make_response(status=STATUS_ERROR, error="No coordinator configured")

    def get_current_leader(self) -> Dict[str, Any]:
        """Remote endpoint: Query who this node believes is the current leader."""
        if self.coordinator:
            leader = self.coordinator.get_current_leader()
            if leader:
                return make_response(status=STATUS_SUCCESS, data={"leader": leader.to_dict()})
        return make_response(
            status=STATUS_SUCCESS,
            data={"leader": self.descriptor.to_dict() if self.is_leader() else None},
        )

    def get_cluster_status(self) -> Dict[str, Any]:
        """Remote endpoint: Return status of all nodes known to coordinator."""
        if self.coordinator:
            nodes_data = [n.to_dict() for n in self.coordinator.get_all_nodes()]
            return make_response(
                status=STATUS_SUCCESS,
                data={
                    "nodes": nodes_data,
                    "leader": self.coordinator.get_current_leader_id(),
                },
                node_id=self.node_id,
            )
        return make_response(
            status=STATUS_SUCCESS,
            data={"nodes": [self.descriptor.to_dict()], "leader": self.node_id},
            node_id=self.node_id,
        )

    def get_rpc_methods(self) -> Dict[str, Callable]:
        """Returns map of RPC method names to bound callable methods."""
        return {
            SUBMIT_ORDER: self.submit_order,
            GET_ORDER_BOOK: self.get_order_book,
            GET_DEPTH: self.get_depth,
            GET_ORDER: self.get_order,
            GET_ALL_ORDERS: self.get_all_orders,
            GET_ALL_TRADES: self.get_all_trades,
            CANCEL_ORDER: self.cancel_order,
            GET_NODE_INFO: self.get_node_info,
            GET_LAMPORT_TIME: self.get_lamport_time,
            GET_CURRENT_LEADER: self.get_current_leader,
            START_ELECTION: self.start_election,
            ELECTION_MESSAGE: self.election_message,
            HEARTBEAT: self.heartbeat,
            REPLICATE_EVENT: self.replicate_event,
            GET_CLUSTER_STATUS: self.get_cluster_status,
        }

    def close(self):
        """Release database and system resources."""
        if self.failure_detector:
            self.failure_detector.stop()
        self.persistence.close()
