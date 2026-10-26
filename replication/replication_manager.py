"""
Replication Manager for Distributed Stock Exchange Simulator.

Coordinates replication duties for a node:
  - If acting as Primary (Leader): broadcasts state events via PrimaryReplicationCoordinator
  - If acting as Backup: applies incoming events via BackupReplicationReceiver
"""

import threading
from typing import Any, Callable, Dict, List, Optional

from core.clock import LamportClock
from core.matching_engine import MatchingEngine
from core.models import NodeDescriptor, NodeRole, Order, Trade
from core.persistence import SQLitePersistence
from replication.backup import BackupReplicationReceiver
from replication.consistency import ConsistencyMode
from replication.primary import PrimaryReplicationCoordinator


class ReplicationManager:
    """
    Unified manager for state replication across the cluster.
    """

    def __init__(
        self,
        local_node: NodeDescriptor,
        matching_engine: MatchingEngine,
        persistence: SQLitePersistence,
        clock: LamportClock,
        get_backup_nodes_fn: Callable[[], List[NodeDescriptor]],
        consistency_mode: str = ConsistencyMode.STRONG,
        write_quorum_backups: int = 1,
    ):
        self.local_node = local_node
        self.matching_engine = matching_engine
        self.persistence = persistence
        self.clock = clock
        self.consistency_mode = ConsistencyMode.normalize(consistency_mode)
        self._lock = threading.RLock()

        self.primary = PrimaryReplicationCoordinator(
            local_node=self.local_node,
            get_backup_nodes_fn=get_backup_nodes_fn,
            clock=self.clock,
            write_quorum_backups=write_quorum_backups,
        )

        self.backup = BackupReplicationReceiver(
            node_id=self.local_node.node_id,
            matching_engine=self.matching_engine,
            persistence=self.persistence,
            clock=self.clock,
        )

    def set_consistency_mode(self, mode: str):
        """Update consistency mode ('strong' or 'eventual')."""
        with self._lock:
            self.consistency_mode = ConsistencyMode.normalize(mode)

    def replicate_order_submission(
        self,
        order: Order,
        trades: List[Trade],
        counterparties: List[Order],
    ) -> Dict[str, Any]:
        """Broadcast order submission and fills to backup replicas."""
        data = {
            "order": order.to_dict(),
            "trades": [t.to_dict() for t in trades],
            "counterparties": [cp.to_dict() for cp in counterparties],
        }
        return self.primary.replicate(
            event_type="ORDER_SUBMITTED",
            data=data,
            mode=self.consistency_mode,
        )

    def replicate_order_cancellation(self, symbol: str, order_id: int) -> Dict[str, Any]:
        """Broadcast order cancellation to backup replicas."""
        data = {
            "symbol": symbol,
            "order_id": int(order_id),
        }
        return self.primary.replicate(
            event_type="ORDER_CANCELLED",
            data=data,
            mode=self.consistency_mode,
        )

    def handle_replicate_event(self, event_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle incoming replication RPC on backup replica."""
        return self.backup.apply_event(event_payload)
