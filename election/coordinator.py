"""
Coordinator abstraction for Distributed Stock Exchange Simulator.

Manages cluster leadership, election algorithm selection (Bully vs. Ring),
and state synchronization across node instances.
"""

import threading
from typing import Callable, Dict, List, Optional

from core.clock import LamportClock
from core.models import NodeDescriptor, NodeRole
from election.bully import BullyElection
from election.ring import RingElection


class Coordinator:
    """
    Coordinator manager for a TradingNode.
    Tracks cluster nodes, current leader, and coordinates leader elections.
    """

    def __init__(
        self,
        local_node: NodeDescriptor,
        known_nodes: Optional[List[NodeDescriptor]] = None,
        clock: Optional[LamportClock] = None,
        algorithm: str = "bully",
        on_leader_changed_fn: Optional[Callable[[str], None]] = None,
    ):
        self.local_node = local_node
        self.clock = clock or LamportClock()
        self.algorithm = algorithm.lower().strip()
        self.on_leader_changed_fn = on_leader_changed_fn

        self._lock = threading.RLock()
        self._nodes: Dict[str, NodeDescriptor] = {}
        if known_nodes:
            for n in known_nodes:
                self._nodes[n.node_id] = n
        self._nodes[self.local_node.node_id] = self.local_node

        self._current_leader_id: Optional[str] = None
        # Default leader is highest priority configured node
        if self._nodes:
            default_leader = max(self._nodes.values(), key=lambda n: n.priority)
            self._current_leader_id = default_leader.node_id

        # Instantiate algorithm implementations
        self._bully = BullyElection(
            local_node=self.local_node,
            get_all_nodes_fn=self.get_all_nodes,
            clock=self.clock,
        )
        self._ring = RingElection(
            local_node=self.local_node,
            get_all_nodes_fn=self.get_all_nodes,
            clock=self.clock,
        )

    def get_all_nodes(self) -> List[NodeDescriptor]:
        """Return list of all known cluster nodes."""
        with self._lock:
            return list(self._nodes.values())

    def register_node(self, node: NodeDescriptor):
        """Register or update a cluster node descriptor."""
        with self._lock:
            self._nodes[node.node_id] = node

    def set_algorithm(self, algorithm: str):
        """Set election algorithm ('bully' or 'ring')."""
        with self._lock:
            a = algorithm.lower().strip()
            if a not in ("bully", "ring"):
                raise ValueError(f"Unknown election algorithm: {algorithm}. Must be 'bully' or 'ring'.")
            self.algorithm = a

    def get_current_leader_id(self) -> Optional[str]:
        with self._lock:
            return self._current_leader_id

    def get_current_leader(self) -> Optional[NodeDescriptor]:
        """Return NodeDescriptor for the current leader."""
        with self._lock:
            if not self._current_leader_id:
                return None
            return self._nodes.get(self._current_leader_id)

    def is_leader(self, node_id: Optional[str] = None) -> bool:
        """Check if specified node (or local node) is the current leader."""
        target_id = node_id or self.local_node.node_id
        with self._lock:
            return self._current_leader_id == target_id

    def set_leader(self, leader_id: str):
        """Set the current leader and update local node role."""
        with self._lock:
            prev = self._current_leader_id
            self._current_leader_id = leader_id

            # Update local node role
            if leader_id == self.local_node.node_id:
                self.local_node.role = NodeRole.LEADER
            else:
                self.local_node.role = NodeRole.BACKUP

            if leader_id in self._nodes:
                self._nodes[leader_id].role = NodeRole.LEADER

            # Mark previous leader as backup if different
            if prev and prev != leader_id and prev in self._nodes:
                self._nodes[prev].role = NodeRole.BACKUP

            if self.on_leader_changed_fn and prev != leader_id:
                self.on_leader_changed_fn(leader_id)

    def start_election(self) -> Optional[str]:
        """
        Trigger an election using the configured algorithm.
        Returns the elected leader_id.
        """
        with self._lock:
            algo = self.algorithm

        if algo == "ring":
            elected = self._ring.start_election()
        else:
            elected = self._bully.start_election()

        if elected:
            self.set_leader(elected)
        return elected

    def handle_election_message(self, sender_id: str, msg_type: str, data: Dict) -> Dict:
        """
        Process an incoming election message from another node.
        Dispatches to bully or ring algorithm based on message payload or configuration.
        """
        algo = data.get("algorithm", self.algorithm).lower()
        if algo == "ring":
            resp = self._ring.handle_message(sender_id, msg_type, data)
        else:
            resp = self._bully.handle_message(sender_id, msg_type, data)

        # If a coordinator message was acknowledged or decided, update local leader
        if msg_type == "COORDINATOR":
            leader_id = data.get("leader_id")
            if leader_id:
                self.set_leader(leader_id)

        if resp.get("action") == "leader_elected" and "leader_id" in resp:
            self.set_leader(resp["leader_id"])

        return resp

    def handle_leader_failure(self, failed_leader_id: str) -> Optional[str]:
        """
        Handle detection of leader failure.
        Marks failed node as unalive and triggers a new election.
        """
        with self._lock:
            if failed_leader_id in self._nodes:
                self._nodes[failed_leader_id].is_alive = False

            if self._current_leader_id == failed_leader_id:
                self._current_leader_id = None

        return self.start_election()
