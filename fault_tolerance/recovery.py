"""
Recovery Manager for Distributed Stock Exchange Simulator.

Orchestrates failover transitions:
  - Detects leader crashes
  - Initiates leader election
  - Promotes surviving backup to Primary role
  - Re-establishes state authority using local replicated state
"""

import threading
from typing import Callable, Optional

from core.models import NodeDescriptor, NodeRole
from election.coordinator import Coordinator


class RecoveryManager:
    """
    Manages automated failover, election triggering, and backup promotion.
    """

    def __init__(
        self,
        local_node: NodeDescriptor,
        coordinator: Coordinator,
        on_promoted_to_leader_fn: Optional[Callable[[], None]] = None,
    ):
        self.local_node = local_node
        self.coordinator = coordinator
        self.on_promoted_to_leader = on_promoted_to_leader_fn
        self._lock = threading.Lock()

    def handle_leader_failure(self, failed_leader_id: str) -> Optional[str]:
        """
        Invoked when FailureDetector confirms the active leader is unreachable.
        Runs leader election and promotes winner.
        """
        with self._lock:
            # Trigger election across surviving nodes
            new_leader_id = self.coordinator.handle_leader_failure(failed_leader_id)

            if new_leader_id == self.local_node.node_id:
                # This node won the election -> promote to LEADER!
                self.local_node.role = NodeRole.LEADER
                if self.on_promoted_to_leader:
                    self.on_promoted_to_leader()

            return new_leader_id
