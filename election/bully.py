"""
Bully Election Algorithm for Distributed Stock Exchange Simulator.

Implementation of Garcia-Molina's Bully Algorithm (1982):
  1. A node P starts an election if it detects the coordinator has failed.
  2. P sends an 'ELECTION' message to all nodes with higher priority than P.
  3. If no higher-priority node answers within the timeout, P wins the election,
     assumes the coordinator role, and broadcasts a 'COORDINATOR' message to all nodes.
  4. If any higher-priority node responds with 'OK', that higher-priority node takes
     over the election, and P waits for the eventual 'COORDINATOR' announcement.
"""

import threading
import time
import xmlrpc.client
from typing import Callable, Dict, List, Optional

from core.clock import LamportClock
from core.models import NodeDescriptor


class BullyElection:
    """
    Executes the Bully election algorithm on a node.
    """

    def __init__(
        self,
        local_node: NodeDescriptor,
        get_all_nodes_fn: Callable[[], List[NodeDescriptor]],
        clock: LamportClock,
        send_message_fn: Optional[Callable[[NodeDescriptor, str, Dict], Optional[Dict]]] = None,
        timeout: float = 1.0,
    ):
        self.local_node = local_node
        self.get_all_nodes = get_all_nodes_fn
        self.clock = clock
        self.send_message_fn = send_message_fn or self._default_send_message
        self.timeout = timeout
        self._lock = threading.RLock()
        self.is_electing = False

    def _default_send_message(
        self, target: NodeDescriptor, msg_type: str, data: Dict
    ) -> Optional[Dict]:
        """Send election message via XML-RPC to target node."""
        try:
            proxy = xmlrpc.client.ServerProxy(target.url, allow_none=True)
            return proxy.election_message(self.local_node.node_id, msg_type, data)
        except Exception:
            return None

    def start_election(self) -> Optional[str]:
        """
        Initiate the Bully election process.
        Returns the node_id of the elected coordinator, or None if failed.
        """
        with self._lock:
            self.is_electing = True
            ts = self.clock.increment()

        all_nodes = self.get_all_nodes()
        higher_nodes = [
            n for n in all_nodes
            if n.node_id != self.local_node.node_id and n.priority > self.local_node.priority
        ]

        answered_higher = False

        if higher_nodes:
            # Send ELECTION message to all higher-priority nodes
            for target in higher_nodes:
                payload = {
                    "sender_priority": self.local_node.priority,
                    "lamport_ts": ts,
                    "algorithm": "bully",
                }
                resp = self.send_message_fn(target, "ELECTION", payload)
                if resp and resp.get("status") == "ok":
                    answered_higher = True

        with self._lock:
            if not answered_higher:
                # No higher node is alive -> this node is the BULLY winner!
                return self.become_coordinator()
            else:
                # A higher node answered and will coordinate. We wait briefly for announcement.
                self.is_electing = False
                return None

    def become_coordinator(self) -> str:
        """Declare this node as coordinator and announce to all other nodes."""
        with self._lock:
            self.is_electing = False
            ts = self.clock.increment()
            leader_id = self.local_node.node_id

        all_nodes = self.get_all_nodes()
        other_nodes = [n for n in all_nodes if n.node_id != self.local_node.node_id]

        payload = {
            "leader_id": leader_id,
            "leader_priority": self.local_node.priority,
            "lamport_ts": ts,
            "algorithm": "bully",
        }

        for target in other_nodes:
            self.send_message_fn(target, "COORDINATOR", payload)

        return leader_id

    def handle_message(self, sender_id: str, msg_type: str, data: Dict) -> Dict:
        """
        Handle incoming bully election messages.
        """
        remote_ts = data.get("lamport_ts")
        if remote_ts is not None:
            self.clock.update(int(remote_ts))
        else:
            self.clock.increment()

        if msg_type == "ELECTION":
            sender_priority = data.get("sender_priority", 0)
            if self.local_node.priority > sender_priority:
                # Higher priority node responds OK and triggers its own election in background
                threading.Thread(target=self.start_election, daemon=True).start()
                return {"status": "ok", "message": f"{self.local_node.node_id} takes over"}
            return {"status": "ignored"}

        elif msg_type == "COORDINATOR":
            leader_id = data.get("leader_id")
            with self._lock:
                self.is_electing = False
            return {"status": "acknowledged", "leader_id": leader_id}

        return {"status": "unknown_message"}
