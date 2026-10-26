"""
Ring Election Algorithm for Distributed Stock Exchange Simulator.

Implementation of the logical token/message ring election algorithm:
  1. Active nodes are arranged in a deterministic logical ring (ordered by priority or node_id).
  2. An initiator creates an 'ELECTION' message with its own identity/priority and sends it
     to its successor in the ring.
  3. Each receiving node appends its descriptor and forwards the message to the next active successor.
  4. When the message returns to the initiator, the participant with the highest priority is chosen.
  5. The initiator circulates a 'COORDINATOR' message around the ring announcing the winner.
"""

import threading
import xmlrpc.client
from typing import Callable, Dict, List, Optional

from core.clock import LamportClock
from core.models import NodeDescriptor


class RingElection:
    """
    Executes the Ring election algorithm on a node.
    """

    def __init__(
        self,
        local_node: NodeDescriptor,
        get_all_nodes_fn: Callable[[], List[NodeDescriptor]],
        clock: LamportClock,
        send_message_fn: Optional[Callable[[NodeDescriptor, str, Dict], Optional[Dict]]] = None,
    ):
        self.local_node = local_node
        self.get_all_nodes = get_all_nodes_fn
        self.clock = clock
        self.send_message_fn = send_message_fn or self._default_send_message
        self._lock = threading.RLock()
        self.is_electing = False

    def _default_send_message(
        self, target: NodeDescriptor, msg_type: str, data: Dict
    ) -> Optional[Dict]:
        """Send message via XML-RPC to target node."""
        try:
            proxy = xmlrpc.client.ServerProxy(target.url, allow_none=True)
            return proxy.election_message(self.local_node.node_id, msg_type, data)
        except Exception:
            return None

    def _get_ring(self) -> List[NodeDescriptor]:
        """Return all nodes sorted deterministically to form the logical ring."""
        nodes = self.get_all_nodes()
        return sorted(nodes, key=lambda n: n.priority)

    def _get_successors(self) -> List[NodeDescriptor]:
        """Return list of candidate successors starting from next node in the ring."""
        ring = self._get_ring()
        if not ring:
            return []
        local_idx = 0
        for i, n in enumerate(ring):
            if n.node_id == self.local_node.node_id:
                local_idx = i
                break
        # Return all other nodes starting from successor, wrapping around
        successors = []
        n_total = len(ring)
        for offset in range(1, n_total):
            successors.append(ring[(local_idx + offset) % n_total])
        return successors

    def _send_to_next_active(self, msg_type: str, data: Dict) -> Optional[NodeDescriptor]:
        """Attempt to send message to the next reachable successor in the ring."""
        successors = self._get_successors()
        for succ in successors:
            resp = self.send_message_fn(succ, msg_type, data)
            if resp and resp.get("status") in ("ok", "acknowledged", "forwarded"):
                return succ
        return None

    def start_election(self) -> Optional[str]:
        """
        Initiate the Ring election.
        Sends initial candidate list to successor.
        """
        with self._lock:
            self.is_electing = True
            ts = self.clock.increment()

        candidates = [{
            "node_id": self.local_node.node_id,
            "priority": self.local_node.priority,
        }]

        payload = {
            "initiator_id": self.local_node.node_id,
            "candidates": candidates,
            "lamport_ts": ts,
            "algorithm": "ring",
        }

        # Send to next active node in ring
        sent = self._send_to_next_active("ELECTION", payload)
        if not sent:
            # No other node is reachable -> this node wins by default!
            return self.announce_coordinator(self.local_node.node_id)
        return None

    def announce_coordinator(self, leader_id: str) -> str:
        """Circulate COORDINATOR message around the ring."""
        with self._lock:
            self.is_electing = False
            ts = self.clock.increment()

        payload = {
            "leader_id": leader_id,
            "originator_id": self.local_node.node_id,
            "lamport_ts": ts,
            "algorithm": "ring",
        }
        self._send_to_next_active("COORDINATOR", payload)
        return leader_id

    def handle_message(self, sender_id: str, msg_type: str, data: Dict) -> Dict:
        """Handle incoming ring election message."""
        remote_ts = data.get("lamport_ts")
        if remote_ts is not None:
            self.clock.update(int(remote_ts))
        else:
            self.clock.increment()

        if msg_type == "ELECTION":
            initiator_id = data.get("initiator_id")
            candidates = list(data.get("candidates", []))

            # Check if this node is already in candidates (message completed full circle)
            already_visited = any(c["node_id"] == self.local_node.node_id for c in candidates)

            if already_visited:
                # The ring traversal is complete! Select candidate with highest priority
                winner = max(candidates, key=lambda c: c["priority"])
                leader_id = winner["node_id"]
                threading.Thread(target=self.announce_coordinator, args=(leader_id,), daemon=True).start()
                return {"status": "ok", "action": "leader_elected", "leader_id": leader_id}
            else:
                # Append this node and forward to next active in ring
                candidates.append({
                    "node_id": self.local_node.node_id,
                    "priority": self.local_node.priority,
                })
                data["candidates"] = candidates
                data["lamport_ts"] = self.clock.increment()

                threading.Thread(target=self._send_to_next_active, args=("ELECTION", data), daemon=True).start()
                return {"status": "forwarded"}

        elif msg_type == "COORDINATOR":
            leader_id = data.get("leader_id")
            originator_id = data.get("originator_id")

            with self._lock:
                self.is_electing = False

            # If not originated by this node, continue forwarding around the ring
            if originator_id != self.local_node.node_id:
                data["lamport_ts"] = self.clock.increment()
                threading.Thread(target=self._send_to_next_active, args=("COORDINATOR", data), daemon=True).start()

            return {"status": "acknowledged", "leader_id": leader_id}

        return {"status": "unknown_message"}
