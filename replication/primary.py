"""
Primary Replication Coordinator for Distributed Stock Exchange Simulator.

Coordinates state replication from the current Leader (Primary) to healthy Backup replicas.
Supports both synchronous Quorum (Strong) and asynchronous (Eventual) replication.
"""

import threading
import time
import xmlrpc.client
from typing import Any, Callable, Dict, List, Optional

from core.clock import LamportClock
from core.models import NodeDescriptor
from replication.consistency import ConsistencyMode


class PrimaryReplicationCoordinator:
    """
    Manages fan-out of state changes from Primary to Backups.
    """

    def __init__(
        self,
        local_node: NodeDescriptor,
        get_backup_nodes_fn: Callable[[], List[NodeDescriptor]],
        clock: LamportClock,
        write_quorum_backups: int = 1,
        timeout_sec: float = 2.0,
    ):
        self.local_node = local_node
        self.get_backup_nodes = get_backup_nodes_fn
        self.clock = clock
        self.write_quorum_backups = write_quorum_backups
        self.timeout_sec = timeout_sec
        self._lock = threading.Lock()
        self._next_event_id = 1

    def _send_to_backup(self, backup: NodeDescriptor, event: Dict[str, Any], ack_tracker: Dict):
        """Send replicate_event to a single backup replica."""
        try:
            proxy = xmlrpc.client.ServerProxy(backup.url, allow_none=True)
            resp = proxy.replicate_event(event)
            if resp and resp.get("status") == "success":
                with ack_tracker["lock"]:
                    ack_tracker["count"] += 1
                    ack_tracker["nodes"].append(backup.node_id)
        except Exception as e:
            # Backup is down or unreachable
            pass

    def replicate(
        self,
        event_type: str,
        data: Dict[str, Any],
        mode: str = ConsistencyMode.STRONG,
    ) -> Dict[str, Any]:
        """
        Replicate a state event to all known backup nodes according to consistency policy.
        """
        with self._lock:
            event_id = self._next_event_id
            self._next_event_id += 1

        ts = self.clock.increment()
        event_payload = {
            "event_id": event_id,
            "event_type": event_type,
            "server_lamport_ts": ts,
            "primary_id": self.local_node.node_id,
            "data": data,
        }

        backups = [b for b in self.get_backup_nodes() if b.node_id != self.local_node.node_id]

        if not backups:
            # Single-node cluster; commit immediately
            return {
                "status": "committed",
                "mode": mode,
                "acks": 0,
                "quorum_met": True,
                "latency_ms": 0.0,
                "event_id": event_id,
            }

        ack_tracker = {
            "count": 0,
            "nodes": [],
            "lock": threading.Lock(),
        }

        # Create worker threads for fan-out
        threads = [
            threading.Thread(
                target=self._send_to_backup,
                args=(b, event_payload, ack_tracker),
                daemon=True,
                name=f"Repl-{b.node_id}-{event_id}",
            )
            for b in backups
        ]

        if mode == ConsistencyMode.STRONG:
            start_time = time.time()
            for t in threads:
                t.start()

            # Required ACKs: min(configured quorum, available backups)
            required_acks = min(self.write_quorum_backups, len(backups))

            # Block until quorum achieved or timeout reached
            deadline = start_time + self.timeout_sec
            while time.time() < deadline:
                with ack_tracker["lock"]:
                    if ack_tracker["count"] >= required_acks:
                        break
                time.sleep(0.002)

            elapsed_ms = round((time.time() - start_time) * 1000, 2)
            with ack_tracker["lock"]:
                quorum_met = ack_tracker["count"] >= required_acks
                ack_count = ack_tracker["count"]

            return {
                "status": "committed" if quorum_met else "quorum_timeout",
                "mode": ConsistencyMode.STRONG,
                "acks": ack_count,
                "quorum_required": required_acks,
                "quorum_met": quorum_met,
                "latency_ms": elapsed_ms,
                "event_id": event_id,
            }

        else:
            # Eventual consistency: dispatch asynchronously and return immediately
            for t in threads:
                t.start()

            return {
                "status": "accepted",
                "mode": ConsistencyMode.EVENTUAL,
                "acks": 0,
                "latency_ms": 0.0,
                "event_id": event_id,
            }
