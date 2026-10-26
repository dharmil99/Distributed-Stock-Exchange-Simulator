"""
Node Health Monitor for Distributed Stock Exchange Simulator.

Periodically pings trading nodes via XML-RPC to maintain a live registry
of healthy nodes and identify the active cluster Leader.
"""

import threading
import time
import xmlrpc.client
from typing import Dict, List, Optional

from core.models import NodeDescriptor, NodeRole


class NodeHealthMonitor:
    """
    Background health checker maintaining real-time node availability tables.
    """

    def __init__(
        self,
        nodes: List[NodeDescriptor],
        poll_interval: float = 0.5,
        timeout: float = 0.8,
    ):
        self.poll_interval = poll_interval
        self.timeout = timeout
        self._lock = threading.RLock()
        self._nodes: Dict[str, NodeDescriptor] = {n.node_id: n for n in nodes}
        self._health_status: Dict[str, Dict] = {
            n.node_id: {
                "is_alive": True,
                "role": n.role,
                "last_seen": time.time(),
                "node": n,
            }
            for n in nodes
        }
        self._is_running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Start periodic health polling."""
        with self._lock:
            if self._is_running:
                return
            self._is_running = True
            # Perform immediate check on start
            self.check_all_nodes()
            self._thread = threading.Thread(
                target=self._poll_loop,
                daemon=True,
                name="LoadBalancer-HealthMonitor",
            )
            self._thread.start()

    def stop(self):
        """Stop background health polling."""
        with self._lock:
            self._is_running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def check_all_nodes(self):
        """Perform a single health check sweep over all registered nodes."""
        with self._lock:
            nodes_to_check = list(self._nodes.values())

        for node in nodes_to_check:
            alive = False
            reported_role = node.role
            try:
                proxy = xmlrpc.client.ServerProxy(node.url, allow_none=True)
                info = proxy.get_node_info()
                if info and info.get("status") == "success":
                    alive = True
                    node_data = info.get("node", {})
                    reported_role = node_data.get("role", node.role)
            except Exception:
                alive = False

            with self._lock:
                node.is_alive = alive
                node.role = reported_role
                status = self._health_status.get(node.node_id, {})
                status["is_alive"] = alive
                status["role"] = reported_role
                if alive:
                    status["last_seen"] = time.time()
                self._health_status[node.node_id] = status

    def _poll_loop(self):
        while True:
            with self._lock:
                if not self._is_running:
                    break
                interval = self.poll_interval

            time.sleep(interval)
            self.check_all_nodes()

    def get_healthy_nodes(self) -> List[NodeDescriptor]:
        """Return list of currently healthy, reachable nodes."""
        with self._lock:
            return [
                info["node"]
                for info in self._health_status.values()
                if info["is_alive"]
            ]

    def get_current_leader(self) -> Optional[NodeDescriptor]:
        """Return the healthy node currently reporting role == LEADER."""
        with self._lock:
            # First search among healthy nodes reporting LEADER
            for info in self._health_status.values():
                if info["is_alive"] and info["role"] == NodeRole.LEADER:
                    return info["node"]

            # Fallback: return healthy node with highest priority
            healthy = self.get_healthy_nodes()
            if healthy:
                return max(healthy, key=lambda n: n.priority)
            return None

    def get_cluster_status(self) -> Dict[str, Dict]:
        """Return full snapshot of cluster health."""
        with self._lock:
            return {
                node_id: {
                    "node_id": node_id,
                    "is_alive": info["is_alive"],
                    "role": info["role"],
                    "port": info["node"].port,
                    "priority": info["node"].priority,
                    "last_seen_sec_ago": round(time.time() - info["last_seen"], 1),
                }
                for node_id, info in self._health_status.items()
            }
