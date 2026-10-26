"""
Centralized Configuration for Distributed Stock Exchange Simulator.

Defines the standard development topology:
  - Load Balancer: Port 8000
  - Node 1: Port 8001 (Priority 1)
  - Node 2: Port 8002 (Priority 2)
  - Node 3: Port 8003 (Priority 3)
"""

from typing import Dict, List, Optional
from core.models import NodeDescriptor, NodeRole

DEFAULT_HOST = "127.0.0.1"
LOAD_BALANCER_PORT = 8000

NODE_CONFIGS: Dict[str, Dict] = {
    "node-1": {
        "node_id": "node-1",
        "host": DEFAULT_HOST,
        "port": 8001,
        "priority": 1,
        "role": NodeRole.BACKUP,
    },
    "node-2": {
        "node_id": "node-2",
        "host": DEFAULT_HOST,
        "port": 8002,
        "priority": 2,
        "role": NodeRole.BACKUP,
    },
    "node-3": {
        "node_id": "node-3",
        "host": DEFAULT_HOST,
        "port": 8003,
        "priority": 3,
        "role": NodeRole.LEADER,  # Highest priority starts as Leader by default
    },
}


class ClusterConfig:
    """Helper class providing typed descriptors for the entire cluster."""

    @classmethod
    def get_load_balancer(cls, host: str = DEFAULT_HOST, port: int = LOAD_BALANCER_PORT) -> NodeDescriptor:
        return NodeDescriptor(
            node_id="load-balancer",
            host=host,
            port=port,
            priority=0,
            role=NodeRole.BACKUP,
            is_alive=True,
        )

    @classmethod
    def get_all_nodes(cls, host: str = DEFAULT_HOST) -> List[NodeDescriptor]:
        nodes = []
        for conf in NODE_CONFIGS.values():
            nodes.append(NodeDescriptor(
                node_id=conf["node_id"],
                host=host if host else conf["host"],
                port=conf["port"],
                priority=conf["priority"],
                role=conf["role"],
                is_alive=True,
            ))
        return sorted(nodes, key=lambda n: n.priority)

    @classmethod
    def get_node(cls, node_id: str, host: str = DEFAULT_HOST) -> Optional[NodeDescriptor]:
        conf = NODE_CONFIGS.get(node_id)
        if not conf:
            return None
        return NodeDescriptor(
            node_id=conf["node_id"],
            host=host if host else conf["host"],
            port=conf["port"],
            priority=conf["priority"],
            role=conf["role"],
            is_alive=True,
        )

    @classmethod
    def get_default_leader(cls, host: str = DEFAULT_HOST) -> NodeDescriptor:
        """Returns the default initial leader (Node with highest priority)."""
        nodes = cls.get_all_nodes(host=host)
        return max(nodes, key=lambda n: n.priority)
