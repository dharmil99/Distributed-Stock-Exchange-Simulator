"""
Heartbeat monitoring utilities for Distributed Stock Exchange Simulator.

Enables nodes and load balancers to poll liveness and role status across the cluster.
"""

import xmlrpc.client
from typing import Any, Dict, Optional

from core.models import NodeDescriptor


def check_heartbeat(node: NodeDescriptor, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
    """
    Ping a node via XML-RPC heartbeat call.
    Returns response dict if reachable, or None if down/unreachable.
    """
    try:
        proxy = xmlrpc.client.ServerProxy(node.url, allow_none=True)
        resp = proxy.heartbeat()
        if isinstance(resp, dict) and resp.get("alive"):
            return resp
    except Exception:
        pass
    return None
