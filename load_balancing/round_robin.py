"""
Round-Robin Load Balancing Strategy for Distributed Stock Exchange Simulator.
"""

import threading
from typing import List, Optional

from core.models import NodeDescriptor


class RoundRobinSelector:
    """
    Selects healthy nodes in circular round-robin sequence.
    Thread-safe counter ensures fair distribution under concurrent traffic.
    """

    def __init__(self):
        self._index = 0
        self._lock = threading.Lock()

    def select(self, healthy_nodes: List[NodeDescriptor]) -> Optional[NodeDescriptor]:
        """Select next node from healthy pool."""
        if not healthy_nodes:
            return None
        with self._lock:
            selected = healthy_nodes[self._index % len(healthy_nodes)]
            self._index = (self._index + 1) % len(healthy_nodes)
            return selected

    def reset(self):
        """Reset sequence index."""
        with self._lock:
            self._index = 0
