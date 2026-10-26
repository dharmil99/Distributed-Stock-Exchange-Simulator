"""
Failure Detector for Distributed Stock Exchange Simulator.

Monitors the active Leader by issuing periodic heartbeats.
If the leader misses a threshold of consecutive heartbeats, the detector
flags the node as failed and triggers failover / election routines.
"""

import threading
import time
from typing import Callable, Optional

from core.models import NodeDescriptor
from fault_tolerance.heartbeat import check_heartbeat


class FailureDetector:
    """
    Heartbeat-based failure detector watching the cluster Leader.
    """

    def __init__(
        self,
        local_node_id: str,
        get_target_leader_fn: Callable[[], Optional[NodeDescriptor]],
        on_leader_failed_fn: Callable[[str], None],
        heartbeat_interval: float = 0.5,
        max_missed: int = 3,
    ):
        self.local_node_id = local_node_id
        self.get_target_leader = get_target_leader_fn
        self.on_leader_failed = on_leader_failed_fn
        self.heartbeat_interval = heartbeat_interval
        self.max_missed = max_missed

        self._missed_count = 0
        self._is_running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self):
        """Start the background heartbeat monitor thread."""
        with self._lock:
            if self._is_running:
                return
            self._is_running = True
            self._thread = threading.Thread(
                target=self._monitor_loop,
                daemon=True,
                name=f"FailureDetector-{self.local_node_id}",
            )
            self._thread.start()

    def stop(self):
        """Stop the background monitor thread."""
        with self._lock:
            self._is_running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def _monitor_loop(self):
        while True:
            with self._lock:
                if not self._is_running:
                    break
                interval = self.heartbeat_interval

            time.sleep(interval)

            target = self.get_target_leader()
            # If no target or local node is the leader, no need to monitor
            if not target or target.node_id == self.local_node_id:
                with self._lock:
                    self._missed_count = 0
                continue

            resp = check_heartbeat(target, timeout=self.heartbeat_interval)
            if resp:
                with self._lock:
                    self._missed_count = 0
            else:
                with self._lock:
                    self._missed_count += 1
                    missed = self._missed_count

                if missed >= self.max_missed:
                    # Target failed!
                    with self._lock:
                        self._missed_count = 0
                    self.on_leader_failed(target.node_id)
