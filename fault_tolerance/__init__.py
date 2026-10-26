"""
Fault Tolerance package for Distributed Stock Exchange Simulator.
Provides heartbeat monitoring, failure detection, and automatic primary recovery.
"""

from fault_tolerance.heartbeat import check_heartbeat
from fault_tolerance.failure_detector import FailureDetector
from fault_tolerance.recovery import RecoveryManager

__all__ = ["check_heartbeat", "FailureDetector", "RecoveryManager"]
