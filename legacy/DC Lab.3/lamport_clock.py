"""
lamport_clock.py

A minimal, thread-safe implementation of Lamport's logical clock
(L. Lamport, "Time, Clocks, and the Ordering of Events in a Distributed
System", CACM 1978).

Rules implemented:
  LC1: Before a process executes an internal event or sends a message,
       it increments its own counter:      C = C + 1
  LC2: When a process receives a message with timestamp Cmsg, it sets:
       C = max(C, Cmsg) + 1

This gives a "happens-before" (->) consistent partial order: if event A
happens-before event B, then L(A) < L(B). It does NOT give a total order
by itself (two independent events can get the same value on different
nodes), so a tie-breaker (e.g. process/trader id) is used wherever a
strict total order is required -- see order_book sorting in
matching_engine_server.py.
"""

import threading


class LamportClock:
    def __init__(self, start: int = 0):
        self._value = start
        self._lock = threading.Lock()

    def tick(self) -> int:
        """LC1: call before sending a message or doing a local event."""
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_ts: int) -> int:
        """LC2: call upon receiving a message stamped received_ts."""
        with self._lock:
            self._value = max(self._value, received_ts) + 1
            return self._value

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def __repr__(self):
        return f"LamportClock({self._value})"
