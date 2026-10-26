"""
Lamport Logical Clock implementation for Distributed Stock Exchange Simulator.

Implements Leslie Lamport's 1978 logical clock algorithm ("Time, Clocks, and the
Ordering of Events in a Distributed System"):
  - LC1: Before executing a local event or sending a message, a process increments
         its own logical clock: C = C + 1
  - LC2: When a process receives a message stamped with logical timestamp C_msg,
         it updates its own clock: C = max(C, C_msg) + 1

This guarantees that if event A causally precedes event B (A -> B), then
L(A) < L(B), establishing a consistent partial order across distributed nodes.
"""

import threading


class LamportClock:
    """
    Thread-safe Lamport Logical Clock.
    Protects clock increment and update operations with an internal mutual exclusion lock.
    """

    def __init__(self, initial_value: int = 0):
        self._value = int(initial_value)
        self._lock = threading.Lock()

    def increment(self) -> int:
        """
        LC1: Increment logical clock prior to sending a message or registering a local event.
        Returns the updated logical timestamp.
        """
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_timestamp: int) -> int:
        """
        LC2: Advance logical clock upon receiving a remote message carrying received_timestamp.
        Sets C = max(C, received_timestamp) + 1.
        Returns the updated logical timestamp.
        """
        with self._lock:
            self._value = max(self._value, int(received_timestamp)) + 1
            return self._value

    def read(self) -> int:
        """Return the current logical clock value without modifying it."""
        with self._lock:
            return self._value

    # Backward-compatibility aliases for legacy Lab 3/5/8 callers
    def tick(self) -> int:
        """Alias for increment() (used in legacy Lab 3/5/8)."""
        return self.increment()

    @property
    def value(self) -> int:
        """Alias for read() as a property (used in legacy Lab 3/5)."""
        return self.read()

    def __repr__(self) -> str:
        with self._lock:
            return f"LamportClock(value={self._value})"

    def __str__(self) -> str:
        with self._lock:
            return str(self._value)
