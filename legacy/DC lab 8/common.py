import threading


class LamportClock:
    """Simple Lamport logical clock (carried over from Lab 3)."""

    def __init__(self):
        self._time = 0
        self._lock = threading.Lock()

    def tick(self):
        """Call this when a local event happens (e.g. an order is accepted)."""
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received_time):
        """Call this when a timestamp arrives from another node."""
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time

    def value(self):
        with self._lock:
            return self._time
