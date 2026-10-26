import threading


class LamportClock:
    """Simple thread-safe Lamport logical clock (carried over from Lab 3)."""

    def __init__(self, start: int = 0):
        self._value = start
        self._lock = threading.Lock()

    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_ts: int) -> int:
        with self._lock:
            self._value = max(self._value, received_ts) + 1
            return self._value

    @property
    def value(self) -> int:
        with self._lock:
            return self._value
