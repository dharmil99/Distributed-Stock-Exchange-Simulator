"""
Thread pool utility for concurrent background operations in Distributed Stock Exchange Simulator.
"""

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Iterable, List, Optional


class WorkerThreadPool:
    """
    Lightweight managed thread pool wrapper for executing concurrent asynchronous tasks.
    """

    def __init__(self, max_workers: int = 16, thread_name_prefix: str = "ExchangeWorker"):
        self.max_workers = max_workers
        self.thread_name_prefix = thread_name_prefix
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix=self.thread_name_prefix,
        )

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a task to the pool."""
        return self._executor.submit(fn, *args, **kwargs)

    def map(self, fn: Callable, *iterables: Iterable, timeout: Optional[float] = None):
        """Map a function over iterables concurrently."""
        return self._executor.map(fn, *iterables, timeout=timeout)

    def shutdown(self, wait: bool = True):
        """Shutdown thread pool."""
        self._executor.shutdown(wait=wait)
