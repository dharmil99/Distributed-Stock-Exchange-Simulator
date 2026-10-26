"""
Multithreaded XML-RPC Server implementation for Distributed Stock Exchange Simulator.

Combines socketserver.ThreadingMixIn and xmlrpc.server.SimpleXMLRPCServer
to handle concurrent client RPC calls across worker threads without blocking.
"""

import threading
from socketserver import ThreadingMixIn
from typing import Any, Callable, Dict, List, Optional
from xmlrpc.server import SimpleXMLRPCRequestHandler, SimpleXMLRPCServer


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    """
    Multithreaded XML-RPC server.
    Spawns a new worker thread for each incoming connection request (Lab 2).
    """
    daemon_threads = True
    allow_reuse_address = True


class RequestHandler(SimpleXMLRPCRequestHandler):
    """
    Custom request handler that serves both /RPC2 and / endpoints.
    """
    rpc_paths = ("/RPC2", "/")


class NodeRPCServer:
    """
    Wrapper managing the lifecycle of an XML-RPC server instance for a TradingNode.
    Supports graceful starting, stopping, and dynamic port queries.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8001,
        log_requests: bool = False,
    ):
        self.host = host
        self.port = port
        self.log_requests = log_requests
        self._server = ThreadedXMLRPCServer(
            (self.host, self.port),
            requestHandler=RequestHandler,
            allow_none=True,
            logRequests=self.log_requests,
        )
        # In case port 0 was passed, capture actual bound port
        self.server_port: int = self._server.server_address[1]
        self._is_running = False
        self._server_thread: Optional[threading.Thread] = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.server_port}/RPC2"

    def register_function(self, func: Callable, name: Optional[str] = None):
        """Register a specific callable remote method."""
        self._server.register_function(func, name)

    def register_functions(self, func_map: Dict[str, Callable]):
        """Register a dictionary of name -> function mappings."""
        for name, func in func_map.items():
            self._server.register_function(func, name)

    def start(self, block: bool = False):
        """
        Start serving requests.
        If block is False, starts in a background daemon thread.
        """
        self._is_running = True
        if block:
            try:
                self._server.serve_forever()
            except KeyboardInterrupt:
                self.shutdown()
        else:
            self._server_thread = threading.Thread(
                target=self._server.serve_forever,
                daemon=True,
                name=f"RPCServer-{self.host}:{self.server_port}",
            )
            self._server_thread.start()

    def shutdown(self):
        """Cleanly shutdown the RPC server."""
        if self._is_running:
            self._is_running = False
            self._server.shutdown()
            self._server.server_close()
            if self._server_thread and self._server_thread.is_alive():
                self._server_thread.join(timeout=2.0)
