"""
Load Balancer for Distributed Stock Exchange Simulator.

Acts as a client-facing reverse proxy:
  - Distributes read-only queries (books, depth, trades) across healthy nodes via Round-Robin.
  - Routes authoritative state-changing requests (order submissions, cancellations) directly
    to the active cluster Leader.
  - Automatically adapts to leader failovers and cluster topology changes.
"""

import argparse
import signal
import sys
import xmlrpc.client
from typing import Any, Dict, List, Optional

from core.config import ClusterConfig, DEFAULT_HOST, LOAD_BALANCER_PORT
from core.models import NodeDescriptor, NodeRole
from communication.protocol import (
    SUBMIT_ORDER,
    GET_ORDER_BOOK,
    GET_DEPTH,
    GET_ORDER,
    GET_ALL_ORDERS,
    GET_ALL_TRADES,
    CANCEL_ORDER,
    GET_NODE_INFO,
    GET_LAMPORT_TIME,
    GET_CURRENT_LEADER,
    GET_CLUSTER_STATUS,
    STATUS_SUCCESS,
    STATUS_ERROR,
    STATUS_NOT_FOUND,
    make_response,
)
from communication.rpc_server import NodeRPCServer
from load_balancing.node_health import NodeHealthMonitor
from load_balancing.round_robin import RoundRobinSelector


class LoadBalancer:
    """
    Reverse proxy distributing traffic to healthy trading nodes.
    """

    def __init__(
        self,
        nodes: Optional[List[NodeDescriptor]] = None,
        host: str = DEFAULT_HOST,
        port: int = LOAD_BALANCER_PORT,
    ):
        self.host = host
        self.port = port
        self.nodes = nodes or ClusterConfig.get_all_nodes(host=self.host)

        self.health_monitor = NodeHealthMonitor(self.nodes)
        self.round_robin = RoundRobinSelector()
        self.server = NodeRPCServer(host=self.host, port=self.port)
        self.server.register_functions(self.get_rpc_methods())

    def start(self, block: bool = True):
        """Start health monitoring and RPC server."""
        self.health_monitor.start()
        self.server.start(block=block)

    def shutdown(self):
        """Clean shutdown."""
        self.health_monitor.stop()
        self.server.shutdown()

    # -------------------------------------------------------------------------
    # Routing Logic
    # -------------------------------------------------------------------------

    def _route_state_changing_to_leader(self, method_name: str, *args) -> Dict[str, Any]:
        """Route state-changing mutation (submit, cancel) directly to the authoritative Leader."""
        leader = self.health_monitor.get_current_leader()
        if not leader:
            # Refresh once in case an election just completed
            self.health_monitor.check_all_nodes()
            leader = self.health_monitor.get_current_leader()

        if not leader:
            return make_response(
                status=STATUS_ERROR,
                error="No active Leader currently available. An election may be in progress.",
            )

        try:
            proxy = xmlrpc.client.ServerProxy(leader.url, allow_none=True)
            res = getattr(proxy, method_name)(*args)
            if isinstance(res, dict):
                res["routed_by"] = "load-balancer"
                res["servicing_node"] = leader.node_id
            return res
        except Exception as e:
            return make_response(
                status=STATUS_ERROR,
                error=f"Failed forwarding to leader {leader.node_id} ({e})",
            )

    def _route_read_round_robin(self, method_name: str, *args) -> Dict[str, Any]:
        """Distribute read-only queries across all healthy nodes using round-robin."""
        healthy = self.health_monitor.get_healthy_nodes()
        if not healthy:
            return make_response(
                status=STATUS_ERROR,
                error="No healthy trading nodes currently available.",
            )

        # Attempt up to len(healthy) nodes in case of transient error
        for _ in range(len(healthy)):
            target = self.round_robin.select(healthy)
            if not target:
                break
            try:
                proxy = xmlrpc.client.ServerProxy(target.url, allow_none=True)
                res = getattr(proxy, method_name)(*args)
                if isinstance(res, dict):
                    res["routed_by"] = "load-balancer"
                    res["servicing_node"] = target.node_id
                return res
            except Exception:
                # Mark node as temporarily dead and try next
                self.health_monitor.check_all_nodes()
                healthy = self.health_monitor.get_healthy_nodes()

        return make_response(
            status=STATUS_ERROR,
            error="All attempts to query healthy nodes failed.",
        )

    # -------------------------------------------------------------------------
    # Public RPC Endpoints
    # -------------------------------------------------------------------------

    def submit_order(self, order_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Route order submission to current Leader."""
        return self._route_state_changing_to_leader(SUBMIT_ORDER, order_dict)

    def cancel_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        """Route order cancellation to current Leader."""
        return self._route_state_changing_to_leader(CANCEL_ORDER, symbol, int(order_id))

    def get_order_book(self, symbol: str) -> Dict[str, Any]:
        """Round-robin read for order book."""
        return self._route_read_round_robin(GET_ORDER_BOOK, symbol)

    def get_depth(self, symbol: str) -> Dict[str, Any]:
        """Round-robin read for market depth."""
        return self._route_read_round_robin(GET_DEPTH, symbol)

    def get_order(self, order_id: int) -> Dict[str, Any]:
        """Round-robin read for specific order."""
        return self._route_read_round_robin(GET_ORDER, int(order_id))

    def get_all_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Round-robin read for all orders."""
        return self._route_read_round_robin(GET_ALL_ORDERS, symbol)

    def get_all_trades(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Round-robin read for all trades."""
        return self._route_read_round_robin(GET_ALL_TRADES, symbol)

    def get_node_info(self) -> Dict[str, Any]:
        """Return load balancer status."""
        return make_response(
            status=STATUS_SUCCESS,
            data={
                "node": {
                    "node_id": "load-balancer",
                    "host": self.host,
                    "port": self.port,
                    "role": "LOAD_BALANCER",
                },
                "active_nodes_count": len(self.health_monitor.get_healthy_nodes()),
                "current_leader": getattr(self.health_monitor.get_current_leader(), "node_id", None),
            },
        )

    def get_current_leader(self) -> Dict[str, Any]:
        """Return current cluster leader."""
        leader = self.health_monitor.get_current_leader()
        if leader:
            return make_response(
                status=STATUS_SUCCESS,
                data={"leader": leader.to_dict()},
            )
        return make_response(
            status=STATUS_NOT_FOUND,
            error="No active Leader found.",
        )

    def get_cluster_status(self) -> Dict[str, Any]:
        """Return snapshot of the entire cluster's health and roles."""
        return make_response(
            status=STATUS_SUCCESS,
            data={"cluster": self.health_monitor.get_cluster_status()},
        )

    def get_lamport_time(self) -> Dict[str, Any]:
        """Return highest reported Lamport time from active nodes."""
        leader = self.health_monitor.get_current_leader()
        if leader:
            try:
                proxy = xmlrpc.client.ServerProxy(leader.url, allow_none=True)
                return proxy.get_lamport_time()
            except Exception:
                pass
        return make_response(status=STATUS_SUCCESS, data={"lamport_ts": 0})

    def get_rpc_methods(self) -> Dict[str, Any]:
        return {
            SUBMIT_ORDER: self.submit_order,
            CANCEL_ORDER: self.cancel_order,
            GET_ORDER_BOOK: self.get_order_book,
            GET_DEPTH: self.get_depth,
            GET_ORDER: self.get_order,
            GET_ALL_ORDERS: self.get_all_orders,
            GET_ALL_TRADES: self.get_all_trades,
            GET_NODE_INFO: self.get_node_info,
            GET_CURRENT_LEADER: self.get_current_leader,
            GET_CLUSTER_STATUS: self.get_cluster_status,
            GET_LAMPORT_TIME: self.get_lamport_time,
        }


def parse_args():
    parser = argparse.ArgumentParser(description="Start Load Balancer Server")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host interface")
    parser.add_argument("--port", type=int, default=LOAD_BALANCER_PORT, help="Port to listen on")
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 65)
    print("  DISTRIBUTED STOCK EXCHANGE — LOAD BALANCER")
    print(f"  Endpoint: http://{args.host}:{args.port}/RPC2")
    print(f"  Target Cluster: Ports 8001, 8002, 8003")
    print("=" * 65)

    lb = LoadBalancer(host=args.host, port=args.port)

    def handle_exit(signum, frame):
        print("\n[LOAD BALANCER] Stopping...")
        lb.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    print(f"[LOAD BALANCER] Ready on {args.host}:{args.port}...")
    lb.start(block=True)


if __name__ == "__main__":
    main()
