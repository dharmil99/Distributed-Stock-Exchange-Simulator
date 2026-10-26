"""
CLI Entry Point to launch a Trading Node instance.

Usage:
  python -m nodes.node_server --node-id node-1
  python -m nodes.node_server --node-id node-2
  python -m nodes.node_server --node-id node-3

Options:
  --election-algorithm {bully,ring}
  --consistency {strong,eventual}
  --enable-failure-detector
"""

import argparse
import os
import signal
import sys
import time

from core.config import ClusterConfig, DEFAULT_HOST
from core.models import NodeDescriptor
from nodes.trading_node import TradingNode
from communication.rpc_server import NodeRPCServer


def parse_args():
    parser = argparse.ArgumentParser(description="Start a Trading Node XML-RPC Server")
    parser.add_argument(
        "--node-id",
        type=str,
        default="node-1",
        help="Node ID from cluster config (e.g. node-1, node-2, node-3)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help=f"Host interface to bind (default from config or {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to listen on (default from cluster config)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Custom SQLite database file path (default data/<node-id>.db)",
    )
    parser.add_argument(
        "--election-algorithm",
        type=str,
        default="bully",
        choices=["bully", "ring"],
        help="Leader election algorithm (bully or ring)",
    )
    parser.add_argument(
        "--consistency",
        type=str,
        default="strong",
        choices=["strong", "eventual"],
        help="Replication consistency model (strong or eventual)",
    )
    parser.add_argument(
        "--enable-failure-detector",
        action="store_true",
        default=False,
        help="Enable background heartbeat failure detection against leader",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Look up node configuration
    configured_node = ClusterConfig.get_node(args.node_id)
    if not configured_node:
        print(f"[ERROR] Unknown node-id '{args.node_id}'. Available: node-1, node-2, node-3")
        sys.exit(1)

    host = args.host or configured_node.host
    port = args.port or configured_node.port

    descriptor = NodeDescriptor(
        node_id=args.node_id,
        host=host,
        port=port,
        priority=configured_node.priority,
        role=configured_node.role,
        is_alive=True,
    )

    all_cluster_nodes = ClusterConfig.get_all_nodes(host=host)

    print("=" * 65)
    print(f"  DISTRIBUTED STOCK EXCHANGE — TRADING NODE")
    print(f"  Node ID:     {descriptor.node_id}")
    print(f"  Role:        {descriptor.role}")
    print(f"  Priority:    {descriptor.priority}")
    print(f"  Endpoint:    http://{host}:{port}/RPC2")
    print(f"  Election:    {args.election_algorithm.upper()}")
    print(f"  Consistency: {args.consistency.upper()}")
    print("=" * 65)

    # Initialize Trading Node with cluster coordination
    node = TradingNode(
        descriptor=descriptor,
        db_path=args.db_path,
        peer_nodes=all_cluster_nodes,
        election_algorithm=args.election_algorithm,
        consistency_mode=args.consistency,
        enable_failure_detector=args.enable_failure_detector,
    )
    print(f"[DATABASE] Connected to SQLite: {node.db_path}")

    # Initialize RPC Server
    server = NodeRPCServer(host=host, port=port)
    server.register_functions(node.get_rpc_methods())

    def handle_exit(signum, frame):
        print("\n[SERVER] Shutting down node gracefully...")
        server.shutdown()
        node.close()
        print("[SERVER] Node stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    print(f"[SERVER] Listening on {host}:{port}...")
    server.start(block=True)


if __name__ == "__main__":
    main()
