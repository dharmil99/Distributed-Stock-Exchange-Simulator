"""
RPC Protocol constants and message definitions for Distributed Stock Exchange Simulator.

Centralizes:
  - Remote procedure call method names
  - Response status codes
  - Standard response formatting helpers
"""

from typing import Any, Dict, Optional

# Standard RPC Method Names
SUBMIT_ORDER = "submit_order"
GET_ORDER_BOOK = "get_order_book"
GET_DEPTH = "get_depth"
GET_ORDER = "get_order"
GET_ALL_ORDERS = "get_all_orders"
GET_ALL_TRADES = "get_all_trades"
CANCEL_ORDER = "cancel_order"
GET_NODE_INFO = "get_node_info"
GET_LAMPORT_TIME = "get_lamport_time"
GET_CURRENT_LEADER = "get_current_leader"
START_ELECTION = "start_election"
ELECTION_MESSAGE = "election_message"
HEARTBEAT = "heartbeat"
REPLICATE_EVENT = "replicate_event"
GET_CLUSTER_STATUS = "get_cluster_status"

# All publicly registered RPC methods for TradingNode and cluster components
REGISTERED_RPC_METHODS = [
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
    START_ELECTION,
    ELECTION_MESSAGE,
    HEARTBEAT,
    REPLICATE_EVENT,
    GET_CLUSTER_STATUS,
]

# Response Statuses
STATUS_SUCCESS = "success"
STATUS_ERROR = "error"
STATUS_NOT_FOUND = "not_found"


def make_response(
    status: str = STATUS_SUCCESS,
    data: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    node_id: Optional[str] = None,
    server_lamport_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """Helper to construct standardized RPC response dictionaries."""
    resp: Dict[str, Any] = {"status": status}
    if data:
        resp.update(data)
    if error is not None:
        resp["error"] = str(error)
    if node_id is not None:
        resp["node_id"] = str(node_id)
    if server_lamport_ts is not None:
        resp["server_lamport_ts"] = int(server_lamport_ts)
    return resp
