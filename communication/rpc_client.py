"""
XML-RPC Client wrapper for Distributed Stock Exchange Simulator.

Connects to a TradingNode (or Load Balancer) over XML-RPC, manages client-side
Lamport logical clocks, serializes canonical Order dictionaries, and processes responses.
"""

import xmlrpc.client
from typing import Any, Dict, List, Optional

from core.clock import LamportClock
from core.models import Order, OrderSide
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
)


class TradingClient:
    """
    Client for interacting with a TradingNode or cluster via XML-RPC.
    Manages local Lamport clock synchronization across remote requests.
    """

    def __init__(self, node_url: str = "http://127.0.0.1:8001/RPC2"):
        self.node_url = node_url
        self.clock = LamportClock()
        self._proxy = xmlrpc.client.ServerProxy(self.node_url, allow_none=True)

    def _sync_clock_from_response(self, response: Dict[str, Any]):
        """LC2: Advance client clock using server_lamport_ts if present."""
        if isinstance(response, dict) and "server_lamport_ts" in response:
            server_ts = response["server_lamport_ts"]
            if server_ts is not None:
                self.clock.update(int(server_ts))

    def submit_order(
        self,
        trader_id: str,
        symbol: str,
        side: str,
        price: float,
        quantity: int,
    ) -> Dict[str, Any]:
        """
        Build and submit a new limit order to the trading node.
        Applies LC1 before sending and LC2 upon receiving the server's reply.
        """
        # LC1: Tick client logical clock before sending request
        client_ts = self.clock.increment()

        order = Order(
            trader_id=trader_id,
            symbol=symbol,
            side=OrderSide.normalize(side),
            price=float(price),
            quantity=int(quantity),
            client_lamport_ts=client_ts,
        )

        resp = getattr(self._proxy, SUBMIT_ORDER)(order.to_dict())
        self._sync_clock_from_response(resp)
        return resp

    def submit_order_dict(self, order_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Submit an existing order dictionary."""
        # Ensure client_lamport_ts is stamped if missing
        if "client_lamport_ts" not in order_dict or order_dict["client_lamport_ts"] is None:
            order_dict["client_lamport_ts"] = self.clock.increment()

        resp = getattr(self._proxy, SUBMIT_ORDER)(order_dict)
        self._sync_clock_from_response(resp)
        return resp

    def get_order_book(self, symbol: str) -> Dict[str, Any]:
        """Query snapshot of the order book for the given symbol."""
        resp = getattr(self._proxy, GET_ORDER_BOOK)(symbol)
        self._sync_clock_from_response(resp)
        return resp

    def get_depth(self, symbol: str) -> Dict[str, Any]:
        """Query aggregated market depth by price level for the given symbol."""
        resp = getattr(self._proxy, GET_DEPTH)(symbol)
        self._sync_clock_from_response(resp)
        return resp

    def get_order(self, order_id: int) -> Dict[str, Any]:
        """Query single order by ID."""
        resp = getattr(self._proxy, GET_ORDER)(int(order_id))
        self._sync_clock_from_response(resp)
        return resp

    def get_all_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Query all registered orders."""
        resp = getattr(self._proxy, GET_ALL_ORDERS)(symbol)
        self._sync_clock_from_response(resp)
        return resp

    def get_all_trades(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Query all executed trades."""
        resp = getattr(self._proxy, GET_ALL_TRADES)(symbol)
        self._sync_clock_from_response(resp)
        return resp

    def cancel_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        """Cancel an open order."""
        resp = getattr(self._proxy, CANCEL_ORDER)(symbol, int(order_id))
        self._sync_clock_from_response(resp)
        return resp

    def get_node_info(self) -> Dict[str, Any]:
        """Query node status and descriptor."""
        resp = getattr(self._proxy, GET_NODE_INFO)()
        self._sync_clock_from_response(resp)
        return resp

    def get_lamport_time(self) -> Dict[str, Any]:
        """Query server's current logical clock timestamp."""
        resp = getattr(self._proxy, GET_LAMPORT_TIME)()
        self._sync_clock_from_response(resp)
        return resp

    def get_current_leader(self) -> Dict[str, Any]:
        """Query the cluster leader from the connected node or load balancer."""
        resp = getattr(self._proxy, "get_current_leader")()
        self._sync_clock_from_response(resp)
        return resp

    def get_cluster_status(self) -> Dict[str, Any]:
        """Query full cluster health and node status."""
        resp = getattr(self._proxy, "get_cluster_status")()
        self._sync_clock_from_response(resp)
        return resp

    def start_election(self) -> Dict[str, Any]:
        """Trigger an election on the cluster."""
        resp = getattr(self._proxy, "start_election")()
        self._sync_clock_from_response(resp)
        return resp
