"""
Core package for the Distributed Stock Exchange Simulator.
Provides canonical models, clock synchronization, matching engine,
persistence layer, and cluster configuration.
"""

from core.models import Order, Trade, NodeDescriptor, OrderSide, OrderStatus, NodeRole
from core.clock import LamportClock
from core.matching_engine import MatchingEngine, OrderBook
from core.persistence import SQLitePersistence
from core.config import ClusterConfig, DEFAULT_HOST, LOAD_BALANCER_PORT, NODE_CONFIGS

__all__ = [
    "Order",
    "Trade",
    "NodeDescriptor",
    "OrderSide",
    "OrderStatus",
    "NodeRole",
    "LamportClock",
    "MatchingEngine",
    "OrderBook",
    "SQLitePersistence",
    "ClusterConfig",
    "DEFAULT_HOST",
    "LOAD_BALANCER_PORT",
    "NODE_CONFIGS",
]
