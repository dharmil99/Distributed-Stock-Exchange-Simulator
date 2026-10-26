"""
Canonical Data Models for Distributed Stock Exchange Simulator.

Defines the core data contracts:
- Order: Represents a buy or sell limit order.
- Trade: Represents an immutable executed transaction between a buyer and seller.
- NodeDescriptor: Represents a node instance within the distributed exchange cluster.

All models support XML-RPC serialization/deserialization with backward-compatible
handling of legacy attributes from Labs 1, 2, 3, 5, and 8.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any


class OrderSide:
    BUY = "BUY"
    SELL = "SELL"

    @classmethod
    def normalize(cls, side: str) -> str:
        s = str(side).strip().upper()
        if s not in (cls.BUY, cls.SELL):
            raise ValueError(f"Invalid order side: '{side}'. Must be 'BUY' or 'SELL'.")
        return s


class OrderStatus:
    PENDING = "PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"

    @classmethod
    def all(cls):
        return {cls.PENDING, cls.PARTIALLY_FILLED, cls.FILLED, cls.CANCELLED}


class NodeRole:
    LEADER = "LEADER"
    BACKUP = "BACKUP"
    RECOVERY = "RECOVERY"

    @classmethod
    def all(cls):
        return {cls.LEADER, cls.BACKUP, cls.RECOVERY}


@dataclass
class Order:
    """
    Canonical Order model.
    Represents a buy or sell limit order submitted by a trader.
    """
    order_id: Optional[int] = None
    trader_id: str = ""
    symbol: str = ""
    side: str = OrderSide.BUY
    price: float = 0.0
    quantity: int = 0
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    client_lamport_ts: Optional[int] = None
    server_lamport_ts: Optional[int] = None
    status: str = OrderStatus.PENDING
    filled_quantity: int = 0

    def __post_init__(self):
        self.side = OrderSide.normalize(self.side)
        self.symbol = str(self.symbol).strip().upper()
        self.price = float(self.price)
        self.quantity = int(self.quantity)
        self.filled_quantity = int(self.filled_quantity)
        if self.price < 0:
            raise ValueError(f"Price cannot be negative: {self.price}")
        if self.quantity <= 0:
            raise ValueError(f"Quantity must be positive: {self.quantity}")
        if self.status not in OrderStatus.all():
            raise ValueError(f"Invalid order status: '{self.status}'")

    @property
    def remaining_quantity(self) -> int:
        """Remaining unfilled shares for this order."""
        return max(0, self.quantity - self.filled_quantity)

    @property
    def is_filled(self) -> bool:
        return self.filled_quantity >= self.quantity

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for XML-RPC transmission and storage."""
        return {
            "order_id": self.order_id,
            "trader_id": self.trader_id,
            "symbol": self.symbol,
            "side": self.side,
            "price": float(self.price),
            "quantity": int(self.quantity),
            "timestamp": self.timestamp,
            "client_lamport_ts": self.client_lamport_ts,
            "server_lamport_ts": self.server_lamport_ts,
            "status": self.status,
            "filled_quantity": int(self.filled_quantity),
            "remaining_quantity": int(self.remaining_quantity),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Order":
        """
        Create an Order from a dictionary.
        Accepts canonical field names and maps legacy aliases:
        - 'stock' -> 'symbol' (Lab 3)
        - 'type' -> 'side' (Lab 1, Lab 2)
        - 'qty' -> 'quantity' (Lab 5, Lab 8)
        - 'id' -> 'order_id' (Lab 8)
        - 'trader' -> 'trader_id' (Lab 5, Lab 8)
        - 'lamport_ts' -> 'client_lamport_ts' (Lab 3)
        """
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data).__name__}")

        order_id = data.get("order_id")
        if order_id is None:
            order_id = data.get("id")

        trader_id = data.get("trader_id")
        if trader_id is None:
            trader_id = data.get("trader", "")

        symbol = data.get("symbol")
        if symbol is None:
            symbol = data.get("stock", "")

        side = data.get("side")
        if side is None:
            side = data.get("type", OrderSide.BUY)

        price = data.get("price", 0.0)

        quantity = data.get("quantity")
        if quantity is None:
            quantity = data.get("qty", 0)

        timestamp = data.get("timestamp")
        if not timestamp:
            timestamp = datetime.now(timezone.utc).isoformat()

        client_lamport_ts = data.get("client_lamport_ts")
        if client_lamport_ts is None:
            client_lamport_ts = data.get("lamport_ts")

        server_lamport_ts = data.get("server_lamport_ts")
        status = data.get("status", OrderStatus.PENDING)
        filled_quantity = data.get("filled_quantity", 0)

        return cls(
            order_id=int(order_id) if order_id is not None else None,
            trader_id=str(trader_id),
            symbol=str(symbol),
            side=str(side),
            price=float(price),
            quantity=int(quantity),
            timestamp=str(timestamp),
            client_lamport_ts=int(client_lamport_ts) if client_lamport_ts is not None else None,
            server_lamport_ts=int(server_lamport_ts) if server_lamport_ts is not None else None,
            status=str(status),
            filled_quantity=int(filled_quantity),
        )


@dataclass(frozen=True)
class Trade:
    """
    Canonical Trade model.
    Represents an immutable match execution between a buyer and seller.
    """
    trade_id: int
    buy_order_id: int
    sell_order_id: int
    buyer_id: str
    seller_id: str
    symbol: str
    price: float
    quantity: int
    timestamp: str
    lamport_ts: int

    def __post_init__(self):
        object.__setattr__(self, "symbol", str(self.symbol).strip().upper())
        object.__setattr__(self, "price", float(self.price))
        object.__setattr__(self, "quantity", int(self.quantity))
        if self.price < 0:
            raise ValueError(f"Trade price cannot be negative: {self.price}")
        if self.quantity <= 0:
            raise ValueError(f"Trade quantity must be positive: {self.quantity}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for XML-RPC transmission and storage."""
        return {
            "trade_id": self.trade_id,
            "buy_order_id": self.buy_order_id,
            "sell_order_id": self.sell_order_id,
            "buyer_id": self.buyer_id,
            "seller_id": self.seller_id,
            "symbol": self.symbol,
            "price": float(self.price),
            "quantity": int(self.quantity),
            "timestamp": self.timestamp,
            "lamport_ts": int(self.lamport_ts),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Trade":
        """Reconstruct Trade from dict."""
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data).__name__}")
        return cls(
            trade_id=int(data["trade_id"]),
            buy_order_id=int(data["buy_order_id"]),
            sell_order_id=int(data["sell_order_id"]),
            buyer_id=str(data["buyer_id"]),
            seller_id=str(data["seller_id"]),
            symbol=str(data["symbol"]),
            price=float(data["price"]),
            quantity=int(data["quantity"]),
            timestamp=str(data["timestamp"]),
            lamport_ts=int(data.get("lamport_ts", 0)),
        )


@dataclass
class NodeDescriptor:
    """
    Canonical NodeDescriptor model.
    Describes a node in the distributed stock exchange cluster.
    """
    node_id: str
    host: str
    port: int
    priority: int
    role: str = NodeRole.BACKUP
    is_alive: bool = True

    def __post_init__(self):
        self.node_id = str(self.node_id).strip()
        self.host = str(self.host).strip()
        self.port = int(self.port)
        self.priority = int(self.priority)
        self.role = str(self.role).strip().upper()
        if self.role not in NodeRole.all():
            raise ValueError(f"Invalid role: '{self.role}'. Must be one of {NodeRole.all()}")

    @property
    def url(self) -> str:
        """XML-RPC URL for contacting this node."""
        return f"http://{self.host}:{self.port}/RPC2"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for serialization."""
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "priority": self.priority,
            "role": self.role,
            "is_alive": self.is_alive,
            "url": self.url,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeDescriptor":
        """Reconstruct NodeDescriptor from dict."""
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data).__name__}")
        return cls(
            node_id=str(data["node_id"]),
            host=str(data["host"]),
            port=int(data["port"]),
            priority=int(data["priority"]),
            role=str(data.get("role", NodeRole.BACKUP)),
            is_alive=bool(data.get("is_alive", True)),
        )
