import time
import xmlrpc.client

PRIMARY_URL = "http://localhost:8000/"
BACKUP_URL = "http://localhost:8001/"


def get_active_proxy():
    """Ask each known node who is currently primary and use that one."""
    for url in (PRIMARY_URL, BACKUP_URL):
        try:
            proxy = xmlrpc.client.ServerProxy(url, allow_none=True)
            status = proxy.get_status()
            if status["role"] == "primary":
                return proxy, url
        except Exception:
            continue
    return None, None


def submit_order(trader, symbol, side, price, qty):
    proxy, url = get_active_proxy()
    if proxy is None:
        print("No primary reachable right now — both nodes down or mid-failover.")
        return
    order = {"trader": trader, "symbol": symbol, "side": side, "price": price, "qty": qty}
    try:
        result = proxy.submit_order(order)
        print(f"[client] via {url} -> {result}")
    except Exception as e:
        print(f"[client] submit failed on {url}: {e}")


if __name__ == "__main__":
    demo_orders = [
        ("alice", "STOCK_A", "buy", 101.5, 10),
        ("bob", "STOCK_A", "sell", 101.5, 5),
        ("carol", "STOCK_B", "buy", 55.0, 20),
        ("dave", "STOCK_A", "buy", 102.0, 8),
        ("erin", "STOCK_B", "sell", 54.5, 12),
    ]
    for trader, symbol, side, price, qty in demo_orders:
        submit_order(trader, symbol, side, price, qty)
        time.sleep(2)
