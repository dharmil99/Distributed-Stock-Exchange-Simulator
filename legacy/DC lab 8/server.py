import os
import time
import threading
import xmlrpc.client
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer

from common import LamportClock


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    """XML-RPC server that handles each incoming call on its own thread (Lab 2)."""
    daemon_threads = True


class MatchingEngine:
    def __init__(self, role, peer_host, peer_port):
        self.role = role                      # "primary" or "backup"
        self.peer_url = f"http://{peer_host}:{peer_port}/"
        self.clock = LamportClock()
        self.order_book = []
        self.lock = threading.RLock()
        self.next_order_id = 1
        self.missed_heartbeats = 0
        self.max_missed = 3                   # promote after 3 missed pings
        self.heartbeat_interval = 2           # seconds

    # ---------- methods exposed over XML-RPC ----------

    def submit_order(self, order):
        with self.lock:
            if self.role != "primary":
                return {"status": "rejected", "reason": f"this node is '{self.role}', not primary"}
            ts = self.clock.tick()
            order["id"] = self.next_order_id
            order["timestamp"] = ts
            self.next_order_id += 1
            self.order_book.append(order)
            print(f"[{self.role.upper()}] accepted order #{order['id']} @ clock={ts}: {order}", flush=True)
            self._replicate(order)
            return {"status": "accepted", "order_id": order["id"], "timestamp": ts}

    def replicate_order(self, order):
        """Called BY the primary ON the backup to copy state over."""
        with self.lock:
            self.clock.update(order["timestamp"])
            if not any(o["id"] == order["id"] for o in self.order_book):
                self.order_book.append(order)
                if order["id"] >= self.next_order_id:
                    self.next_order_id = order["id"] + 1
            print(f"[{self.role.upper()}] replicated order #{order['id']}", flush=True)
            return {"status": "replicated"}

    def ping(self):
        return {"alive": True, "role": self.role, "clock": self.clock.value()}

    def get_order_book(self):
        with self.lock:
            return list(self.order_book)

    def get_status(self):
        with self.lock:
            return {"role": self.role, "orders": len(self.order_book), "clock": self.clock.value()}

    # ---------- internal helpers (not exposed) ----------

    def _replicate(self, order):
        try:
            proxy = xmlrpc.client.ServerProxy(self.peer_url, allow_none=True)
            proxy.replicate_order(order)
        except Exception as e:
            print(f"[{self.role.upper()}] WARNING: could not replicate to peer ({e})", flush=True)

    def _heartbeat_loop(self):
        """Only a backup watches its peer; a primary just idles here."""
        proxy = xmlrpc.client.ServerProxy(self.peer_url, allow_none=True)
        while True:
            time.sleep(self.heartbeat_interval)
            if self.role != "backup":
                continue
            try:
                resp = proxy.ping()
                if resp.get("role") == "primary":
                    self.missed_heartbeats = 0
            except Exception:
                self.missed_heartbeats += 1
                print(f"[{self.role.upper()}] missed heartbeat #{self.missed_heartbeats}", flush=True)
                if self.missed_heartbeats >= self.max_missed:
                    self._promote()

    def _promote(self):
        with self.lock:
            if self.role == "primary":
                return
            self.role = "primary"
            print(f"*** PRIMARY UNREACHABLE — PROMOTING SELF TO PRIMARY "
                  f"(retained {len(self.order_book)} replicated orders) ***", flush=True)


def main():
    role = os.environ.get("ROLE", "primary")
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    peer_host = os.environ.get("PEER_HOST", "localhost")
    peer_port = int(os.environ.get("PEER_PORT", "8001"))

    engine = MatchingEngine(role, peer_host, peer_port)

    server = ThreadedXMLRPCServer((host, port), allow_none=True, logRequests=False)
    server.register_instance(engine)

    threading.Thread(target=engine._heartbeat_loop, daemon=True).start()

    print(f"[{role.upper()}] matching engine listening on {host}:{port}, "
          f"peer={peer_host}:{peer_port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
