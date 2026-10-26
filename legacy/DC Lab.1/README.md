# Lab 1 (Python): Client-Server Communication using RPC
### Project: Distributed Trading / Order Matching Engine

## What this demonstrates
A **trader client** submits buy/sell orders to a **matching engine server**
using XML-RPC (Python's built-in remote procedure call library — the
closest Python equivalent to Java RMI). The server assigns each order an
ID, stores it, and confirms back to the client.

## Files
- `order.py` — shared helper functions to create/format an order (a plain dict,
  since XML-RPC can only send simple types, not custom classes).
- `matching_engine_server.py` — starts the XML-RPC server and exposes
  `submit_order()` and `get_order_book()` as remote methods.
- `trader_client.py` — connects to the server and submits orders.

## Requirements
Python 3 only — `xmlrpc` is part of the standard library, no `pip install` needed.

## How to run

### 1. Start the server (Terminal 1)
```bash
python3 matching_engine_server.py
```
You should see:
```
[SERVER] Matching engine server is up.
[SERVER] Listening on localhost:8000
[SERVER] Waiting for trader clients to connect...
```
Leave this running.

### 2. Run a client (Terminal 2)

**Single order:**
```bash
python3 trader_client.py localhost 8000 trader1 BUY STOCK_A 101.5 10
```

**Interactive mode (submit multiple orders in a session):**
```bash
python3 trader_client.py localhost 8000 trader1
> BUY STOCK_A 101.5 10
> SELL STOCK_B 55.25 20
> book
> exit
```

You can open more terminals and run more `trader_client.py` instances
(e.g. `trader2`, `trader3`) — they all talk to the same server and share
the same order book.

### 3. Run across two different machines (optional)
1. On the server machine, run `matching_engine_server.py` and note its IP address.
   You may want to change `HOST = "localhost"` to `HOST = "0.0.0.0"` in
   `matching_engine_server.py` so it accepts connections from other machines.
2. On the client machine, run:
   ```bash
   python3 trader_client.py <server_ip> 8000 trader1 BUY STOCK_A 101.5 10
   ```
   Make sure port `8000` is open between the machines.

## What to point out in your lab report / viva
- **RPC over XML-RPC**: `proxy.submit_order(order)` in `trader_client.py`
  looks like a normal function call, but it's actually serialized into an
  XML message, sent over HTTP to the server, executed there, and the
  result is sent back and deserialized.
- **Marshalling**: XML-RPC only supports simple types (dict, list, str,
  int, float, bool) — this is why orders are represented as plain dicts
  instead of custom Python objects.
- **Service exposure**: `server.register_function(...)` is XML-RPC's way
  of publishing a callable method to remote clients — conceptually the
  same role as the RMI registry binding in the Java version.
- **Limitations of this lab (intentional — fixed in later labs)**:
  - No matching logic yet — orders are just stored, not matched.
  - Only one server — no replication or fault tolerance yet.
  - `SimpleXMLRPCServer` handles one request at a time by default — Lab 2
    adds proper concurrency (threading) so multiple clients can be served
    at once without blocking each other.

## Next step (Lab 2)
Make the server handle multiple trader clients concurrently using Python's
`threading` module (or `socketserver.ThreadingMixIn`), and start reasoning
about race conditions on the shared order book (e.g. using a `Lock`).
