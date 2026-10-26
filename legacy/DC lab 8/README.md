# Lab 8 — Fault Tolerance (Primary–Backup Failover)

Static primary-backup replication using Docker Compose. No leader election yet
(that's Lab 4) — the primary/backup roles are fixed at startup, and the backup
promotes itself only when the primary stops responding.

## Files

- `common.py` — Lamport clock (from Lab 3).
- `server.py` — matching engine: accepts orders (if primary), replicates them
  to its peer, and runs a heartbeat thread (if backup) that watches the primary.
- `client.py` — trader client; auto-detects whichever node currently reports
  itself as primary and submits there.
- `Dockerfile` — image used for both nodes (same code, different env vars).
- `docker-compose.yml` — spins up `primary` (port 8000) and `backup` (port 8001)
  on a shared Docker network, where each container can reach the other by name.

## Running it

1. Install Docker Desktop (or Docker Engine + Compose) if you don't have it.
2. From this folder, build and start both nodes:

   ```
   docker compose up --build
   ```

   You'll see both containers' logs interleaved in one terminal, each order
   accepted on `primary` immediately followed by a `replicated` log line on
   `backup`.

3. In a second terminal, submit some orders:

   ```
   python3 client.py
   ```

   Watch the `primary` logs accept each order and the `backup` logs replicate it.

## Demoing the failover (the actual point of Lab 8)

1. With everything running, kill the primary mid-session:

   ```
   docker compose stop primary
   ```

2. Watch the `backup` terminal — after ~3 missed heartbeats (about 6 seconds)
   you'll see:

   ```
   *** PRIMARY UNREACHABLE — PROMOTING SELF TO PRIMARY (retained N replicated orders) ***
   ```

3. Run `python3 client.py` again (or submit orders manually). The client checks
   both nodes and finds `backup` now reports `role: primary`, so it sends new
   orders there — and `backup` already has every order the old primary had
   accepted, because they were replicated as they happened.

4. Bring the old primary back if you want to inspect it:

   ```
   docker compose start primary
   ```

   Note: it comes back still thinking it's "primary" and out of sync with the
   new state — reconciling that on recovery is exactly what Lab 4 (election)
   and a more complete version of Lab 5 (replication) will handle. For this
   lab, the one-way failover is the deliverable.

## Useful Docker commands while working on this

- `docker compose up --build` — rebuild images and start both containers.
- `docker compose ps` — see which containers are running.
- `docker compose logs -f backup` — follow just the backup's logs.
- `docker compose stop primary` — simulate a crash (used above).
- `docker compose down` — stop and remove both containers + the network.
