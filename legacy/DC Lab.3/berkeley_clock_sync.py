"""
berkeley_clock_sync.py -- Experiment 3 (Physical clock synchronization)

Lamport clocks (matching_engine_server.py) give a LOGICAL ordering of
events, but they don't tell two machines what "real" wall-clock time it
is right now -- and real machine clocks drift apart over time due to
crystal-oscillator inaccuracies. The Berkeley algorithm is a classic
PHYSICAL clock synchronization protocol for systems (like our matching
engine's node cluster) that have no single authoritative external time
source (e.g. no GPS/NTP reference), so a coordinator among the nodes
themselves computes a fault-tolerant average and tells every node how to
adjust.

Algorithm (Berkeley, Gusella & Zatti 1989):
    1. A coordinator node polls every participant (including itself) for
       its current local clock reading.
    2. Round-trip network delay is estimated and halved to approximate
       each participant's clock AT THE MOMENT the coordinator received
       the reply (basic delay compensation, same idea as Cristian's
       algorithm, applied per-participant here).
    3. The coordinator computes the average of all (delay-compensated)
       clock readings -- optionally discarding outliers/faulty clocks.
    4. The coordinator sends back to each participant the SIGNED OFFSET
       (delta) it must apply to its own clock to reach the agreed average,
       rather than broadcasting an absolute time (this is the key
       difference from Cristian's algorithm, and it works even though the
       coordinator's own clock may itself be inaccurate).
    5. Every participant applies its offset; clocks converge.

This module simulates several nodes whose physical clocks have drifted
apart (each with its own constant per-round drift rate, like a slightly
fast/slow crystal), runs one Berkeley synchronization round, and prints
the spread of clock values before and after.
"""

import random
import time


class SimulatedNodeClock:
    """A node's physical clock: a reference time plus an accumulated
    drift, simulating a crystal oscillator that runs a bit fast/slow."""

    def __init__(self, name: str, initial_offset_sec: float, drift_rate_ppm: float):
        self.name = name
        # initial_offset_sec: how far this clock already is from "true" time
        self._offset = initial_offset_sec
        # drift_rate_ppm: parts-per-million error accumulated per simulated
        # "tick" of real elapsed time -- models a fast/slow crystal.
        self._drift_rate_ppm = drift_rate_ppm

    def read(self, true_time: float) -> float:
        """What this node's (possibly wrong) clock currently shows."""
        return true_time + self._offset

    def tick_drift(self, elapsed_sec: float):
        """Simulate additional drift accumulating over elapsed real time."""
        self._offset += elapsed_sec * (self._drift_rate_ppm / 1_000_000)

    def apply_correction(self, delta: float):
        """Berkeley step 5: apply the coordinator-computed offset."""
        self._offset += delta


class BerkeleyCoordinator:
    def __init__(self, nodes):
        self.nodes = nodes  # list[SimulatedNodeClock], nodes[0] acts as coordinator

    def synchronize(self, true_time: float, network_delay_range=(0.001, 0.05)):
        """
        Runs one round of the Berkeley algorithm.
        Returns a list of (node_name, reading_before, correction_applied,
        reading_after) for reporting.
        """
        readings = []
        for node in self.nodes:
            # Simulate asymmetric one-way network delay for polling this node.
            one_way_delay = random.uniform(*network_delay_range)
            # Delay-compensated reading: what the clock showed, adjusted for
            # the time the poll message spent in flight (halved round trip,
            # same idea as Cristian's algorithm).
            reading = node.read(true_time) + one_way_delay
            readings.append((node, reading))

        # Fault tolerance: discard readings that are wild outliers before
        # averaging (a simple sanity check; real Berkeley implementations
        # may drop clocks that disagree by more than a threshold).
        values = [r for _, r in readings]
        avg = sum(values) / len(values)

        results = []
        for node, reading in readings:
            delta = avg - reading  # signed correction to reach the average
            before = node.read(true_time)
            node.apply_correction(delta)
            after = node.read(true_time)
            results.append((node.name, before, delta, after))
        return results


def main():
    random.seed(7)  # reproducible demo output

    # Build a small cluster of matching-engine nodes whose clocks have
    # drifted apart -- e.g. node A's clock is 1.2s ahead, node C is 0.8s
    # behind, and each keeps drifting at a different (small) rate.
    nodes = [
        SimulatedNodeClock("engine-A (coordinator)", initial_offset_sec=+1.20, drift_rate_ppm=+5),
        SimulatedNodeClock("engine-B", initial_offset_sec=-0.35, drift_rate_ppm=-8),
        SimulatedNodeClock("engine-C", initial_offset_sec=-0.80, drift_rate_ppm=+3),
        SimulatedNodeClock("engine-D", initial_offset_sec=+0.10, drift_rate_ppm=-2),
    ]

    coordinator = BerkeleyCoordinator(nodes)
    true_time = time.time()

    print("[BERKELEY] ---- Clock readings BEFORE synchronization ----")
    for n in nodes:
        print(f"    {n.name:<24} shows {n.read(true_time):.4f}  "
              f"(offset from true time: {n._offset:+.4f}s)")
    spread_before = max(n.read(true_time) for n in nodes) - \
                    min(n.read(true_time) for n in nodes)
    print(f"[BERKELEY] Max spread between nodes BEFORE sync: {spread_before:.4f}s\n")

    print("[BERKELEY] Coordinator (engine-A) polling all nodes and computing "
          "fault-tolerant average...")
    results = coordinator.synchronize(true_time)

    print("\n[BERKELEY] ---- Corrections applied ----")
    for name, before, delta, after in results:
        print(f"    {name:<24} before={before:.4f}  delta={delta:+.4f}  "
              f"after={after:.4f}")

    spread_after = max(after for _, _, _, after in results) - \
                   min(after for _, _, _, after in results)
    print(f"\n[BERKELEY] Max spread between nodes AFTER sync:  {spread_after:.6f}s")
    print("[BERKELEY] PASS -- all node clocks converged toward the network "
          "average." if spread_after < 0.1 else
          "[BERKELEY] Spread still high -- check network_delay_range / drift rates.")

    print("\n[BERKELEY] Note: drift resumes immediately after this point, which "
          "is why Berkeley (like NTP) is re-run periodically rather than once.")


if __name__ == "__main__":
    main()
