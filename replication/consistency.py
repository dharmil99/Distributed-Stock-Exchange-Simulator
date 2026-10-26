"""
Data Consistency and Replication Models for Distributed Stock Exchange Simulator.

Implements Lab 5 consistency policies:
  - Strong Consistency: Synchronous quorum replication. The primary blocks until
    a write quorum (W) of replicas acknowledge before confirming to the client.
    Guarantees that read quorums (R) overlap the write quorum (W + R > N),
    preventing stale reads.
  - Eventual Consistency: Asynchronous background replication. The primary accepts
    and commits locally, replies immediately to the client, and propagates updates
    to backups asynchronously, trading instant consistency for minimal latency.
"""


class ConsistencyMode:
    STRONG = "strong"
    EVENTUAL = "eventual"

    @classmethod
    def all(cls):
        return {cls.STRONG, cls.EVENTUAL}

    @classmethod
    def normalize(cls, mode: str) -> str:
        m = str(mode).strip().lower()
        if m not in cls.all():
            raise ValueError(f"Invalid consistency mode: '{mode}'. Must be 'strong' or 'eventual'.")
        return m
