# Raft-based Distributed Key-Value Store

A fault-tolerant, sharded key-value store built on the Raft consensus algorithm, providing linearizable semantics under failures and network partitions.

---

## Overview

This project implements a distributed key-value storage system inspired by production-grade distributed systems.  
It is designed to remain available and consistent in the presence of node crashes, leader changes, and dynamic reconfiguration.

The system is built around a replicated state machine using **Raft**, with support for log compaction via snapshots and scalable sharding across multiple replication groups.

---

## Architecture

The system consists of the following major components:

- **Raft Consensus Module**
  - Leader election and log replication
  - Persistent state and crash recovery
  - Snapshot-based log compaction

- **Key-Value Service**
  - Linearizable `Get / Put / Append` operations
  - Client request deduplication to ensure at-most-once semantics
  - Transparent leader forwarding

- **Sharding Layer**
  - Partitioned key space across multiple Raft-backed groups
  - Replicated shard controller managing configurations
  - Online shard migration during reconfiguration

All components communicate via RPC and are designed to tolerate partial failures.

---

## Key Features

- **Fault Tolerance**
  - Continues operation despite node crashes and restarts
  - Automatic leader re-election

- **Strong Consistency**
  - Linearizable client operations
  - Deterministic state machine execution

- **Scalability**
  - Sharded key space with dynamic rebalancing
  - Independent Raft groups per shard set

- **Efficiency**
  - Snapshot-based log compaction
  - Bounded log growth under long-running workloads

---

## Consistency & Failure Handling

- Client requests are uniquely identified and deduplicated to prevent re-execution.
- State is persisted to stable storage to support crash recovery.
- Configuration changes are serialized through the replicated shard controller.
- Shard migration preserves correctness during partial availability.
