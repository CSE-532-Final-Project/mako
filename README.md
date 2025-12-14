# Mako

<div align="center">

![CI](https://github.com/makodb/mako/actions/workflows/ci.yml/badge.svg)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![OSDI'25](https://img.shields.io/badge/OSDI'25-Mako-orange.svg)](#what-is-mako)

**High-Performance Distributed Transactional Key-Value Store with Geo-Replication Support**

[What is Mako?](#what-is-mako) • [Quick Start](#quick-start) • [Replication Layers](#replication-layers) • [Benchmarks](#benchmarks)

</div>

---

## What is Mako?

**Mako** is a high-performance distributed transactional key-value store system with geo-replication support, built on cutting-edge systems research.
Mako's core design-level innovation is **decoupling transaction execution from replication** using a novel speculative 2PC protocol. Unlike traditional systems where transactions must wait for replication and persistence before committing, Mako allows distributed transactions to execute speculatively without blocking on cross-datacenter consensus. Transactions run at full speed locally while replication happens asynchronously in the background, achieving fault-tolerance without sacrificing performance. The system employs novel mechanisms to prevent unbounded cascading aborts when shards fail during replication, ensuring both high throughput (processing **3.66M TPC-C transactions per second** with 10 shards replicated across the continent) and strong consistency guarantees. More details can be found in our [OSDI'25 paper](https://www.usenix.org/conference/osdi25/presentation/shen-weihai).

---

## Replication Layers

Mako supports **two replication backends** that can be selected at build time:

| Replication | Build Command | Binary | Use Case |
|-------------|---------------|--------|----------|
| **Paxos** (default) | `make -j32` | `dbtest` | Production Mako with Paxos consensus |
| **Raft** | `make mako-raft -j64` | `dbtest` | Mako with Raft as replication layer |

Additionally, **Raft can run standalone** (without Mako) via `deptran_server`:

| Mode | Build Command | Binary | Use Case |
|------|---------------|--------|----------|
| **Standalone Raft** | `make -j32` or `make mako-raft -j64` | `deptran_server` | Raft consensus without Mako transactions |
| **Raft Lab Tests** | `make raft-test -j32` | `deptran_server` | Raft coroutine-based lab test suite only |

> **Note**: `make raft-test` enables `RAFT_TEST` coroutines for the lab test harness. This mode is **only for running `config/raft_lab_test.yml`** - the normal concurrency configs (like `12c1s3r1p.yml`) won't work with this build.

### Understanding the Difference

- **Mako + Paxos**: Original Mako system using Paxos for replication (`./ci/ci.sh`)
- **Mako + Raft**: Mako transactions with Raft as the replication layer (`./ci/ci_mako_raft.sh`)
- **Standalone Raft**: Pure Raft consensus testing via `deptran_server` (no Mako transactions) - use regular `make` build
- **Raft Lab Tests**: Coroutine-based Raft tests (`make raft-test`) - only for `raft_lab_test.yml`

---

## Why Choose Mako?

### Proven Research & Performance
- Backed by peer-reviewed research published at OSDI'25, one of the top-tier systems conferences
- **8.6× higher throughput** than state-of-the-art geo-replicated systems
- Processing **3.66M TPC-C transactions per second** with geo-replication

### Core Capabilities
- **Serializable Transactions**: Strongest isolation level with full ACID guarantees across distributed partitions
- **Geo-Replication**: Multi-datacenter support with configurable consistency for disaster recovery
- **Pluggable Replication**: Choose between Paxos or Raft consensus protocols
- **High-Performance Storage**: Built on **Masstree** for in-memory indexing; RocksDB backend for persistence
- **Horizontal Scalability**: Automatic sharding and data partitioning across nodes
- **Fault Tolerance**: Crash recovery and replication for high availability
- **Advanced Networking**: DPDK support for kernel bypass and ultra-low latency
- **Rust-like memory safety** by using RustyCpp for borrow checking and lifetime analysis

### Developer-Friendly
- **Industry-standard benchmarks**: TPC-C, TPC-A, read-write workloads, and micro-benchmarks
- **RocksDB-like interface** for easy migration from single-node deployments
- **Redis-compatible layer** for familiar API with enhanced consistency
- Comprehensive test suite
- Modular architecture for extensions

---

## Quick Start

### Prerequisites

Tested on **Debian 12** and **Ubuntu 22.04**.

### Installation

```bash
# 1. Clone the repository with submodules
git clone --recursive https://github.com/makodb/mako.git
cd mako

# 2. Install dependencies
bash apt_packages.sh
source install_rustc.sh
bash src/mako/update_config.sh

# 3. Build (use fewer cores on PC, e.g., -j4)
make -j32
```

---

## Build System

### Build Targets

| Target | Command | Description |
|--------|---------|-------------|
| **Mako + Paxos** | `make -j32` | Default build with Paxos replication (also builds `deptran_server`) |
| **Mako + Raft** | `make mako-raft -j64` | Mako with Raft as replication layer (also builds `deptran_server`) |
| **Raft Lab Tests** | `make raft-test -j32` | Raft coroutine lab tests only (breaks normal configs) |
| **Clean** | `make clean` | Remove all build artifacts |
| **Help** | `make help` | Show all available targets |

### Output Binaries

| Binary | Description |
|--------|-------------|
| `build/dbtest` | Main Mako binary (works with both Paxos and Raft replication) |
| `build/deptran_server` | Standalone Raft server (for Raft-only testing without Mako) |
| `build/simpleRaft` | Simple Raft replication test |
| `build/simpleTransactionRepRaft` | Raft-based transaction replication test |

### CMake Options

| Option | Default | Description |
|--------|---------|-------------|
| `MAKO_USE_RAFT` | `OFF` | Enable Raft as Mako's replication layer |
| `RAFT_TEST` | `OFF` | Enable standalone Raft testing (no Mako) |
| `PAXOS_LIB_ENABLED` | `1` | Build Paxos components |
| `SHARDS` | `3` | Default shard count |

---

## Running Tests

### Mako + Paxos Tests

Use `./ci/ci.sh` for testing Mako with **Paxos** replication:

```bash
# Run all Paxos CI tests
./ci/ci.sh all

# Individual tests
./ci/ci.sh simpleTransaction       # Simple transactions
./ci/ci.sh simplePaxos             # Paxos replication
./ci/ci.sh shard1Replication       # 1-shard with replication
./ci/ci.sh shard2Replication       # 2-shards with replication
./ci/ci.sh shard1ReplicationSimple
./ci/ci.sh shard2ReplicationSimple
./ci/ci.sh rocksdbTests            # RocksDB persistence
./ci/ci.sh shardFaultTolerance     # Fault tolerance
./ci/ci.sh multiShardSingleProcess
./ci/ci.sh cpuThrottlingScaling
```

### Mako + Raft Tests

Use `./ci/ci_mako_raft.sh` for testing Mako with **Raft** replication:

```bash
# Build Mako with Raft first
make mako-raft -j64

# Run all Mako-Raft CI tests
./ci/ci_mako_raft.sh all

# Individual tests
./ci/ci_mako_raft.sh compile                    # Build with Raft
./ci/ci_mako_raft.sh simpleRaft                 # Simple Raft replication
./ci/ci_mako_raft.sh shard1ReplicationRaft      # 1-shard Raft
./ci/ci_mako_raft.sh shard2ReplicationRaft      # 2-shard Raft
./ci/ci_mako_raft.sh shard1ReplicationSimpleRaft
./ci/ci_mako_raft.sh shard2ReplicationSimpleRaft
./ci/ci_mako_raft.sh cleanup                    # Clean up processes
```

### Standalone Raft Tests (No Mako)

Use `deptran_server` for testing **Raft consensus only** (without Mako transactions).

#### Running Standalone Raft (use regular build)

```bash
# Build (regular make, NOT raft-test)
make -j32

# Basic Raft (1 client, 1 shard, 3 replicas)
./build/deptran_server \
  -f config/none_raft.yml \
  -f config/1c1s3r1p.yml \
  -f config/rw.yml \
  -f config/client_closed.yml \
  -f config/concurrent_1.yml \
  -d 30 -m 100 -P localhost

# Higher concurrency (12 clients, ~25k TPS)
./build/deptran_server \
  -f config/none_raft.yml \
  -f config/12c1s3r1p.yml \
  -f config/rw.yml \
  -f config/client_closed.yml \
  -f config/concurrent_12.yml \
  -d 30 -m 100 -P localhost
```

#### Raft Lab Tests (use raft-test build)

```bash
# Build with RAFT_TEST coroutines (only for lab tests)
make raft-test -j32

# Run Raft lab test suite
./build/deptran_server -f config/raft_lab_test.yml
```

> **Warning**: The `make raft-test` build enables special coroutines for the lab harness. The normal concurrency configs (`1c1s3r1p.yml`, `12c1s3r1p.yml`, etc.) will **not work** with this build.

### Unit Tests

```bash
# CTest integration
make test                 # Run all tests
make test-verbose         # Verbose output
make test-parallel        # Parallel execution

# Silo/STO unit tests
cd tests && ./run_tests.sh all
```

---

## Running Mako with Paxos

### Single Machine Setup (1 Leader + 2 Followers + 1 Learner)

```bash
# Build
make -j32

# Start followers and leader in separate terminals
# Follower p1
./build/dbtest --verbose --bench tpcc --basedir ./tmp \
  --db-type mbta --num-threads 6 --scale-factor 6 \
  -F config/1leader_2followers/paxos6_shardidx0.yml -F config/occ_paxos.yml \
  --txn-flags 1 --runtime 30 -P p1 &

# Follower p2
./build/dbtest --verbose --bench tpcc --basedir ./tmp \
  --db-type mbta --num-threads 6 --scale-factor 6 \
  -F config/1leader_2followers/paxos6_shardidx0.yml -F config/occ_paxos.yml \
  --txn-flags 1 --runtime 30 -P p2 &

# Leader
./build/dbtest --verbose --bench tpcc --basedir ./tmp \
  --db-type mbta --num-threads 6 --scale-factor 6 \
  -F config/1leader_2followers/paxos6_shardidx0.yml -F config/occ_paxos.yml \
  --txn-flags 1 --runtime 30 -P localhost &

# Monitor logs
tail -f leader.log p1.log p2.log
```

### One-Command Local TPC-C Run

To avoid juggling multiple terminals you can run the same 1-leader + 2-follower
setup with a helper script:

```bash
# Default: 6 threads, 6 warehouses, 30s runtime
./scripts/run_local_tpcc.sh

# Override settings 
./scripts/run_local_tpcc.sh --threads 4 --scale 1
```

The script ensures `build/dbtest` is present, launches both follower replicas in
the background, streams the leader output to `logs/local_tpcc/leader.log`, and
cleans up the background processes when the run completes. Extra `dbtest`
arguments can be appended after `--`, e.g. pass `-- --workload-mix 45,44,4,4,3`
or change runtime via `-- --runtime 45`.

---

## Running Mako with Raft

### Build and Test

```bash
# Build Mako with Raft replication
make mako-raft -j64

# Run the Mako-Raft CI suite
./ci/ci_mako_raft.sh all

# Or run individual tests
./ci/ci_mako_raft.sh simpleRaft
./ci/ci_mako_raft.sh shard1ReplicationRaft
```

The `dbtest` binary with Raft replication runs Mako transactions but uses Raft (instead of Paxos) for log replication and leader election.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client Applications                   │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│              Transaction Coordinators                    │
│  ┌──────────┬──────────┬──────────┬──────────┐         │
│  │  Mako    │   2PL    │   OCC    │  Janus   │         │
│  └──────────┴──────────┴──────────┴──────────┘         │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│            Replication Layer (Pluggable)                 │
│         ┌──────────────┬──────────────┐                 │
│         │    Paxos     │     Raft     │                 │
│         └──────────────┴──────────────┘                 │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│                RPC Communication Layer                   │
│           (TCP/IP, DPDK, RDMA, eRPC)                    │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│              Sharded Data Partitions                     │
│  ┌─────────────┬─────────────┬─────────────┐           │
│  │   Shard 1   │   Shard 2   │   Shard N   │           │
│  │  (Replicas) │  (Replicas) │  (Replicas) │           │
│  └─────────────┴─────────────┴─────────────┘           │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│              Storage Backends                            │
│    Masstree (In-Memory)  |  RocksDB (Persistent)        │
└─────────────────────────────────────────────────────────┘
```

---

## Benchmarks

Performance results from our OSDI'25 evaluation on Azure cloud infrastructure (TPC-C benchmark):

| Configuration | Shards | Threads/Shard | Throughput | Notes |
|--------------|--------|---------------|------------|-------|
| Single Shard | 1 | 24 | 960K TPS | 22.5× faster than Calvin |
| Geo-Replicated | 10 | 24 | 3.66M TPS | 8.6× faster than Calvin |

### Performance Advantages

- **8.6× higher throughput** than Calvin (state-of-the-art geo-replicated system)
- **22.5× higher throughput** than Calvin at single shard
- **32.2× higher throughput** than OCC+OR at 10 shards

---

## Configuration

Update host maps for distributed runs:

```bash
bash ./src/mako/update_config.sh
```

Key configuration directories:

| Directory | Purpose |
|-----------|---------|
| `config/hosts*.yml` | Host topology |
| `config/rw.yml`, `config/concurrent_*.yml` | Workload settings |
| `config/occ_paxos.yml`, `config/1leader_2followers/` | Paxos protocol |
| `config/none_raft.yml`, `config/rule_raft.yml` | Raft protocol |

---

## Project Structure

```
mako/
├── src/
│   ├── deptran/           # Transaction protocols
│   │   ├── paxos/         # Paxos replication
│   │   ├── raft/          # Raft replication
│   │   └── ...
│   ├── mako/              # Mako core (Masstree, watermarks)
│   ├── bench/             # Benchmarks (TPC-C, TPC-A, RW)
│   ├── rrr/               # RPC framework
│   └── memdb/             # In-memory datastore
├── config/                # YAML configurations
├── ci/
│   ├── ci.sh              # Mako + Paxos tests
│   └── ci_mako_raft.sh    # Mako + Raft tests
├── examples/
│   └── mako-raft-tests/   # Mako-Raft test scripts
├── tests/                 # Unit tests
├── third-party/           # Dependencies
└── rust-lib/              # Rust components
```

---

## Use Cases

### Distributed RocksDB Alternative

Mako provides a familiar key-value API with distributed transactions, geo-replication, and fault tolerance. Perfect for applications that need:
- **Horizontal scalability** across multiple nodes
- **ACID transactions** spanning multiple keys or partitions
- **Geographic replication** for disaster recovery

### Redis Alternative with Transactions

Mako includes a Redis-compatible layer for:
- **Strong consistency** with serializable transactions
- **Multi-key atomic operations** with full ACID guarantees
- **Geographic distribution** with automatic failover

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Frequent Raft leader churn | Increase heartbeat interval in `config/none_raft.yml` |
| Commands stuck uncommitted | Check connectivity and `match_index_` in logs |
| Build failures after CMake edits | Re-run `cmake -B build ...` before building |
| Hanging test processes | Run `./ci/ci_mako_raft.sh cleanup` |

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Ensure all tests pass (`make test`)
5. Submit a pull request

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **Research Team**: Mako research and development team
- **Contributors**: All researchers and students who have contributed
- **Dependencies**: Built on Janus, Masstree, RocksDB, eRPC, and other open-source projects
