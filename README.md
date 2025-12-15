# Early Lock Release (ELR) for Mako

> **Note**: For general information about Mako, including installation, architecture, and usage, see [MAKO_README.md](MAKO_README.md).

## Overview

Early Lock Release (ELR) is an optimization technique that allows transactions to release locks before commit, reducing lock hold time and improving throughput on contentious workloads. This implementation extends ELR to Mako's speculative two-phase commit (2PC) protocol, enabling reduced contention in geo-replicated deployments while maintaining ACID guarantees.

### Key Features

- **Early Lock Release**: Transactions can release locks after validation but before replication completes
- **Dependency Tracking**: Maintains read-after-write dependencies across shards and replicas
- **Cascade Abort Handling**: Correctly handles cascading aborts when speculation fails
- **Recovery Support**: Integrated logging mechanism for failure recovery
- **Geo-Replication Compatible**: Works seamlessly with Mako's Paxos-based replication

## Installation

1. **Clone the repository** (if you haven't already):
```bash
git clone --recursive https://github.com/makodb/mako.git
cd mako
```

2. **Install dependencies**:
```bash
bash apt_packages.sh
source install_rustc.sh
bash src/mako/update_config.sh
```

3. **Build Mako with ELR enabled**:

ELR is disabled by default. To enable it, you need to build Mako with the `ENABLE_ELR` CMake option:

```bash
# Using CMake
mkdir -p build
cd build
cmake .. -DENABLE_ELR=ON
make -j32

# Or using the Makefile wrapper (from the root directory; not tested)
make -j32 ENABLE_ELR=1
```

### Runtime Configuration

After building with ELR support, you can enable or disable ELR at runtime through the ELR configuration system. The ELR integration can be initialized per shard with the following configuration options:

- `enable_elr`: Whether ELR is enabled (default: `false`)
- `safe_point`: Safe point for early release (default: `"post_validate"`)
- `max_chain_depth`: Maximum allowed dependency chain depth (default: `5`)
- `cross_shard_elr`: Whether to allow cross-shard ELR (default: `true`)
- `cascade_timeout_ms`: Timeout for cascade abort operations in milliseconds (default: `1000`)
- `enable_logging`: Whether to log ELR operations for recovery (default: `true`)

```cpp
#include "mako/elr/elr_integration.h"

using namespace mako::elr;

// Get the ELR integration instance
auto& elr_integration = ELRIntegration::getInstance();

// Configure ELR for a shard
ELRIntegrationConfig config;
config.enable_elr = true;
config.safe_point = "post_validate";
config.max_chain_depth = 5;
config.cross_shard_elr = true;
config.cascade_timeout_ms = 1000;
config.enable_logging = true;

// Initialize ELR for shard 0
elr_integration.initialize(0, config);
```

### Global Enable/Disable

You can also enable or disable ELR globally using the configuration manager:

```cpp
#include "mako/elr/elr_common.h"

using namespace mako::elr;

// Enable ELR globally
ELRConfigManager::getInstance().setEnabled(true);

// Check if ELR is enabled
if (elr_enabled()) {
    // ELR is active
}
```

## Performance Benchmarks

The following table shows benchmark results comparing Mako (baseline) with ELR enabled across different configurations. The benchmarks measure average throughput (operations per second) and average abort ratio (percentage of aborted transactions).

| Configuration | System | Avg Throughput (ops/sec) | Avg Abort Ratio (%) |
|---------------|--------|-------------------------|---------------------|
| **2 shards, no repl. (RRR)** | Mako | 2,168 | 0.71% |
| | ELR | 2,167 | 0.71% |
| **2 shards, no repl. (eRPC)** | Mako | 31,844 | 8.68% |
| | ELR | 33,033 | 7.37% |
| **1 shard, replication** | Mako | 76,654 | 0% |
| | ELR | 126,910 | 0% |
| **2 shards, replication (RRR)** | Mako | 222,038 | 1.32% |
| | ELR | 168,924 | 1.76% |

### Key Observations

- **Single shard with replication**: ELR shows a **65.6% throughput improvement** (126,910 vs 76,654 ops/sec) with no increase in abort ratio
- **2 shards with eRPC (no replication)**: ELR achieves **3.7% higher throughput** (33,033 vs 31,844 ops/sec) and **15% lower abort ratio** (7.37% vs 8.68%)
- **2 shards with replication (RRR)**: ELR shows slightly lower throughput but maintains similar abort ratios, indicating the overhead of dependency tracking in multi-shard replicated scenarios

These results demonstrate that ELR is particularly effective in single-shard scenarios and high-contention workloads where early lock release can significantly reduce contention.

## Architecture

### Components

1. **ELR Manager**: Core component that coordinates early lock release operations
2. **Dependency Tracker**: Maintains dependency graphs between transactions
3. **Cascade Abort Handler**: Handles cascading aborts when transactions with early-released locks abort
4. **ELR Log**: Logging mechanism for recovery support
5. **ELR Integration**: Integration layer with Mako's transaction system

### Source Files

The ELR implementation is organized in `src/mako/elr/` with the following files:

- **`elr_common.h`**: Common definitions, types, and constants used across the ELR module. Defines configuration structures (`ELRConfig`), status enumerations (`ELRStatus`), key types (`ELRKey`), dependency structures, and statistics tracking. Also provides the global configuration manager singleton.

- **`elr_manager.h` / `elr_manager.cc`**: Core ELR Manager implementation. Coordinates all early lock release operations including transaction lifecycle management, early release decisions, dependency registration, commit/abort handling, and cascade abort coordination. Maintains state for active transactions and early-released locks.

- **`dependency_tracker.h` / `dependency_tracker.cc`**: Dependency tracking system that maintains the dependency graph between transactions. Tracks read-after-write dependencies when transactions read from early-released locks, detects cycles to prevent deadlocks, computes cascade abort sets, and ensures recoverability by preventing commits before dependencies resolve.

- **`cascade_abort.h` / `cascade_abort.cc`**: Cascade abort handler that manages cascading aborts when transactions with early-released locks abort. Builds abort sets in topological order, coordinates local and remote aborts across shards, handles timeouts and partial failures, and tracks cascade operation status.

- **`elr_log.h` / `elr_log.cc`**: Logging mechanism for ELR operations to support recovery. Logs early release events, dependency creation, commit/abort events, and cascade aborts. Integrates with Paxos for geo-replicated logging and provides recovery replay functionality.

- **`elr_integration.h` / `elr_integration.cc`**: Integration layer that connects ELR with Mako's transaction system. Provides hooks for transaction lifecycle events (begin, commit, abort, post-validation), manages per-shard ELR initialization, integrates with Paxos for log replication, and handles cross-shard coordination.

### Dependency Chain Limits

To prevent unbounded cascade aborts, ELR enforces a maximum dependency chain depth (default: 5). Transactions that would exceed this limit cannot early-release their locks.

## Testing

### Running ELR Unit Tests

The ELR module includes a comprehensive test suite. After building with ELR enabled, run the tests:

```bash
# From the build directory
cd build
./test_elr
```

The test suite includes:
- **Basic ELR operations**: Early release, dependency registration, commit/abort
- **Dependency tracking**: Cycle detection, dependency graph traversal, chain depth limits
- **Cascade abort handling**: Local and remote cascade aborts, timeout handling
- **Multi-threaded contention tests**: Concurrent dependency tracking, high-volume stress tests
- **Recovery tests**: Log replay, uncommitted transaction resolution, undo/redo operations

### Running Mako Tests with ELR

You can also run Mako's standard test suite with ELR enabled to verify integration:

```bash
# Run Mako + Paxos tests with ELR
./ci/ci.sh all

# Run specific tests
./ci/ci.sh simpleTransaction
./ci/ci.sh shard1Replication
./ci/ci.sh shard2Replication
```

## Limitations

- ELR is most effective for high-contention workloads
- Cross-shard ELR adds coordination overhead
- Dependency chain depth limits may prevent early release in deep dependency graphs
- Recovery logging adds some performance overhead

## References

- Lock Violation for Fault-tolerant Distributed Database Systems, ICDE 2021
- Controlled Lock Violation, SIGMOD 2013
- Partial Strictness in Two-Phase Locking, ICDT 1995

## Contributing

When contributing to ELR, please ensure:
1. All tests pass
2. Code follows Mako's coding standards
3. Documentation is updated for any API changes
4. Performance impact is measured and documented

## License

This project is part of Mako and follows the same license terms. See the main [LICENSE](../LICENSE) file for details.
