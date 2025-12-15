/**
 * @file elr_common.h
 * @brief Common definitions for Early Lock Release (ELR) in Mako
 *
 * Early Lock Release allows transactions to release locks before commit,
 * reducing lock hold time and improving throughput on contentious workloads.
 * This file contains common types and constants used across the ELR module.
 *
 * References:
 * - Lock Violation for Fault-tolerant Distributed Database Systems, ICDE 2021
 * - Controlled Lock Violation, SIGMOD 2013
 * - Partial Strictness in Two-Phase Locking, ICDT 1995
 */

#ifndef _MAKO_ELR_COMMON_H_
#define _MAKO_ELR_COMMON_H_

#include <cstdint>
#include <string>
#include <vector>
#include <atomic>
#include <functional>

namespace mako {
namespace elr {

// Transaction ID type
using txnid_t = uint64_t;
// Version/timestamp type
using tid_t = uint64_t;
// Shard ID type
using shardid_t = uint32_t;
// Partition ID type
using parid_t = uint32_t;

/**
 * @brief ELR configuration options
 */
struct ELRConfig {
    // Whether ELR is enabled
    bool enabled = false;
    
    // Safe point for early release: "post_validate" or "post_lock"
    std::string safe_point = "post_validate";
    
    // Timeout for cascade abort in milliseconds
    uint32_t cascade_timeout_ms = 1000;
    
    // Maximum allowed dependency chain depth
    uint32_t max_dependency_chain = 5;
    
    // Whether to allow cross-shard ELR
    bool allow_cross_shard = true;
    
    // Minimum time to hold lock before early release (microseconds)
    uint64_t min_hold_time_us = 0;
    
    // Whether to log ELR operations for recovery
    bool enable_logging = true;
};

/**
 * @brief Status of an early-released lock
 */
enum class ELRStatus : uint8_t {
    NONE = 0,           // Not early-released
    PENDING = 1,        // Early release in progress
    RELEASED = 2,       // Successfully early-released
    COMMITTED = 3,      // Owner transaction committed
    ABORTED = 4,        // Owner transaction aborted
    CASCADE_ABORT = 5   // Cascade abort triggered
};

/**
 * @brief Type of ELR dependency
 */
enum class DependencyType : uint8_t {
    READ_UNCOMMITTED = 0,   // Read from uncommitted write
    WRITE_AFTER_READ = 1,   // Write after reading uncommitted
    ANTI_DEPENDENCY = 2     // Anti-dependency (write-read conflict)
};

/**
 * @brief Represents a key in the database
 */
struct ELRKey {
    shardid_t shard_id;
    uint16_t table_id;
    std::string key;
    
    bool operator==(const ELRKey& other) const {
        return shard_id == other.shard_id &&
               table_id == other.table_id &&
               key == other.key;
    }
    
    bool operator<(const ELRKey& other) const {
        if (shard_id != other.shard_id) return shard_id < other.shard_id;
        if (table_id != other.table_id) return table_id < other.table_id;
        return key < other.key;
    }
};

/**
 * @brief Hash function for ELRKey
 */
struct ELRKeyHash {
    size_t operator()(const ELRKey& k) const {
        size_t h1 = std::hash<uint32_t>{}(k.shard_id);
        size_t h2 = std::hash<uint16_t>{}(k.table_id);
        size_t h3 = std::hash<std::string>{}(k.key);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

/**
 * @brief Information about an early-released lock
 */
struct ELRLockInfo {
    txnid_t owner_txn;          // Transaction that owns (early-released) the lock
    tid_t version;              // Version of the value written
    ELRStatus status;           // Current status
    uint64_t release_time_us;   // When the lock was early-released
    std::vector<txnid_t> dependent_txns;  // Transactions that depend on this
    
    ELRLockInfo()
        : owner_txn(0), version(0), status(ELRStatus::NONE),
          release_time_us(0) {}
    
    ELRLockInfo(txnid_t owner, tid_t ver)
        : owner_txn(owner), version(ver), status(ELRStatus::PENDING),
          release_time_us(0) {}
};

/**
 * @brief Represents a dependency between transactions
 */
struct ELRDependency {
    txnid_t writer_txn;         // Transaction that early-released
    txnid_t reader_txn;         // Transaction that read uncommitted value
    ELRKey key;                 // Key involved
    tid_t version;              // Version read
    DependencyType type;        // Type of dependency
    bool writer_committed;      // Whether writer has committed
    bool reader_committed;      // Whether reader has committed
    uint64_t create_time_us;    // When dependency was created
    
    ELRDependency()
        : writer_txn(0), reader_txn(0), version(0),
          type(DependencyType::READ_UNCOMMITTED),
          writer_committed(false), reader_committed(false),
          create_time_us(0) {}
};

/**
 * @brief Callback types for ELR operations
 */
using ELRCommitCallback = std::function<void(txnid_t txn_id, bool success)>;
using ELRAbortCallback = std::function<void(txnid_t txn_id, const std::vector<txnid_t>& cascade_list)>;
using ELRDependencyCallback = std::function<void(const ELRDependency& dep)>;

/**
 * @brief Statistics for ELR operations
 */
struct ELRStats {
    std::atomic<uint64_t> total_early_releases{0};
    std::atomic<uint64_t> successful_commits{0};
    std::atomic<uint64_t> cascade_aborts{0};
    std::atomic<uint64_t> cascade_abort_depth_sum{0};
    std::atomic<uint64_t> dependencies_created{0};
    std::atomic<uint64_t> dependencies_resolved{0};
    std::atomic<uint64_t> locks_saved_time_us{0};
    
    void reset() {
        total_early_releases.store(0);
        successful_commits.store(0);
        cascade_aborts.store(0);
        cascade_abort_depth_sum.store(0);
        dependencies_created.store(0);
        dependencies_resolved.store(0);
        locks_saved_time_us.store(0);
    }
};

/**
 * @brief Global ELR configuration (singleton)
 */
class ELRConfigManager {
public:
    static ELRConfigManager& getInstance() {
        static ELRConfigManager instance;
        return instance;
    }
    
    const ELRConfig& getConfig() const { return config_; }
    ELRConfig& getConfig() { return config_; }
    
    void setEnabled(bool enabled) { config_.enabled = enabled; }
    bool isEnabled() const { return config_.enabled; }
    
    ELRStats& getStats() { return stats_; }
    const ELRStats& getStats() const { return stats_; }
    
private:
    ELRConfigManager() = default;
    ELRConfig config_;
    ELRStats stats_;
};

// Convenience function
inline bool elr_enabled() {
    return ELRConfigManager::getInstance().isEnabled();
}

} // namespace elr
} // namespace mako

#endif // _MAKO_ELR_COMMON_H_
