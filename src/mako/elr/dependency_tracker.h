/**
 * @file dependency_tracker.h
 * @brief Dependency tracking for Early Lock Release (ELR)
 *
 * Tracks dependencies between transactions when early lock release is used.
 * A dependency exists when a transaction reads an uncommitted value from
 * another transaction that has early-released its lock.
 *
 * The dependency graph is used to:
 * 1. Detect cycles (which would cause deadlocks)
 * 2. Propagate aborts through the dependency chain (cascade aborts)
 * 3. Ensure recoverability by preventing commits before dependencies resolve
 */

#ifndef _MAKO_ELR_DEPENDENCY_TRACKER_H_
#define _MAKO_ELR_DEPENDENCY_TRACKER_H_

#include <unordered_map>
#include <unordered_set>
#include <map>
#include <set>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include <queue>
#include <memory>
#include <atomic>

#include "elr_common.h"

namespace mako {
namespace elr {

/**
 * @brief Node in the dependency graph representing a transaction
 */
struct DependencyNode {
    txnid_t txn_id;
    
    // Transactions this transaction depends on (we read their uncommitted writes)
    std::unordered_set<txnid_t> depends_on;
    
    // Transactions that depend on us (they read our uncommitted writes)
    std::unordered_set<txnid_t> depended_by;
    
    // Detailed dependency information per key
    std::vector<ELRDependency> dependencies;
    
    // Transaction status
    enum class Status {
        ACTIVE,
        COMMITTING,
        COMMITTED,
        ABORTING,
        ABORTED
    };
    Status status = Status::ACTIVE;
    
    // Depth in the dependency chain (for limiting cascade)
    uint32_t chain_depth = 0;
    
    DependencyNode(txnid_t id) : txn_id(id) {}
};

/**
 * @brief Tracks dependencies between transactions for ELR
 *
 * Thread-safe implementation using reader-writer locks.
 */
class DependencyTracker {
public:
    DependencyTracker();
    ~DependencyTracker();
    
    /**
     * @brief Register a new transaction
     * @param txn_id Transaction ID
     */
    void registerTransaction(txnid_t txn_id);
    
    /**
     * @brief Unregister a transaction (after commit/abort)
     * @param txn_id Transaction ID
     */
    void unregisterTransaction(txnid_t txn_id);
    
    /**
     * @brief Add a dependency between two transactions
     * @param reader_txn Transaction that read uncommitted data
     * @param writer_txn Transaction that wrote the data
     * @param key The key involved
     * @param version Version of the data read
     * @param type Type of dependency
     * @return true if dependency was added, false if it would create a cycle
     */
    bool addDependency(txnid_t reader_txn, txnid_t writer_txn,
                       const ELRKey& key, tid_t version,
                       DependencyType type = DependencyType::READ_UNCOMMITTED);
    
    /**
     * @brief Remove a dependency (when writer commits)
     * @param reader_txn Transaction that had the dependency
     * @param writer_txn Transaction that committed
     */
    void removeDependency(txnid_t reader_txn, txnid_t writer_txn);
    
    /**
     * @brief Check if adding a dependency would create a cycle
     * @param reader_txn Potential reader
     * @param writer_txn Potential writer
     * @return true if cycle would be created
     */
    bool wouldCreateCycle(txnid_t reader_txn, txnid_t writer_txn);
    
    /**
     * @brief Get all transactions that depend on the given transaction
     * @param txn_id Transaction ID
     * @return Set of dependent transaction IDs
     */
    std::unordered_set<txnid_t> getDependentTransactions(txnid_t txn_id);
    
    /**
     * @brief Get all transactions that the given transaction depends on
     * @param txn_id Transaction ID
     * @return Set of transaction IDs we depend on
     */
    std::unordered_set<txnid_t> getDependencies(txnid_t txn_id);
    
    /**
     * @brief Get the full cascade abort set for a transaction
     *
     * Returns all transactions that would need to abort if the given
     * transaction aborts, including transitive dependencies.
     *
     * @param txn_id Transaction that is aborting
     * @param max_depth Maximum depth to traverse (0 = unlimited)
     * @return Ordered list of transactions to abort (in abort order)
     */
    std::vector<txnid_t> getCascadeAbortSet(txnid_t txn_id, uint32_t max_depth = 0);
    
    /**
     * @brief Check if a transaction can safely commit
     *
     * A transaction can commit if all transactions it depends on have committed.
     *
     * @param txn_id Transaction ID
     * @return true if safe to commit
     */
    bool canCommit(txnid_t txn_id);
    
    /**
     * @brief Mark a transaction as committed
     * @param txn_id Transaction ID
     */
    void markCommitted(txnid_t txn_id);
    
    /**
     * @brief Mark a transaction as aborted
     * @param txn_id Transaction ID
     */
    void markAborted(txnid_t txn_id);
    
    /**
     * @brief Get the chain depth for a transaction
     * @param txn_id Transaction ID
     * @return Chain depth (0 if not found)
     */
    uint32_t getChainDepth(txnid_t txn_id);
    
    /**
     * @brief Check if dependency chain is too deep
     * @param txn_id Transaction ID
     * @param max_depth Maximum allowed depth
     * @return true if chain is too deep
     */
    bool isChainTooDeep(txnid_t txn_id, uint32_t max_depth);
    
    /**
     * @brief Get detailed dependencies for a transaction
     * @param txn_id Transaction ID
     * @return Vector of ELRDependency objects
     */
    std::vector<ELRDependency> getDetailedDependencies(txnid_t txn_id);
    
    /**
     * @brief Get number of active transactions in the tracker
     */
    size_t getActiveTransactionCount() const;
    
    /**
     * @brief Get number of dependencies in the tracker
     */
    size_t getDependencyCount() const;
    
    /**
     * @brief Clear all tracking data
     */
    void clear();
    
    /**
     * @brief Dump dependency graph for debugging
     */
    std::string dumpGraph() const;
    
private:
    // Main storage for dependency nodes
    std::unordered_map<txnid_t, std::unique_ptr<DependencyNode>> nodes_;
    
    // Reader-writer lock for thread safety
    mutable std::shared_mutex mutex_;
    
    // Counter for dependencies
    std::atomic<size_t> dependency_count_{0};
    
    // Helper: Check for cycle using DFS (must hold lock)
    bool hasCycle(txnid_t from, txnid_t to) const;
    
    // Helper: Update chain depths after adding dependency
    void updateChainDepths(txnid_t start_txn);
    
    // Helper: Get or create node
    DependencyNode* getOrCreateNode(txnid_t txn_id);
    
    // Helper: Get node (returns nullptr if not found)
    DependencyNode* getNode(txnid_t txn_id) const;
};

/**
 * @brief Per-shard dependency tracker
 *
 * In a distributed system, each shard maintains its own dependency tracker.
 * Cross-shard dependencies are tracked separately.
 */
class ShardDependencyTracker {
public:
    ShardDependencyTracker(shardid_t shard_id);
    
    shardid_t getShardId() const { return shard_id_; }
    
    // Local dependency tracking (delegates to DependencyTracker)
    DependencyTracker& getLocalTracker() { return local_tracker_; }
    
    /**
     * @brief Register a cross-shard dependency
     */
    void addCrossShardDependency(txnid_t local_txn, txnid_t remote_txn,
                                  shardid_t remote_shard);
    
    /**
     * @brief Get cross-shard dependencies for a transaction (what it depends on)
     */
    std::vector<std::pair<txnid_t, shardid_t>> getCrossShardDependencies(txnid_t txn_id);
    
    /**
     * @brief Get remote transactions that depend on a local transaction
     * Used for cascade aborts - when local_txn aborts, these remote transactions
     * must also be notified to abort.
     */
    std::vector<std::pair<txnid_t, shardid_t>> getRemoteDependents(txnid_t local_txn);
    
    /**
     * @brief Register that a remote transaction depends on a local transaction
     */
    void addRemoteDependent(txnid_t local_txn, txnid_t remote_txn, shardid_t remote_shard);
    
    /**
     * @brief Notify that a remote transaction committed
     */
    void notifyRemoteCommit(txnid_t remote_txn, shardid_t remote_shard);
    
    /**
     * @brief Notify that a remote transaction aborted
     */
    void notifyRemoteAbort(txnid_t remote_txn, shardid_t remote_shard);
    
private:
    shardid_t shard_id_;
    DependencyTracker local_tracker_;
    
    // Cross-shard dependencies: local_txn -> [(remote_txn, remote_shard)]
    // These are transactions that local_txn depends on (the local reads uncommitted remote data)
    std::unordered_map<txnid_t, std::vector<std::pair<txnid_t, shardid_t>>> cross_shard_deps_;
    
    // Remote dependents: local_txn -> [(remote_txn, remote_shard)]
    // These are remote transactions that depend on local_txn (remote reads uncommitted local data)
    // Used for cascade aborts
    std::unordered_map<txnid_t, std::vector<std::pair<txnid_t, shardid_t>>> remote_dependents_;
    
    std::mutex cross_shard_mutex_;
};

} // namespace elr
} // namespace mako

#endif // _MAKO_ELR_DEPENDENCY_TRACKER_H_
