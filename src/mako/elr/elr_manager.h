/**
 * @file elr_manager.h
 * @brief Core Early Lock Release (ELR) Manager
 *
 * The ELR Manager coordinates early lock release operations across the system.
 * It manages:
 * - Tracking of early-released locks
 * - Dependency tracking between transactions
 * - Cascade abort coordination
 * - Recovery logging integration
 */

#ifndef _MAKO_ELR_MANAGER_H_
#define _MAKO_ELR_MANAGER_H_

#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <functional>
#include <thread>
#include <condition_variable>

#include "elr_common.h"
#include "dependency_tracker.h"

namespace mako {
namespace elr {

// Forward declarations
class ELRLog;
class CascadeAbortHandler;

/**
 * @brief Information about an active ELR transaction
 */
struct ELRTransactionInfo {
    txnid_t txn_id;
    shardid_t home_shard;
    
    // Keys this transaction has early-released
    std::vector<ELRKey> early_released_keys;
    
    // Current status
    ELRStatus status;
    
    // Timestamps
    uint64_t start_time_us;
    uint64_t early_release_time_us;
    uint64_t commit_time_us;
    
    // Whether we can still abort (before validation)
    bool abortable;
    
    ELRTransactionInfo(txnid_t id, shardid_t shard)
        : txn_id(id), home_shard(shard), status(ELRStatus::NONE),
          start_time_us(0), early_release_time_us(0), commit_time_us(0),
          abortable(true) {}
};

/**
 * @brief Result of an early release operation
 */
struct EarlyReleaseResult {
    bool success;
    std::string error_message;
    std::vector<ELRKey> released_keys;
    
    EarlyReleaseResult() : success(false) {}
    static EarlyReleaseResult Success(const std::vector<ELRKey>& keys) {
        EarlyReleaseResult r;
        r.success = true;
        r.released_keys = keys;
        return r;
    }
    static EarlyReleaseResult Failure(const std::string& msg) {
        EarlyReleaseResult r;
        r.success = false;
        r.error_message = msg;
        return r;
    }
};

/**
 * @brief Result of a cascade abort operation
 */
struct CascadeAbortResult {
    bool success;
    std::vector<txnid_t> aborted_txns;
    uint32_t cascade_depth;
    std::string error_message;
    
    CascadeAbortResult() : success(false), cascade_depth(0) {}
};

/**
 * @brief Core ELR Manager - coordinates all ELR operations
 */
class ELRManager {
public:
    /**
     * @brief Get singleton instance for a shard
     */
    static ELRManager& getInstance(shardid_t shard_id = 0);
    
    /**
     * @brief Initialize the ELR Manager
     * @param shard_id ID of this shard
     * @param config ELR configuration
     */
    void initialize(shardid_t shard_id, const ELRConfig& config);
    
    /**
     * @brief Shutdown the ELR Manager
     */
    void shutdown();
    
    // ========================================================================
    // Transaction Lifecycle
    // ========================================================================
    
    /**
     * @brief Register a new transaction for ELR tracking
     */
    void beginTransaction(txnid_t txn_id);
    
    /**
     * @brief Record that a transaction has completed (commit/abort)
     */
    void endTransaction(txnid_t txn_id, bool committed);
    
    // ========================================================================
    // Early Lock Release Operations
    // ========================================================================
    
    /**
     * @brief Check if early release is safe for a transaction
     *
     * Criteria for safe early release:
     * 1. ELR is enabled
     * 2. Transaction has passed validation
     * 3. Dependency chain is not too deep
     * 4. Not too many uncommitted dependencies
     *
     * @param txn_id Transaction ID
     * @return true if early release is safe
     */
    bool canEarlyRelease(txnid_t txn_id);
    
    /**
     * @brief Early release locks held by a transaction
     *
     * This marks the locks as early-released but keeps track of the
     * owning transaction for dependency tracking and potential cascade abort.
     *
     * @param txn_id Transaction ID
     * @param keys Keys to early release
     * @return Result of the early release operation
     */
    EarlyReleaseResult earlyRelease(txnid_t txn_id, const std::vector<ELRKey>& keys);
    
    /**
     * @brief Register that a transaction read from an early-released lock
     *
     * This creates a dependency between the reader and the writer.
     *
     * @param reader_txn Transaction that is reading
     * @param writer_txn Transaction that early-released
     * @param key Key being read
     * @param version Version of the data
     * @return true if dependency was registered successfully
     */
    bool registerELRRead(txnid_t reader_txn, txnid_t writer_txn,
                         const ELRKey& key, tid_t version);
    
    /**
     * @brief Get information about early-released locks for a key
     */
    ELRLockInfo getELRLockInfo(const ELRKey& key);
    
    /**
     * @brief Check if a key has been early-released by an uncommitted transaction
     */
    bool isEarlyReleased(const ELRKey& key);
    
    /**
     * @brief Get the transaction that early-released a key (if any)
     */
    txnid_t getELROwner(const ELRKey& key);
    
    // ========================================================================
    // Commit Operations
    // ========================================================================
    
    /**
     * @brief Check if a transaction can commit
     *
     * A transaction can commit if all its dependencies have committed.
     *
     * @param txn_id Transaction ID
     * @return true if the transaction can commit
     */
    bool canCommit(txnid_t txn_id);
    
    /**
     * @brief Wait for dependencies to commit
     *
     * Blocks until all dependencies have committed or timeout.
     *
     * @param txn_id Transaction ID
     * @param timeout_ms Timeout in milliseconds
     * @return true if all dependencies committed, false on timeout
     */
    bool waitForDependencies(txnid_t txn_id, uint32_t timeout_ms);
    
    /**
     * @brief Mark a transaction as committed
     *
     * This clears the early-released locks and notifies dependents.
     *
     * @param txn_id Transaction ID
     */
    void commitTransaction(txnid_t txn_id);
    
    // ========================================================================
    // Abort Operations
    // ========================================================================
    
    /**
     * @brief Abort a transaction and cascade to dependents
     *
     * When a transaction that early-released aborts, all transactions
     * that read its uncommitted values must also abort.
     *
     * @param txn_id Transaction ID
     * @return Result of the cascade abort
     */
    CascadeAbortResult abortTransaction(txnid_t txn_id);
    
    /**
     * @brief Get the cascade abort set for a transaction
     *
     * Returns all transactions that would need to abort if this
     * transaction aborts.
     *
     * @param txn_id Transaction ID
     * @return List of transaction IDs that would be aborted
     */
    std::vector<txnid_t> getCascadeAbortSet(txnid_t txn_id);
    
    /**
     * @brief Handle cascade abort notification from another shard
     */
    void handleRemoteCascadeAbort(txnid_t txn_id, shardid_t source_shard);
    
    // ========================================================================
    // Recovery Operations
    // ========================================================================
    
    /**
     * @brief Log an ELR operation for recovery
     */
    void logELROperation(txnid_t txn_id, const std::string& operation,
                         const std::vector<ELRKey>& keys);
    
    /**
     * @brief Recover ELR state from log
     */
    void recoverFromLog();
    
    // ========================================================================
    // Accessors
    // ========================================================================
    
    shardid_t getShardId() const { return shard_id_; }
    const ELRConfig& getConfig() const { return config_; }
    ELRStats& getStats() { return ELRConfigManager::getInstance().getStats(); }
    DependencyTracker& getDependencyTracker() { return *dependency_tracker_; }
    
    /**
     * @brief Get summary statistics
     */
    std::string getStatsSummary() const;
    
    // ========================================================================
    // Callbacks
    // ========================================================================
    
    /**
     * @brief Set callback for when cascade abort is triggered
     */
    void setAbortCallback(ELRAbortCallback callback) {
        abort_callback_ = callback;
    }
    
    /**
     * @brief Set callback for commit notifications
     */
    void setCommitCallback(ELRCommitCallback callback) {
        commit_callback_ = callback;
    }
    
private:
    ELRManager();
    ~ELRManager();
    
    // Prevent copying
    ELRManager(const ELRManager&) = delete;
    ELRManager& operator=(const ELRManager&) = delete;
    
    // Configuration
    shardid_t shard_id_;
    ELRConfig config_;
    bool initialized_;
    
    // Dependency tracking
    std::unique_ptr<DependencyTracker> dependency_tracker_;
    std::unique_ptr<ShardDependencyTracker> shard_dependency_tracker_;
    
    // Active transactions with ELR
    std::unordered_map<txnid_t, std::unique_ptr<ELRTransactionInfo>> active_txns_;
    mutable std::shared_mutex txn_mutex_;
    
    // Early-released locks: key -> lock info
    std::unordered_map<ELRKey, ELRLockInfo, ELRKeyHash> elr_locks_;
    mutable std::shared_mutex lock_mutex_;
    
    // Callbacks
    ELRAbortCallback abort_callback_;
    ELRCommitCallback commit_callback_;
    
    // Background thread for cleanup
    std::thread cleanup_thread_;
    std::atomic<bool> running_;
    std::condition_variable cleanup_cv_;
    std::mutex cleanup_mutex_;
    
    // Helper methods
    ELRTransactionInfo* getTransactionInfo(txnid_t txn_id);
    void cleanupExpiredLocks();
    void runCleanupLoop();
    uint64_t getCurrentTimeUs() const;
    
    // Singleton instances per shard
    static std::unordered_map<shardid_t, std::unique_ptr<ELRManager>> instances_;
    static std::mutex instances_mutex_;
};

/**
 * @brief RAII guard for ELR transactions
 *
 * Automatically handles registration and cleanup of ELR tracking.
 */
class ELRTransactionGuard {
public:
    ELRTransactionGuard(txnid_t txn_id, shardid_t shard_id = 0)
        : txn_id_(txn_id), shard_id_(shard_id), committed_(false) {
        ELRManager::getInstance(shard_id_).beginTransaction(txn_id_);
    }
    
    ~ELRTransactionGuard() {
        ELRManager::getInstance(shard_id_).endTransaction(txn_id_, committed_);
    }
    
    void markCommitted() { committed_ = true; }
    
    // Prevent copying
    ELRTransactionGuard(const ELRTransactionGuard&) = delete;
    ELRTransactionGuard& operator=(const ELRTransactionGuard&) = delete;
    
private:
    txnid_t txn_id_;
    shardid_t shard_id_;
    bool committed_;
};

} // namespace elr
} // namespace mako

#endif // _MAKO_ELR_MANAGER_H_
