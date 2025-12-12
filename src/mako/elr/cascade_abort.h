/**
 * @file cascade_abort.h
 * @brief Cascade Abort Handler for Early Lock Release
 *
 * When a transaction that has early-released locks aborts, all transactions
 * that read those uncommitted values must also abort (cascade abort).
 * This handler coordinates the cascade abort process, including:
 * - Determining the abort set
 * - Ordering aborts correctly
 * - Coordinating across shards
 * - Handling partial failures
 */

#ifndef _MAKO_ELR_CASCADE_ABORT_H_
#define _MAKO_ELR_CASCADE_ABORT_H_

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <memory>
#include <functional>
#include <atomic>
#include <future>
#include <chrono>

#include "elr_common.h"
#include "dependency_tracker.h"

namespace mako {
namespace elr {

/**
 * @brief Status of a cascade abort operation
 */
enum class CascadeStatus {
    PENDING,      // Not yet started
    IN_PROGRESS,  // Abort in progress
    COMPLETED,    // Successfully completed
    PARTIAL,      // Some aborts failed
    TIMEOUT,      // Timed out
    FAILED        // Failed completely
};

/**
 * @brief Information about a single abort in the cascade
 */
struct AbortInfo {
    txnid_t txn_id;
    shardid_t shard_id;
    CascadeStatus status;
    std::string error_message;
    uint64_t abort_time_us;
    
    AbortInfo(txnid_t txn, shardid_t shard)
        : txn_id(txn), shard_id(shard), status(CascadeStatus::PENDING),
          abort_time_us(0) {}
};

/**
 * @brief A cascade abort operation
 *
 * Represents a complete cascade abort starting from a root transaction.
 */
struct CascadeAbortOperation {
    // Unique ID for this cascade operation
    uint64_t cascade_id;
    
    // Root transaction that triggered the cascade
    txnid_t root_txn_id;
    shardid_t root_shard_id;
    
    // All transactions in the cascade
    std::vector<AbortInfo> abort_list;
    
    // Overall status
    CascadeStatus status;
    
    // Timing
    uint64_t start_time_us;
    uint64_t end_time_us;
    
    // Depth of the cascade (longest path from root)
    uint32_t max_depth;
    
    CascadeAbortOperation(uint64_t id, txnid_t root, shardid_t shard)
        : cascade_id(id), root_txn_id(root), root_shard_id(shard),
          status(CascadeStatus::PENDING), start_time_us(0), end_time_us(0),
          max_depth(0) {}
};

/**
 * @brief Callback for remote abort requests
 */
using RemoteAbortCallback = std::function<bool(txnid_t txn_id, shardid_t target_shard)>;

/**
 * @brief Callback for abort completion notification
 */
using AbortCompleteCallback = std::function<void(txnid_t txn_id, bool success)>;

/**
 * @brief Handles cascade abort operations for ELR
 */
class CascadeAbortHandler {
public:
    CascadeAbortHandler(shardid_t shard_id);
    ~CascadeAbortHandler();
    
    /**
     * @brief Set the dependency tracker to use
     */
    void setDependencyTracker(DependencyTracker* tracker) {
        dependency_tracker_ = tracker;
    }
    
    /**
     * @brief Set callback for remote abort requests
     */
    void setRemoteAbortCallback(RemoteAbortCallback callback) {
        remote_abort_callback_ = callback;
    }
    
    /**
     * @brief Set callback for abort completion
     */
    void setAbortCompleteCallback(AbortCompleteCallback callback) {
        abort_complete_callback_ = callback;
    }
    
    /**
     * @brief Initiate a cascade abort operation
     *
     * This computes the full cascade set and aborts all transactions
     * in the correct order (leaves first, root last for local transactions).
     *
     * @param root_txn Root transaction that is aborting
     * @param timeout_ms Timeout for the entire operation
     * @return Cascade abort operation result
     */
    CascadeAbortOperation initiateCascadeAbort(txnid_t root_txn,
                                                 uint32_t timeout_ms = 1000);
    
    /**
     * @brief Handle a cascade abort request from another shard
     *
     * @param txn_id Transaction to abort
     * @param source_shard Shard that initiated the cascade
     * @param cascade_id ID of the cascade operation
     * @return true if abort was successful
     */
    bool handleRemoteCascadeAbort(txnid_t txn_id, shardid_t source_shard,
                                   uint64_t cascade_id);
    
    /**
     * @brief Check if a transaction is being cascade-aborted
     */
    bool isBeingAborted(txnid_t txn_id) const;
    
    /**
     * @brief Wait for a cascade abort to complete
     */
    bool waitForCascade(uint64_t cascade_id, uint32_t timeout_ms);
    
    /**
     * @brief Get the status of a cascade operation
     */
    CascadeAbortOperation* getCascadeOperation(uint64_t cascade_id);
    
    /**
     * @brief Get statistics about cascade aborts
     */
    struct Stats {
        std::atomic<uint64_t> total_cascades{0};
        std::atomic<uint64_t> successful_cascades{0};
        std::atomic<uint64_t> failed_cascades{0};
        std::atomic<uint64_t> timeout_cascades{0};
        std::atomic<uint64_t> total_aborted_txns{0};
        std::atomic<uint64_t> remote_aborts_sent{0};
        std::atomic<uint64_t> remote_aborts_received{0};
    };
    
    const Stats& getStats() const { return stats_; }
    
private:
    shardid_t shard_id_;
    DependencyTracker* dependency_tracker_;
    
    // Active cascade operations
    std::unordered_map<uint64_t, std::unique_ptr<CascadeAbortOperation>> active_cascades_;
    mutable std::mutex cascades_mutex_;
    
    // Transactions currently being aborted
    std::unordered_set<txnid_t> aborting_txns_;
    mutable std::mutex aborting_mutex_;
    
    // Next cascade ID
    std::atomic<uint64_t> next_cascade_id_{1};
    
    // Callbacks
    RemoteAbortCallback remote_abort_callback_;
    AbortCompleteCallback abort_complete_callback_;
    
    // Statistics
    Stats stats_;
    
    // Helper methods
    
    /**
     * @brief Build the abort set in topological order
     *
     * Returns transactions in reverse topological order (leaves first)
     * so that dependent transactions are aborted before their dependencies.
     */
    std::vector<std::pair<txnid_t, shardid_t>> buildAbortSet(txnid_t root_txn);
    
    /**
     * @brief Execute a single abort
     */
    bool executeAbort(txnid_t txn_id, shardid_t shard_id);
    
    /**
     * @brief Send abort request to remote shard
     */
    bool sendRemoteAbort(txnid_t txn_id, shardid_t target_shard,
                         uint64_t cascade_id);
    
    /**
     * @brief Get current time in microseconds
     */
    uint64_t getCurrentTimeUs() const;
};

/**
 * @brief RAII guard to track cascade abort operations
 */
class CascadeAbortGuard {
public:
    CascadeAbortGuard(CascadeAbortHandler& handler, txnid_t txn_id)
        : handler_(handler), txn_id_(txn_id), released_(false) {}
    
    ~CascadeAbortGuard() {
        if (!released_) {
            // Abort was not completed, mark as failed
        }
    }
    
    void release() { released_ = true; }
    
private:
    CascadeAbortHandler& handler_;
    txnid_t txn_id_;
    bool released_;
};

} // namespace elr
} // namespace mako

#endif // _MAKO_ELR_CASCADE_ABORT_H_
