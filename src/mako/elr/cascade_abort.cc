/**
 * @file cascade_abort.cc
 * @brief Implementation of Cascade Abort Handler
 */

#include "cascade_abort.h"
#include <algorithm>
#include <queue>
#include <chrono>
#include <iostream>
#include <thread>

namespace mako {
namespace elr {

CascadeAbortHandler::CascadeAbortHandler(shardid_t shard_id)
    : shard_id_(shard_id), dependency_tracker_(nullptr), shard_dependency_tracker_(nullptr) {}

CascadeAbortHandler::~CascadeAbortHandler() {}

uint64_t CascadeAbortHandler::getCurrentTimeUs() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

std::vector<std::pair<txnid_t, shardid_t>>
CascadeAbortHandler::buildAbortSet(txnid_t root_txn) {
    std::vector<std::pair<txnid_t, shardid_t>> result;
    
    if (!dependency_tracker_) {
        return result;
    }
    
    // Get all local transactions that need to be aborted
    std::vector<txnid_t> local_cascade_set = dependency_tracker_->getCascadeAbortSet(root_txn, 0);
    
    // Add local transactions
    for (txnid_t txn : local_cascade_set) {
        result.push_back({txn, shard_id_});
    }
    
    // Handle cross-shard dependencies
    // For each local transaction in the cascade set, check if it has
    // remote dependents (remote transactions that read local uncommitted data)
    if (shard_dependency_tracker_) {
        for (txnid_t local_txn : local_cascade_set) {
            // Get remote transactions that depend on this local transaction
            // (they read our uncommitted writes and need to abort if we abort)
            auto remote_deps = shard_dependency_tracker_->getRemoteDependents(local_txn);
            for (const auto& [remote_txn, remote_shard] : remote_deps) {
                // Add remote transaction to abort set (will be handled via RPC)
                result.push_back({remote_txn, remote_shard});
            }
        }
    }
    
    // Sort by shard to group remote calls, then reverse within each group
    // to abort leaves first
    std::stable_sort(result.begin(), result.end(),
        [](const auto& a, const auto& b) {
            return a.second < b.second;  // Group by shard
        });
    
    // Reverse to get leaves first (for correct abort ordering within each shard)
    std::reverse(result.begin(), result.end());
    
    return result;
}

CascadeAbortOperation CascadeAbortHandler::initiateCascadeAbort(
    txnid_t root_txn, uint32_t timeout_ms) {
    
    uint64_t cascade_id = next_cascade_id_.fetch_add(1);
    
    CascadeAbortOperation op(cascade_id, root_txn, shard_id_);
    op.start_time_us = getCurrentTimeUs();
    op.status = CascadeStatus::IN_PROGRESS;
    
    stats_.total_cascades.fetch_add(1);
    
    // Build the abort set
    auto abort_set = buildAbortSet(root_txn);
    
    // Add all transactions to the abort list
    for (const auto& [txn_id, shard_id] : abort_set) {
        op.abort_list.emplace_back(txn_id, shard_id);
    }
    
    // Track this cascade operation
    {
        std::lock_guard<std::mutex> lock(cascades_mutex_);
        active_cascades_[cascade_id] = std::make_unique<CascadeAbortOperation>(op);
    }
    
    // Mark all transactions as being aborted
    {
        std::lock_guard<std::mutex> lock(aborting_mutex_);
        for (const auto& info : op.abort_list) {
            aborting_txns_.insert(info.txn_id);
        }
    }
    
    // Execute aborts in order
    uint64_t deadline_us = getCurrentTimeUs() + (timeout_ms * 1000);
    bool all_success = true;
    uint32_t depth = 0;
    
    for (auto& info : op.abort_list) {
        // Check timeout
        if (getCurrentTimeUs() > deadline_us) {
            op.status = CascadeStatus::TIMEOUT;
            stats_.timeout_cascades.fetch_add(1);
            break;
        }
        
        bool success;
        if (info.shard_id == shard_id_) {
            // Local abort
            success = executeAbort(info.txn_id, info.shard_id);
        } else {
            // Remote abort
            success = sendRemoteAbort(info.txn_id, info.shard_id, cascade_id);
            stats_.remote_aborts_sent.fetch_add(1);
        }
        
        if (success) {
            info.status = CascadeStatus::COMPLETED;
            info.abort_time_us = getCurrentTimeUs();
            stats_.total_aborted_txns.fetch_add(1);
            depth++;
        } else {
            info.status = CascadeStatus::FAILED;
            all_success = false;
        }
    }
    
    // Update cascade status
    op.max_depth = depth;
    op.end_time_us = getCurrentTimeUs();
    
    if (op.status != CascadeStatus::TIMEOUT) {
        if (all_success) {
            op.status = CascadeStatus::COMPLETED;
            stats_.successful_cascades.fetch_add(1);
        } else {
            op.status = CascadeStatus::PARTIAL;
            stats_.failed_cascades.fetch_add(1);
        }
    }
    
    // Remove from aborting set
    {
        std::lock_guard<std::mutex> lock(aborting_mutex_);
        for (const auto& info : op.abort_list) {
            aborting_txns_.erase(info.txn_id);
        }
    }
    
    // Update stored operation
    {
        std::lock_guard<std::mutex> lock(cascades_mutex_);
        if (active_cascades_.count(cascade_id)) {
            *active_cascades_[cascade_id] = op;
        }
    }
    
    return op;
}

bool CascadeAbortHandler::executeAbort(txnid_t txn_id, shardid_t shard_id) {
    // Mark the transaction as aborted in the dependency tracker
    if (dependency_tracker_) {
        dependency_tracker_->markAborted(txn_id);
    }
    
    // Invoke the abort complete callback
    if (abort_complete_callback_) {
        abort_complete_callback_(txn_id, true);
    }
    
    return true;
}

bool CascadeAbortHandler::sendRemoteAbort(txnid_t txn_id, shardid_t target_shard,
                                           uint64_t cascade_id) {
    if (remote_abort_callback_) {
        return remote_abort_callback_(txn_id, target_shard);
    }
    
    // No remote abort callback set, fail
    std::cerr << "[ELR] Cannot send remote abort: no callback set" << std::endl;
    return false;
}

bool CascadeAbortHandler::handleRemoteCascadeAbort(txnid_t txn_id,
                                                    shardid_t source_shard,
                                                    uint64_t cascade_id) {
    stats_.remote_aborts_received.fetch_add(1);
    
    // Check if we're already aborting this transaction
    {
        std::lock_guard<std::mutex> lock(aborting_mutex_);
        if (aborting_txns_.count(txn_id)) {
            // Already being aborted, success
            return true;
        }
        aborting_txns_.insert(txn_id);
    }
    
    // Execute the abort locally
    bool success = executeAbort(txn_id, shard_id_);
    
    // Also cascade to our dependents
    if (success && dependency_tracker_) {
        auto dependents = dependency_tracker_->getDependentTransactions(txn_id);
        for (txnid_t dep : dependents) {
            executeAbort(dep, shard_id_);
        }
    }
    
    // Remove from aborting set
    {
        std::lock_guard<std::mutex> lock(aborting_mutex_);
        aborting_txns_.erase(txn_id);
    }
    
    return success;
}

bool CascadeAbortHandler::isBeingAborted(txnid_t txn_id) const {
    std::lock_guard<std::mutex> lock(aborting_mutex_);
    return aborting_txns_.count(txn_id) > 0;
}

bool CascadeAbortHandler::waitForCascade(uint64_t cascade_id, uint32_t timeout_ms) {
    auto start = std::chrono::steady_clock::now();
    auto timeout = std::chrono::milliseconds(timeout_ms);
    
    while (true) {
        {
            std::lock_guard<std::mutex> lock(cascades_mutex_);
            auto it = active_cascades_.find(cascade_id);
            if (it != active_cascades_.end()) {
                CascadeStatus status = it->second->status;
                if (status == CascadeStatus::COMPLETED ||
                    status == CascadeStatus::PARTIAL ||
                    status == CascadeStatus::FAILED ||
                    status == CascadeStatus::TIMEOUT) {
                    return status == CascadeStatus::COMPLETED;
                }
            }
        }
        
        if (std::chrono::steady_clock::now() - start > timeout) {
            return false;
        }
        
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

CascadeAbortOperation* CascadeAbortHandler::getCascadeOperation(uint64_t cascade_id) {
    std::lock_guard<std::mutex> lock(cascades_mutex_);
    auto it = active_cascades_.find(cascade_id);
    if (it != active_cascades_.end()) {
        return it->second.get();
    }
    return nullptr;
}

} // namespace elr
} // namespace mako
