/**
 * @file elr_manager.cc
 * @brief Implementation of the ELR Manager
 */

#include "elr_manager.h"
#include <chrono>
#include <sstream>
#include <algorithm>
#include <iostream>

namespace mako {
namespace elr {

// Static member initialization
std::unordered_map<shardid_t, std::unique_ptr<ELRManager>> ELRManager::instances_;
std::mutex ELRManager::instances_mutex_;

// ============================================================================
// Singleton Management
// ============================================================================

ELRManager& ELRManager::getInstance(shardid_t shard_id) {
    std::lock_guard<std::mutex> lock(instances_mutex_);
    
    auto it = instances_.find(shard_id);
    if (it == instances_.end()) {
        // Create new instance
        instances_[shard_id] = std::unique_ptr<ELRManager>(new ELRManager());
        return *instances_[shard_id];
    }
    return *it->second;
}

ELRManager::ELRManager()
    : shard_id_(0),
      initialized_(false),
      running_(false) {
    dependency_tracker_ = std::make_unique<DependencyTracker>();
}

ELRManager::~ELRManager() {
    shutdown();
}

void ELRManager::initialize(shardid_t shard_id, const ELRConfig& config) {
    std::unique_lock<std::shared_mutex> lock(txn_mutex_);
    
    if (initialized_) {
        return; // Already initialized
    }
    
    shard_id_ = shard_id;
    config_ = config;
    
    shard_dependency_tracker_ = std::make_unique<ShardDependencyTracker>(shard_id);
    
    // Start cleanup thread if ELR is enabled
    if (config_.enabled) {
        running_ = true;
        cleanup_thread_ = std::thread(&ELRManager::runCleanupLoop, this);
    }
    
    initialized_ = true;
    
    std::cerr << "[ELR] Manager initialized for shard " << shard_id
              << " (enabled=" << config_.enabled << ")" << std::endl;
}

void ELRManager::shutdown() {
    running_ = false;
    
    {
        std::lock_guard<std::mutex> lock(cleanup_mutex_);
        cleanup_cv_.notify_all();
    }
    
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    
    std::unique_lock<std::shared_mutex> lock(txn_mutex_);
    active_txns_.clear();
    
    std::unique_lock<std::shared_mutex> lock2(lock_mutex_);
    elr_locks_.clear();
    
    dependency_tracker_->clear();
    initialized_ = false;
}

// ============================================================================
// Transaction Lifecycle
// ============================================================================

void ELRManager::beginTransaction(txnid_t txn_id) {
    if (!config_.enabled) return;
    
    std::unique_lock<std::shared_mutex> lock(txn_mutex_);
    
    if (active_txns_.find(txn_id) != active_txns_.end()) {
        return; // Already registered
    }
    
    auto info = std::make_unique<ELRTransactionInfo>(txn_id, shard_id_);
    info->start_time_us = getCurrentTimeUs();
    active_txns_[txn_id] = std::move(info);
    
    dependency_tracker_->registerTransaction(txn_id);
}

void ELRManager::endTransaction(txnid_t txn_id, bool committed) {
    if (!config_.enabled) return;
    
    if (committed) {
        commitTransaction(txn_id);
    } else {
        abortTransaction(txn_id);
    }
    
    std::unique_lock<std::shared_mutex> lock(txn_mutex_);
    active_txns_.erase(txn_id);
    
    dependency_tracker_->unregisterTransaction(txn_id);
}

ELRTransactionInfo* ELRManager::getTransactionInfo(txnid_t txn_id) {
    auto it = active_txns_.find(txn_id);
    if (it == active_txns_.end()) return nullptr;
    return it->second.get();
}

// ============================================================================
// Early Lock Release Operations
// ============================================================================

bool ELRManager::canEarlyRelease(txnid_t txn_id) {
    if (!config_.enabled) return false;
    
    std::shared_lock<std::shared_mutex> lock(txn_mutex_);
    
    ELRTransactionInfo* info = getTransactionInfo(txn_id);
    if (!info) return false;
    
    // Check if transaction is in a valid state for early release
    if (info->status != ELRStatus::NONE && info->status != ELRStatus::PENDING) {
        return false;
    }
    
    // Check dependency chain depth
    uint32_t chain_depth = dependency_tracker_->getChainDepth(txn_id);
    if (chain_depth >= config_.max_dependency_chain) {
        return false;
    }
    
    // Check if minimum hold time has passed
    if (config_.min_hold_time_us > 0) {
        uint64_t hold_time = getCurrentTimeUs() - info->start_time_us;
        if (hold_time < config_.min_hold_time_us) {
            return false;
        }
    }
    
    return true;
}

EarlyReleaseResult ELRManager::earlyRelease(txnid_t txn_id,
                                              const std::vector<ELRKey>& keys) {
    if (!config_.enabled) {
        return EarlyReleaseResult::Failure("ELR not enabled");
    }
    
    if (!canEarlyRelease(txn_id)) {
        return EarlyReleaseResult::Failure("Early release not allowed for this transaction");
    }
    
    std::vector<ELRKey> released_keys;
    
    {
        std::unique_lock<std::shared_mutex> lock(txn_mutex_);
        
        ELRTransactionInfo* info = getTransactionInfo(txn_id);
        if (!info) {
            return EarlyReleaseResult::Failure("Transaction not found");
        }
        
        // Mark transaction as having early-released
        info->status = ELRStatus::RELEASED;
        info->early_release_time_us = getCurrentTimeUs();
        
        // Record which keys are early-released
        for (const auto& key : keys) {
            info->early_released_keys.push_back(key);
            released_keys.push_back(key);
        }
    }
    
    {
        std::unique_lock<std::shared_mutex> lock(lock_mutex_);
        
        // Register the early-released locks
        for (const auto& key : keys) {
            ELRLockInfo lock_info(txn_id, 0); // Version will be set later
            lock_info.status = ELRStatus::RELEASED;
            lock_info.release_time_us = getCurrentTimeUs();
            elr_locks_[key] = lock_info;
        }
    }
    
    // Log the operation for recovery
    if (config_.enable_logging) {
        logELROperation(txn_id, "EARLY_RELEASE", keys);
    }
    
    // Update stats
    getStats().total_early_releases.fetch_add(1);
    
    return EarlyReleaseResult::Success(released_keys);
}

bool ELRManager::registerELRRead(txnid_t reader_txn, txnid_t writer_txn,
                                  const ELRKey& key, tid_t version) {
    if (!config_.enabled) return false;
    
    // Add dependency: reader depends on writer
    bool added = dependency_tracker_->addDependency(
        reader_txn, writer_txn, key, version, DependencyType::READ_UNCOMMITTED);
    
    if (added) {
        // Update the lock info with the new dependent
        std::unique_lock<std::shared_mutex> lock(lock_mutex_);
        auto it = elr_locks_.find(key);
        if (it != elr_locks_.end()) {
            it->second.dependent_txns.push_back(reader_txn);
        }
    }
    
    return added;
}

ELRLockInfo ELRManager::getELRLockInfo(const ELRKey& key) {
    std::shared_lock<std::shared_mutex> lock(lock_mutex_);
    auto it = elr_locks_.find(key);
    if (it == elr_locks_.end()) {
        return ELRLockInfo();
    }
    return it->second;
}

bool ELRManager::isEarlyReleased(const ELRKey& key) {
    std::shared_lock<std::shared_mutex> lock(lock_mutex_);
    auto it = elr_locks_.find(key);
    if (it == elr_locks_.end()) return false;
    return it->second.status == ELRStatus::RELEASED;
}

txnid_t ELRManager::getELROwner(const ELRKey& key) {
    std::shared_lock<std::shared_mutex> lock(lock_mutex_);
    auto it = elr_locks_.find(key);
    if (it == elr_locks_.end()) return 0;
    return it->second.owner_txn;
}

// ============================================================================
// Commit Operations
// ============================================================================

bool ELRManager::canCommit(txnid_t txn_id) {
    if (!config_.enabled) return true;
    return dependency_tracker_->canCommit(txn_id);
}

bool ELRManager::waitForDependencies(txnid_t txn_id, uint32_t timeout_ms) {
    if (!config_.enabled) return true;
    
    auto start = std::chrono::steady_clock::now();
    auto timeout = std::chrono::milliseconds(timeout_ms);
    
    while (!dependency_tracker_->canCommit(txn_id)) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > timeout) {
            return false; // Timeout
        }
        
        // Brief sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    return true;
}

void ELRManager::commitTransaction(txnid_t txn_id) {
    if (!config_.enabled) return;
    
    std::vector<ELRKey> keys_to_clear;
    
    {
        std::unique_lock<std::shared_mutex> lock(txn_mutex_);
        
        ELRTransactionInfo* info = getTransactionInfo(txn_id);
        if (info) {
            info->status = ELRStatus::COMMITTED;
            info->commit_time_us = getCurrentTimeUs();
            keys_to_clear = info->early_released_keys;
            
            // Calculate time saved
            if (info->early_release_time_us > 0) {
                uint64_t saved = info->commit_time_us - info->early_release_time_us;
                getStats().locks_saved_time_us.fetch_add(saved);
            }
        }
    }
    
    {
        std::unique_lock<std::shared_mutex> lock(lock_mutex_);
        
        // Clear early-released locks for this transaction
        for (const auto& key : keys_to_clear) {
            auto it = elr_locks_.find(key);
            if (it != elr_locks_.end() && it->second.owner_txn == txn_id) {
                it->second.status = ELRStatus::COMMITTED;
                elr_locks_.erase(it);
            }
        }
    }
    
    // Mark committed in dependency tracker
    dependency_tracker_->markCommitted(txn_id);
    
    // Invoke callback if set
    if (commit_callback_) {
        commit_callback_(txn_id, true);
    }
    
    // Log for recovery
    if (config_.enable_logging) {
        logELROperation(txn_id, "COMMIT", keys_to_clear);
    }
}

// ============================================================================
// Abort Operations
// ============================================================================

std::vector<txnid_t> ELRManager::getCascadeAbortSet(txnid_t txn_id) {
    return dependency_tracker_->getCascadeAbortSet(txn_id, config_.max_dependency_chain);
}

CascadeAbortResult ELRManager::abortTransaction(txnid_t txn_id) {
    CascadeAbortResult result;
    
    if (!config_.enabled) {
        result.success = true;
        return result;
    }
    
    std::vector<ELRKey> keys_to_clear;
    
    {
        std::unique_lock<std::shared_mutex> lock(txn_mutex_);
        
        ELRTransactionInfo* info = getTransactionInfo(txn_id);
        if (info) {
            info->status = ELRStatus::ABORTED;
            keys_to_clear = info->early_released_keys;
        }
    }
    
    // Get cascade abort set BEFORE clearing locks
    std::vector<txnid_t> cascade_set = getCascadeAbortSet(txn_id);
    
    {
        std::unique_lock<std::shared_mutex> lock(lock_mutex_);
        
        // Clear early-released locks for this transaction
        for (const auto& key : keys_to_clear) {
            auto it = elr_locks_.find(key);
            if (it != elr_locks_.end() && it->second.owner_txn == txn_id) {
                it->second.status = ELRStatus::ABORTED;
                elr_locks_.erase(it);
            }
        }
    }
    
    // Mark aborted in dependency tracker
    dependency_tracker_->markAborted(txn_id);
    
    // Cascade abort to dependents
    result.aborted_txns.push_back(txn_id);
    result.cascade_depth = 0;
    
    for (txnid_t dependent_txn : cascade_set) {
        // Recursively abort dependents
        CascadeAbortResult sub_result = abortTransaction(dependent_txn);
        
        // Merge results
        result.aborted_txns.insert(result.aborted_txns.end(),
                                   sub_result.aborted_txns.begin(),
                                   sub_result.aborted_txns.end());
        result.cascade_depth = std::max(result.cascade_depth, sub_result.cascade_depth + 1);
    }
    
    // Update stats
    if (!cascade_set.empty()) {
        getStats().cascade_aborts.fetch_add(1);
        getStats().cascade_abort_depth_sum.fetch_add(result.cascade_depth);
    }
    
    // Invoke callback if set
    if (abort_callback_ && !cascade_set.empty()) {
        abort_callback_(txn_id, cascade_set);
    }
    
    // Log for recovery
    if (config_.enable_logging) {
        logELROperation(txn_id, "ABORT", keys_to_clear);
    }
    
    result.success = true;
    return result;
}

void ELRManager::handleRemoteCascadeAbort(txnid_t txn_id, shardid_t source_shard) {
    // Handle cascade abort notification from another shard
    std::cerr << "[ELR] Received cascade abort for txn " << txn_id
              << " from shard " << source_shard << std::endl;
    
    if (shard_dependency_tracker_) {
        shard_dependency_tracker_->notifyRemoteAbort(txn_id, source_shard);
    }
    
    // Check if we have any local transactions that depend on this
    auto dependents = dependency_tracker_->getDependentTransactions(txn_id);
    for (txnid_t local_txn : dependents) {
        abortTransaction(local_txn);
    }
}

// ============================================================================
// Recovery Operations
// ============================================================================

void ELRManager::logELROperation(txnid_t txn_id, const std::string& operation,
                                  const std::vector<ELRKey>& keys) {
    // TODO: Integrate with RocksDB persistence or Paxos log
    // For now, just log to stderr in debug mode
#ifdef DEBUG
    std::cerr << "[ELR LOG] txn=" << txn_id << " op=" << operation
              << " keys=" << keys.size() << std::endl;
#endif
}

void ELRManager::recoverFromLog() {
    // TODO: Implement recovery from RocksDB persistence
    std::cerr << "[ELR] Recovery not yet implemented" << std::endl;
}

// ============================================================================
// Helper Methods
// ============================================================================

uint64_t ELRManager::getCurrentTimeUs() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

void ELRManager::cleanupExpiredLocks() {
    std::unique_lock<std::shared_mutex> lock(lock_mutex_);
    
    uint64_t now = getCurrentTimeUs();
    uint64_t timeout = config_.cascade_timeout_ms * 1000; // Convert to microseconds
    
    auto it = elr_locks_.begin();
    while (it != elr_locks_.end()) {
        if (it->second.status == ELRStatus::COMMITTED ||
            it->second.status == ELRStatus::ABORTED) {
            it = elr_locks_.erase(it);
        } else if (now - it->second.release_time_us > timeout) {
            // Lock has been held too long, force abort
            txnid_t owner = it->second.owner_txn;
            it = elr_locks_.erase(it);
            
            // Trigger abort (must release lock first to avoid deadlock)
            lock.unlock();
            abortTransaction(owner);
            lock.lock();
            
            // Restart iteration since we released the lock
            it = elr_locks_.begin();
        } else {
            ++it;
        }
    }
}

void ELRManager::runCleanupLoop() {
    while (running_) {
        {
            std::unique_lock<std::mutex> lock(cleanup_mutex_);
            cleanup_cv_.wait_for(lock, std::chrono::seconds(1),
                                [this] { return !running_.load(); });
        }
        
        if (running_) {
            cleanupExpiredLocks();
        }
    }
}

std::string ELRManager::getStatsSummary() const {
    std::ostringstream oss;
    const auto& stats = ELRConfigManager::getInstance().getStats();
    
    oss << "ELR Statistics (shard " << shard_id_ << "):\n"
        << "  Total early releases: " << stats.total_early_releases.load() << "\n"
        << "  Successful commits: " << stats.successful_commits.load() << "\n"
        << "  Cascade aborts: " << stats.cascade_aborts.load() << "\n"
        << "  Dependencies created: " << stats.dependencies_created.load() << "\n"
        << "  Dependencies resolved: " << stats.dependencies_resolved.load() << "\n"
        << "  Lock time saved (us): " << stats.locks_saved_time_us.load() << "\n";
    
    if (stats.cascade_aborts.load() > 0) {
        double avg_depth = static_cast<double>(stats.cascade_abort_depth_sum.load()) /
                           stats.cascade_aborts.load();
        oss << "  Avg cascade depth: " << avg_depth << "\n";
    }
    
    return oss.str();
}

} // namespace elr
} // namespace mako
