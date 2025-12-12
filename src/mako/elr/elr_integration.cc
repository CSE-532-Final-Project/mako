/**
 * @file elr_integration.cc
 * @brief Implementation of ELR integration with Mako
 */

#include "elr_integration.h"
#include "elr_manager.h"
#include "dependency_tracker.h"
#include "cascade_abort.h"
#include "elr_log.h"

#include <iostream>
#include <sstream>

namespace mako {
namespace elr {

// ============================================================================
// Singleton Implementation
// ============================================================================

ELRIntegration& ELRIntegration::getInstance() {
    static ELRIntegration instance;
    return instance;
}

ELRIntegration::ELRIntegration() {}

ELRIntegration::~ELRIntegration() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    for (auto& [shard_id, state] : shard_states_) {
        if (state.initialized) {
            ELRManager::getInstance(shard_id).shutdown();
        }
    }
}

// ============================================================================
// Initialization
// ============================================================================

bool ELRIntegration::initialize(shardid_t shard_id, const ELRIntegrationConfig& config) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    
    if (shard_states_.count(shard_id) && shard_states_[shard_id].initialized) {
        std::cerr << "[ELR] Shard " << shard_id << " already initialized" << std::endl;
        return false;
    }
    
    ShardState& state = shard_states_[shard_id];
    state.config = config;
    
    if (!config.enable_elr) {
        std::cerr << "[ELR] ELR disabled for shard " << shard_id << std::endl;
        state.initialized = true;
        return true;
    }
    
    // Configure ELR
    ELRConfig elr_config;
    elr_config.enabled = true;
    elr_config.safe_point = config.safe_point;
    elr_config.max_dependency_chain = config.max_chain_depth;
    elr_config.allow_cross_shard = config.cross_shard_elr;
    elr_config.cascade_timeout_ms = config.cascade_timeout_ms;
    elr_config.enable_logging = config.enable_logging;
    
    // Initialize ELR manager
    auto& manager = ELRManager::getInstance(shard_id);
    manager.initialize(shard_id, elr_config);
    
    // Set up cascade abort callback for cross-shard
    if (remote_cascade_callback_) {
        manager.setAbortCallback([this, shard_id](txnid_t txn_id, const std::vector<txnid_t>& cascade_set) {
            // Notify about cascade abort
            std::cerr << "[ELR] Cascade abort triggered for txn " << txn_id
                      << " on shard " << shard_id
                      << ", cascade set size: " << cascade_set.size() << std::endl;
        });
    }
    
    // Initialize log if enabled
    if (config.enable_logging) {
        state.log = std::make_unique<ELRLog>(shard_id);
        state.log->initialize(config.log_directory);
        
        // Set up Paxos callback if available
        if (paxos_log_callback_ && config.paxos_partition >= 0) {
            state.log->setPaxosLogCallback([this, partition = config.paxos_partition](
                    const std::string& data, int) {
                paxos_log_callback_(data, partition);
            });
        }
    }
    
    state.initialized = true;
    
    // Update global config
    ELRConfigManager::getInstance().setEnabled(true);
    
    std::cerr << "[ELR] Initialized for shard " << shard_id
              << " (safe_point=" << config.safe_point
              << ", max_chain=" << config.max_chain_depth << ")" << std::endl;
    
    return true;
}

void ELRIntegration::shutdown(shardid_t shard_id) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    
    auto it = shard_states_.find(shard_id);
    if (it == shard_states_.end() || !it->second.initialized) {
        return;
    }
    
    ELRManager::getInstance(shard_id).shutdown();
    
    if (it->second.log) {
        it->second.log->shutdown();
    }
    
    it->second.initialized = false;
    
    std::cerr << "[ELR] Shutdown for shard " << shard_id << std::endl;
}

bool ELRIntegration::isInitialized(shardid_t shard_id) const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = shard_states_.find(shard_id);
    return it != shard_states_.end() && it->second.initialized;
}

ELRManager* ELRIntegration::getManager(shardid_t shard_id) {
    if (!isInitialized(shard_id)) {
        return nullptr;
    }
    return &ELRManager::getInstance(shard_id);
}

// ============================================================================
// Transaction Lifecycle Hooks
// ============================================================================

void ELRIntegration::onTransactionBegin(shardid_t shard_id, txnid_t txn_id) {
    auto* manager = getManager(shard_id);
    if (manager) {
        manager->beginTransaction(txn_id);
    }
}

bool ELRIntegration::onPreCommit(shardid_t shard_id, txnid_t txn_id) {
    auto* manager = getManager(shard_id);
    if (!manager) {
        return true; // ELR not enabled, allow commit
    }
    
    // Check if all dependencies are satisfied
    if (!manager->canCommit(txn_id)) {
        // Wait for dependencies (with timeout)
        auto& config = shard_states_[shard_id].config;
        if (!manager->waitForDependencies(txn_id, config.cascade_timeout_ms)) {
            std::cerr << "[ELR] Pre-commit timeout waiting for dependencies for txn "
                      << txn_id << std::endl;
            return false;
        }
    }
    
    return true;
}

void ELRIntegration::onCommit(shardid_t shard_id, txnid_t txn_id) {
    auto* manager = getManager(shard_id);
    if (manager) {
        manager->commitTransaction(txn_id);
    }
    
    // Log the commit
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = shard_states_.find(shard_id);
    if (it != shard_states_.end() && it->second.log) {
        it->second.log->logCommit(txn_id);
    }
}

void ELRIntegration::onAbort(shardid_t shard_id, txnid_t txn_id) {
    auto* manager = getManager(shard_id);
    if (manager) {
        manager->abortTransaction(txn_id);
    }
    
    // Log the abort
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = shard_states_.find(shard_id);
    if (it != shard_states_.end() && it->second.log) {
        it->second.log->logAbort(txn_id);
    }
}

void ELRIntegration::onPostValidate(shardid_t shard_id, txnid_t txn_id) {
    // This is the safe point for early lock release
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = shard_states_.find(shard_id);
    if (it == shard_states_.end() || !it->second.initialized) {
        return;
    }
    
    if (it->second.config.safe_point != "post_validate") {
        return;
    }
    
    // Signal that this transaction can now perform early lock release
    // The actual release is done by the transaction when it's ready
}

// ============================================================================
// ELR Operations
// ============================================================================

bool ELRIntegration::earlyReleaseLocks(shardid_t shard_id, txnid_t txn_id,
                                        const std::vector<ELRKey>& keys) {
    auto* manager = getManager(shard_id);
    if (!manager) {
        return false;
    }
    
    auto result = manager->earlyRelease(txn_id, keys);
    
    if (result.success) {
        // Log the early release
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = shard_states_.find(shard_id);
        if (it != shard_states_.end() && it->second.log) {
            for (const auto& key : keys) {
                it->second.log->logEarlyRelease(txn_id, key);
            }
        }
    }
    
    return result.success;
}

bool ELRIntegration::registerDependency(shardid_t shard_id, txnid_t reader_txn,
                                         txnid_t writer_txn, const ELRKey& key) {
    auto* manager = getManager(shard_id);
    if (!manager) {
        return false;
    }
    
    bool registered = manager->registerELRRead(reader_txn, writer_txn, key, 0);
    
    if (registered) {
        // Log the dependency
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = shard_states_.find(shard_id);
        if (it != shard_states_.end() && it->second.log) {
            it->second.log->logDependency(reader_txn, writer_txn, key);
        }
    }
    
    return registered;
}

bool ELRIntegration::isKeyEarlyReleased(shardid_t shard_id, const ELRKey& key) {
    auto* manager = getManager(shard_id);
    if (!manager) {
        return false;
    }
    return manager->isEarlyReleased(key);
}

txnid_t ELRIntegration::getEarlyReleaseOwner(shardid_t shard_id, const ELRKey& key) {
    auto* manager = getManager(shard_id);
    if (!manager) {
        return 0;
    }
    return manager->getELROwner(key);
}

// ============================================================================
// Cross-Shard Operations
// ============================================================================

void ELRIntegration::setRemoteCascadeAbortCallback(RemoteCascadeAbortCallback callback) {
    remote_cascade_callback_ = callback;
}

void ELRIntegration::handleRemoteCascadeAbort(shardid_t local_shard, txnid_t txn_id,
                                               shardid_t source_shard) {
    auto* manager = getManager(local_shard);
    if (manager) {
        manager->handleRemoteCascadeAbort(txn_id, source_shard);
    }
}

// ============================================================================
// Paxos Integration
// ============================================================================

void ELRIntegration::setPaxosLogCallback(PaxosLogCallback callback) {
    paxos_log_callback_ = callback;
}

void ELRIntegration::onPaxosLeaderCommit(shardid_t shard_id, const std::string& log_data) {
    // Leader has committed an ELR log entry
    // This is called after Paxos commits the entry
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = shard_states_.find(shard_id);
    if (it != shard_states_.end() && it->second.log) {
        // Mark as durable
        it->second.log->forceLog();
    }
}

void ELRIntegration::onPaxosFollowerReplay(shardid_t shard_id, const std::string& log_data) {
    // Follower is replaying an ELR log entry
    // Deserialize and apply the ELR operation
    ELRLogRecord record = ELRLogRecord::deserialize(log_data);
    
    auto* manager = getManager(shard_id);
    if (!manager) {
        return;
    }
    
    switch (record.type) {
        case ELRLogRecordType::EARLY_RELEASE:
            manager->earlyRelease(record.txn_id, {record.key});
            break;
        case ELRLogRecordType::COMMIT:
            manager->commitTransaction(record.txn_id);
            break;
        case ELRLogRecordType::ABORT:
        case ELRLogRecordType::CASCADE_ABORT:
            manager->abortTransaction(record.txn_id);
            break;
        case ELRLogRecordType::DEPENDENCY:
            manager->registerELRRead(record.txn_id, record.related_txn, record.key, 0);
            break;
        default:
            break;
    }
}

// ============================================================================
// Statistics and Debugging
// ============================================================================

ELRStats ELRIntegration::getStats(shardid_t shard_id) const {
    return ELRConfigManager::getInstance().getStats();
}

std::string ELRIntegration::getStatsSummary() const {
    std::ostringstream oss;
    
    oss << "=== ELR Statistics ===\n";
    
    std::lock_guard<std::mutex> lock(state_mutex_);
    for (const auto& [shard_id, state] : shard_states_) {
        if (state.initialized && state.config.enable_elr) {
            oss << ELRManager::getInstance(shard_id).getStatsSummary();
        }
    }
    
    return oss.str();
}

std::string ELRIntegration::dumpDependencyGraph(shardid_t shard_id) const {
    if (!isInitialized(shard_id)) {
        return "Shard not initialized";
    }
    
    return ELRManager::getInstance(shard_id).getDependencyTracker().dumpGraph();
}

} // namespace elr
} // namespace mako
