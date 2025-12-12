/**
 * @file elr_integration.h
 * @brief Integration hooks for Early Lock Release with Mako
 *
 * This file provides the integration layer between the ELR module
 * and the rest of the Mako transaction system. It includes:
 * - Initialization and configuration
 * - Transaction lifecycle hooks
 * - Paxos integration for geo-replication
 */

#ifndef _MAKO_ELR_INTEGRATION_H_
#define _MAKO_ELR_INTEGRATION_H_

#include <string>
#include <functional>
#include <memory>

#include "elr_common.h"
#include "elr_manager.h"
#include "elr_log.h"

namespace mako {
namespace elr {

// Forward declarations
class DependencyTracker;
class CascadeAbortHandler;
class ELRLog;

/**
 * @brief ELR integration configuration
 */
struct ELRIntegrationConfig {
    // Whether to enable ELR
    bool enable_elr = false;
    
    // Safe point for early release
    std::string safe_point = "post_validate";
    
    // Maximum dependency chain depth
    uint32_t max_chain_depth = 5;
    
    // Whether to allow cross-shard ELR
    bool cross_shard_elr = true;
    
    // Cascade abort timeout (ms)
    uint32_t cascade_timeout_ms = 1000;
    
    // Whether to enable ELR logging
    bool enable_logging = true;
    
    // Directory for ELR logs (empty = memory only)
    std::string log_directory = "";
    
    // Paxos partition for ELR log replication
    int paxos_partition = -1;
};

/**
 * @brief Callback type for Paxos log integration
 */
using PaxosLogCallback = std::function<void(const std::string& data, int partition)>;

/**
 * @brief Callback type for remote cascade abort
 */
using RemoteCascadeAbortCallback = std::function<bool(txnid_t txn_id, shardid_t shard_id)>;

/**
 * @brief Main integration class for ELR in Mako
 *
 * This class manages the lifecycle of ELR components and provides
 * hooks for integration with the transaction system.
 */
class ELRIntegration {
public:
    /**
     * @brief Get the singleton instance
     */
    static ELRIntegration& getInstance();
    
    /**
     * @brief Initialize ELR for a shard
     *
     * This should be called once during server startup for each shard.
     *
     * @param shard_id ID of this shard
     * @param config ELR configuration
     * @return true if initialization succeeded
     */
    bool initialize(shardid_t shard_id, const ELRIntegrationConfig& config);
    
    /**
     * @brief Shutdown ELR for a shard
     */
    void shutdown(shardid_t shard_id);
    
    /**
     * @brief Check if ELR is initialized for a shard
     */
    bool isInitialized(shardid_t shard_id) const;
    
    // ========================================================================
    // Transaction Lifecycle Hooks
    // ========================================================================
    
    /**
     * @brief Called when a transaction begins
     */
    void onTransactionBegin(shardid_t shard_id, txnid_t txn_id);
    
    /**
     * @brief Called when a transaction is about to commit
     *
     * This checks if all ELR dependencies are satisfied before
     * allowing the commit to proceed.
     *
     * @return true if commit is allowed
     */
    bool onPreCommit(shardid_t shard_id, txnid_t txn_id);
    
    /**
     * @brief Called when a transaction commits
     */
    void onCommit(shardid_t shard_id, txnid_t txn_id);
    
    /**
     * @brief Called when a transaction aborts
     *
     * This triggers cascade abort if the transaction had early-released.
     */
    void onAbort(shardid_t shard_id, txnid_t txn_id);
    
    /**
     * @brief Called after validation passes
     *
     * This is the safe point where early lock release can occur.
     */
    void onPostValidate(shardid_t shard_id, txnid_t txn_id);
    
    // ========================================================================
    // ELR Operations
    // ========================================================================
    
    /**
     * @brief Perform early lock release for a transaction
     *
     * @param shard_id Shard ID
     * @param txn_id Transaction ID
     * @param keys Keys to early release
     * @return true if early release succeeded
     */
    bool earlyReleaseLocks(shardid_t shard_id, txnid_t txn_id,
                           const std::vector<ELRKey>& keys);
    
    /**
     * @brief Register that a transaction read from an early-released lock
     *
     * @param shard_id Shard ID
     * @param reader_txn Reader transaction ID
     * @param writer_txn Writer transaction ID (who early-released)
     * @param key Key that was read
     * @return true if dependency was registered
     */
    bool registerDependency(shardid_t shard_id, txnid_t reader_txn,
                           txnid_t writer_txn, const ELRKey& key);
    
    /**
     * @brief Check if a key is early-released
     */
    bool isKeyEarlyReleased(shardid_t shard_id, const ELRKey& key);
    
    /**
     * @brief Get the owner of an early-released key
     */
    txnid_t getEarlyReleaseOwner(shardid_t shard_id, const ELRKey& key);
    
    // ========================================================================
    // Cross-Shard Operations
    // ========================================================================
    
    /**
     * @brief Set callback for remote cascade abort
     */
    void setRemoteCascadeAbortCallback(RemoteCascadeAbortCallback callback);
    
    /**
     * @brief Handle cascade abort from remote shard
     */
    void handleRemoteCascadeAbort(shardid_t local_shard, txnid_t txn_id,
                                   shardid_t source_shard);
    
    // ========================================================================
    // Paxos Integration
    // ========================================================================
    
    /**
     * @brief Set callback for Paxos log replication
     */
    void setPaxosLogCallback(PaxosLogCallback callback);
    
    /**
     * @brief Called when Paxos log entry is committed (leader)
     */
    void onPaxosLeaderCommit(shardid_t shard_id, const std::string& log_data);
    
    /**
     * @brief Called when Paxos log entry is replayed (follower)
     */
    void onPaxosFollowerReplay(shardid_t shard_id, const std::string& log_data);
    
    // ========================================================================
    // Statistics and Debugging
    // ========================================================================
    
    /**
     * @brief Get statistics for a shard
     */
    ELRStats getStats(shardid_t shard_id) const;
    
    /**
     * @brief Get overall statistics
     */
    std::string getStatsSummary() const;
    
    /**
     * @brief Dump dependency graph for debugging
     */
    std::string dumpDependencyGraph(shardid_t shard_id) const;
    
private:
    ELRIntegration();
    ~ELRIntegration();
    
    // Prevent copying
    ELRIntegration(const ELRIntegration&) = delete;
    ELRIntegration& operator=(const ELRIntegration&) = delete;
    
    // Per-shard state
    struct ShardState {
        ELRIntegrationConfig config;
        std::unique_ptr<ELRLog> log;
        bool initialized;
        
        ShardState() : initialized(false) {}
    };
    
    std::unordered_map<shardid_t, ShardState> shard_states_;
    mutable std::mutex state_mutex_;
    
    // Callbacks
    RemoteCascadeAbortCallback remote_cascade_callback_;
    PaxosLogCallback paxos_log_callback_;
    
    // Helper to get ELR manager for a shard
    ELRManager* getManager(shardid_t shard_id);
};

/**
 * @brief RAII guard for ELR transaction integration
 *
 * Use this in transaction code to automatically hook into ELR.
 */
class ELRTransactionScope {
public:
    ELRTransactionScope(shardid_t shard_id, txnid_t txn_id)
        : shard_id_(shard_id), txn_id_(txn_id), committed_(false) {
        ELRIntegration::getInstance().onTransactionBegin(shard_id, txn_id);
    }
    
    ~ELRTransactionScope() {
        if (!committed_) {
            ELRIntegration::getInstance().onAbort(shard_id_, txn_id_);
        }
    }
    
    bool preCommit() {
        return ELRIntegration::getInstance().onPreCommit(shard_id_, txn_id_);
    }
    
    void commit() {
        committed_ = true;
        ELRIntegration::getInstance().onCommit(shard_id_, txn_id_);
    }
    
    void postValidate() {
        ELRIntegration::getInstance().onPostValidate(shard_id_, txn_id_);
    }
    
private:
    shardid_t shard_id_;
    txnid_t txn_id_;
    bool committed_;
};

/**
 * @brief Helper macro for ELR transaction scope
 */
#define ELR_TRANSACTION_SCOPE(shard_id, txn_id) \
    mako::elr::ELRTransactionScope _elr_scope(shard_id, txn_id)

} // namespace elr
} // namespace mako

#endif // _MAKO_ELR_INTEGRATION_H_
