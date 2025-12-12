/**
 * @file elr_log.h
 * @brief Logging and Recovery for Early Lock Release
 *
 * ELR requires special logging to support recovery. When a transaction
 * early-releases its locks, we must log enough information to:
 * 1. Redo the writes if the transaction commits
 * 2. Undo the writes if the transaction aborts
 * 3. Track dependencies for cascade abort during recovery
 *
 * The ELR log integrates with Mako's existing Paxos-based replication.
 */

#ifndef _MAKO_ELR_LOG_H_
#define _MAKO_ELR_LOG_H_

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <cstdint>

#include "elr_common.h"

namespace mako {
namespace elr {

/**
 * @brief Type of ELR log record
 */
enum class ELRLogRecordType : uint8_t {
    EARLY_RELEASE = 1,   // Lock early-released
    DEPENDENCY = 2,      // Dependency created
    PREPARE = 3,         // Transaction prepared
    COMMIT = 4,          // Transaction committed
    ABORT = 5,           // Transaction aborted
    CASCADE_ABORT = 6,   // Cascade abort triggered
    UNDO = 7,            // Undo record for recovery
    REDO = 8             // Redo record for recovery
};

/**
 * @brief A single ELR log record
 */
struct ELRLogRecord {
    // Header
    uint64_t lsn;                    // Log sequence number
    uint64_t timestamp_us;           // Timestamp in microseconds
    ELRLogRecordType type;           // Record type
    txnid_t txn_id;                  // Transaction ID
    shardid_t shard_id;              // Shard ID
    
    // Payload (varies by type)
    ELRKey key;                      // Key involved (for EARLY_RELEASE, UNDO, REDO)
    txnid_t related_txn;             // Related transaction (for DEPENDENCY, CASCADE_ABORT)
    std::string before_image;        // Before image (for UNDO)
    std::string after_image;         // After image (for REDO)
    
    // Serialization
    std::string serialize() const;
    static ELRLogRecord deserialize(const std::string& data);
    
    // Helpers
    static ELRLogRecord createEarlyRelease(txnid_t txn, shardid_t shard,
                                            const ELRKey& key);
    static ELRLogRecord createDependency(txnid_t reader, txnid_t writer,
                                          shardid_t shard, const ELRKey& key);
    static ELRLogRecord createCommit(txnid_t txn, shardid_t shard);
    static ELRLogRecord createAbort(txnid_t txn, shardid_t shard);
    static ELRLogRecord createCascadeAbort(txnid_t txn, txnid_t cause,
                                            shardid_t shard);
    static ELRLogRecord createUndo(txnid_t txn, shardid_t shard,
                                    const ELRKey& key, const std::string& before);
    static ELRLogRecord createRedo(txnid_t txn, shardid_t shard,
                                    const ELRKey& key, const std::string& after);
};

/**
 * @brief Callback to integrate with Paxos log
 */
using PaxosLogCallback = std::function<void(const std::string& log_data, int partition)>;

/**
 * @brief Callback for recovery replay
 */
using RecoveryCallback = std::function<void(const ELRLogRecord& record)>;

/**
 * @brief ELR Log Manager
 *
 * Manages logging of ELR operations for recovery and integrates
 * with Mako's Paxos replication.
 */
class ELRLog {
public:
    ELRLog(shardid_t shard_id);
    ~ELRLog();
    
    /**
     * @brief Initialize the log
     * @param log_dir Directory for log files (if using local logging)
     */
    void initialize(const std::string& log_dir = "");
    
    /**
     * @brief Shutdown the log
     */
    void shutdown();
    
    /**
     * @brief Set callback for Paxos log integration
     */
    void setPaxosLogCallback(PaxosLogCallback callback) {
        paxos_callback_ = callback;
    }
    
    // ========================================================================
    // Logging Operations
    // ========================================================================
    
    /**
     * @brief Log an early release operation
     */
    uint64_t logEarlyRelease(txnid_t txn_id, const ELRKey& key);
    
    /**
     * @brief Log a dependency between transactions
     */
    uint64_t logDependency(txnid_t reader_txn, txnid_t writer_txn,
                            const ELRKey& key);
    
    /**
     * @brief Log transaction commit
     */
    uint64_t logCommit(txnid_t txn_id);
    
    /**
     * @brief Log transaction abort
     */
    uint64_t logAbort(txnid_t txn_id);
    
    /**
     * @brief Log cascade abort
     */
    uint64_t logCascadeAbort(txnid_t txn_id, txnid_t cause_txn);
    
    /**
     * @brief Log undo record (before image)
     */
    uint64_t logUndo(txnid_t txn_id, const ELRKey& key,
                      const std::string& before_image);
    
    /**
     * @brief Log redo record (after image)
     */
    uint64_t logRedo(txnid_t txn_id, const ELRKey& key,
                      const std::string& after_image);
    
    /**
     * @brief Force log to durable storage
     */
    void forceLog();
    
    // ========================================================================
    // Recovery Operations
    // ========================================================================
    
    /**
     * @brief Set callback for recovery replay
     */
    void setRecoveryCallback(RecoveryCallback callback) {
        recovery_callback_ = callback;
    }
    
    /**
     * @brief Replay log from a given LSN
     * @param start_lsn Starting LSN (0 for beginning)
     * @return Number of records replayed
     */
    uint64_t replayLog(uint64_t start_lsn = 0);
    
    /**
     * @brief Get the current LSN
     */
    uint64_t getCurrentLSN() const { return current_lsn_.load(); }
    
    /**
     * @brief Get the durable LSN (flushed to disk)
     */
    uint64_t getDurableLSN() const { return durable_lsn_.load(); }
    
    // ========================================================================
    // Transaction State for Recovery
    // ========================================================================
    
    /**
     * @brief Get uncommitted transactions at recovery time
     */
    std::vector<txnid_t> getUncommittedTransactions() const;
    
    /**
     * @brief Get transactions that need cascade abort at recovery
     */
    std::vector<txnid_t> getCascadeAbortTransactions() const;
    
private:
    shardid_t shard_id_;
    std::string log_dir_;
    bool initialized_;
    
    // LSN tracking
    std::atomic<uint64_t> current_lsn_{0};
    std::atomic<uint64_t> durable_lsn_{0};
    
    // Log buffer
    std::vector<ELRLogRecord> log_buffer_;
    mutable std::mutex buffer_mutex_;
    static constexpr size_t BUFFER_SIZE = 1000;
    
    // Callbacks
    PaxosLogCallback paxos_callback_;
    RecoveryCallback recovery_callback_;
    
    // Transaction state (for recovery)
    std::unordered_map<txnid_t, std::vector<ELRLogRecord>> txn_logs_;
    mutable std::mutex txn_logs_mutex_;
    
    // Helper methods
    uint64_t appendRecord(const ELRLogRecord& record);
    void flushBuffer();
    uint64_t getCurrentTimeUs() const;
};

/**
 * @brief Recovery helper that processes ELR log records
 */
class ELRRecoveryHelper {
public:
    ELRRecoveryHelper(ELRLog& log);
    
    /**
     * @brief Perform recovery using the ELR log
     *
     * This replays the log and:
     * 1. Identifies uncommitted transactions
     * 2. Rolls back uncommitted transactions that aborted
     * 3. Redoes committed transactions
     * 4. Handles cascade aborts
     */
    void performRecovery();
    
    /**
     * @brief Check if recovery is needed
     */
    bool isRecoveryNeeded() const;
    
private:
    ELRLog& log_;
    
    // Recovery state
    std::unordered_set<txnid_t> committed_txns_;
    std::unordered_set<txnid_t> aborted_txns_;
    std::unordered_map<txnid_t, std::unordered_set<txnid_t>> dependencies_;
    
    void processRecord(const ELRLogRecord& record);
    void resolveUncommitted();
};

} // namespace elr
} // namespace mako

#endif // _MAKO_ELR_LOG_H_
