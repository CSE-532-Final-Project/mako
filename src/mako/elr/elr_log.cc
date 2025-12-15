/**
 * @file elr_log.cc
 * @brief Implementation of ELR Logging and Recovery
 */

#include "elr_log.h"
#include <chrono>
#include <cstring>
#include <iostream>
#include <fstream>
#include <sstream>

namespace mako {
namespace elr {

// ============================================================================
// ELRLogRecord Implementation
// ============================================================================

std::string ELRLogRecord::serialize() const {
    // Simple binary serialization
    std::ostringstream oss;
    
    // Write header
    oss.write(reinterpret_cast<const char*>(&lsn), sizeof(lsn));
    oss.write(reinterpret_cast<const char*>(&timestamp_us), sizeof(timestamp_us));
    oss.write(reinterpret_cast<const char*>(&type), sizeof(type));
    oss.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
    oss.write(reinterpret_cast<const char*>(&shard_id), sizeof(shard_id));
    
    // Write key
    oss.write(reinterpret_cast<const char*>(&key.shard_id), sizeof(key.shard_id));
    oss.write(reinterpret_cast<const char*>(&key.table_id), sizeof(key.table_id));
    uint32_t key_len = key.key.size();
    oss.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    oss.write(key.key.data(), key_len);
    
    // Write related_txn
    oss.write(reinterpret_cast<const char*>(&related_txn), sizeof(related_txn));
    
    // Write before_image
    uint32_t before_len = before_image.size();
    oss.write(reinterpret_cast<const char*>(&before_len), sizeof(before_len));
    if (before_len > 0) {
        oss.write(before_image.data(), before_len);
    }
    
    // Write after_image
    uint32_t after_len = after_image.size();
    oss.write(reinterpret_cast<const char*>(&after_len), sizeof(after_len));
    if (after_len > 0) {
        oss.write(after_image.data(), after_len);
    }
    
    return oss.str();
}

ELRLogRecord ELRLogRecord::deserialize(const std::string& data) {
    ELRLogRecord record;
    const char* ptr = data.data();
    
    // Read header
    std::memcpy(&record.lsn, ptr, sizeof(record.lsn));
    ptr += sizeof(record.lsn);
    std::memcpy(&record.timestamp_us, ptr, sizeof(record.timestamp_us));
    ptr += sizeof(record.timestamp_us);
    std::memcpy(&record.type, ptr, sizeof(record.type));
    ptr += sizeof(record.type);
    std::memcpy(&record.txn_id, ptr, sizeof(record.txn_id));
    ptr += sizeof(record.txn_id);
    std::memcpy(&record.shard_id, ptr, sizeof(record.shard_id));
    ptr += sizeof(record.shard_id);
    
    // Read key
    std::memcpy(&record.key.shard_id, ptr, sizeof(record.key.shard_id));
    ptr += sizeof(record.key.shard_id);
    std::memcpy(&record.key.table_id, ptr, sizeof(record.key.table_id));
    ptr += sizeof(record.key.table_id);
    uint32_t key_len;
    std::memcpy(&key_len, ptr, sizeof(key_len));
    ptr += sizeof(key_len);
    record.key.key.assign(ptr, key_len);
    ptr += key_len;
    
    // Read related_txn
    std::memcpy(&record.related_txn, ptr, sizeof(record.related_txn));
    ptr += sizeof(record.related_txn);
    
    // Read before_image
    uint32_t before_len;
    std::memcpy(&before_len, ptr, sizeof(before_len));
    ptr += sizeof(before_len);
    if (before_len > 0) {
        record.before_image.assign(ptr, before_len);
        ptr += before_len;
    }
    
    // Read after_image
    uint32_t after_len;
    std::memcpy(&after_len, ptr, sizeof(after_len));
    ptr += sizeof(after_len);
    if (after_len > 0) {
        record.after_image.assign(ptr, after_len);
        ptr += after_len;
    }
    
    return record;
}

ELRLogRecord ELRLogRecord::createEarlyRelease(txnid_t txn, shardid_t shard,
                                               const ELRKey& key) {
    ELRLogRecord record;
    record.type = ELRLogRecordType::EARLY_RELEASE;
    record.txn_id = txn;
    record.shard_id = shard;
    record.key = key;
    record.timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return record;
}

ELRLogRecord ELRLogRecord::createDependency(txnid_t reader, txnid_t writer,
                                             shardid_t shard, const ELRKey& key) {
    ELRLogRecord record;
    record.type = ELRLogRecordType::DEPENDENCY;
    record.txn_id = reader;
    record.related_txn = writer;
    record.shard_id = shard;
    record.key = key;
    record.timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return record;
}

ELRLogRecord ELRLogRecord::createCommit(txnid_t txn, shardid_t shard) {
    ELRLogRecord record;
    record.type = ELRLogRecordType::COMMIT;
    record.txn_id = txn;
    record.shard_id = shard;
    record.timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return record;
}

ELRLogRecord ELRLogRecord::createAbort(txnid_t txn, shardid_t shard) {
    ELRLogRecord record;
    record.type = ELRLogRecordType::ABORT;
    record.txn_id = txn;
    record.shard_id = shard;
    record.timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return record;
}

ELRLogRecord ELRLogRecord::createCascadeAbort(txnid_t txn, txnid_t cause,
                                               shardid_t shard) {
    ELRLogRecord record;
    record.type = ELRLogRecordType::CASCADE_ABORT;
    record.txn_id = txn;
    record.related_txn = cause;
    record.shard_id = shard;
    record.timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return record;
}

ELRLogRecord ELRLogRecord::createUndo(txnid_t txn, shardid_t shard,
                                       const ELRKey& key, const std::string& before) {
    ELRLogRecord record;
    record.type = ELRLogRecordType::UNDO;
    record.txn_id = txn;
    record.shard_id = shard;
    record.key = key;
    record.before_image = before;
    record.timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return record;
}

ELRLogRecord ELRLogRecord::createRedo(txnid_t txn, shardid_t shard,
                                       const ELRKey& key, const std::string& after) {
    ELRLogRecord record;
    record.type = ELRLogRecordType::REDO;
    record.txn_id = txn;
    record.shard_id = shard;
    record.key = key;
    record.after_image = after;
    record.timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return record;
}

// ============================================================================
// ELRLog Implementation
// ============================================================================

ELRLog::ELRLog(shardid_t shard_id)
    : shard_id_(shard_id), initialized_(false) {}

ELRLog::~ELRLog() {
    shutdown();
}

void ELRLog::initialize(const std::string& log_dir) {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    
    if (initialized_) return;
    
    log_dir_ = log_dir;
    log_buffer_.reserve(BUFFER_SIZE);
    initialized_ = true;
    
    std::cerr << "[ELR LOG] Initialized for shard " << shard_id_ << std::endl;
}

void ELRLog::shutdown() {
    if (!initialized_) return;
    
    forceLog();
    
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    log_buffer_.clear();
    initialized_ = false;
}

uint64_t ELRLog::getCurrentTimeUs() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

uint64_t ELRLog::appendRecord(const ELRLogRecord& record) {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    
    ELRLogRecord rec = record;
    rec.lsn = current_lsn_.fetch_add(1);
    
    log_buffer_.push_back(rec);
    
    // Track per-transaction logs for recovery
    {
        std::lock_guard<std::mutex> txn_lock(txn_logs_mutex_);
        txn_logs_[rec.txn_id].push_back(rec);
    }
    
    // Send to Paxos if callback is set
    if (paxos_callback_) {
        std::string serialized = rec.serialize();
        paxos_callback_(serialized, shard_id_);
    }
    
    // Flush if buffer is full
    if (log_buffer_.size() >= BUFFER_SIZE) {
        flushBuffer();
    }
    
    return rec.lsn;
}

void ELRLog::flushBuffer() {
    // In a real implementation, this would write to disk
    // For now, just clear the buffer
    log_buffer_.clear();
    durable_lsn_.store(current_lsn_.load());
}

void ELRLog::forceLog() {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    flushBuffer();
}

uint64_t ELRLog::logEarlyRelease(txnid_t txn_id, const ELRKey& key) {
    return appendRecord(ELRLogRecord::createEarlyRelease(txn_id, shard_id_, key));
}

uint64_t ELRLog::logDependency(txnid_t reader_txn, txnid_t writer_txn,
                                 const ELRKey& key) {
    return appendRecord(ELRLogRecord::createDependency(reader_txn, writer_txn,
                                                        shard_id_, key));
}

uint64_t ELRLog::logCommit(txnid_t txn_id) {
    uint64_t lsn = appendRecord(ELRLogRecord::createCommit(txn_id, shard_id_));
    
    // Clean up transaction logs after commit
    {
        std::lock_guard<std::mutex> lock(txn_logs_mutex_);
        txn_logs_.erase(txn_id);
    }
    
    return lsn;
}

uint64_t ELRLog::logAbort(txnid_t txn_id) {
    uint64_t lsn = appendRecord(ELRLogRecord::createAbort(txn_id, shard_id_));
    
    // Clean up transaction logs after abort
    {
        std::lock_guard<std::mutex> lock(txn_logs_mutex_);
        txn_logs_.erase(txn_id);
    }
    
    return lsn;
}

uint64_t ELRLog::logCascadeAbort(txnid_t txn_id, txnid_t cause_txn) {
    return appendRecord(ELRLogRecord::createCascadeAbort(txn_id, cause_txn, shard_id_));
}

uint64_t ELRLog::logUndo(txnid_t txn_id, const ELRKey& key,
                          const std::string& before_image) {
    return appendRecord(ELRLogRecord::createUndo(txn_id, shard_id_, key, before_image));
}

uint64_t ELRLog::logRedo(txnid_t txn_id, const ELRKey& key,
                          const std::string& after_image) {
    return appendRecord(ELRLogRecord::createRedo(txn_id, shard_id_, key, after_image));
}

uint64_t ELRLog::replayLog(uint64_t start_lsn) {
    // In a real implementation, this would read from disk
    // For now, replay from the transaction logs in memory
    
    uint64_t count = 0;
    
    std::lock_guard<std::mutex> lock(txn_logs_mutex_);
    for (const auto& [txn_id, records] : txn_logs_) {
        for (const auto& record : records) {
            if (record.lsn >= start_lsn && recovery_callback_) {
                recovery_callback_(record);
                count++;
            }
        }
    }
    
    return count;
}

std::vector<txnid_t> ELRLog::getUncommittedTransactions() const {
    std::vector<txnid_t> result;
    
    std::lock_guard<std::mutex> lock(txn_logs_mutex_);
    for (const auto& [txn_id, records] : txn_logs_) {
        // Check if transaction has a commit record
        bool committed = false;
        for (const auto& record : records) {
            if (record.type == ELRLogRecordType::COMMIT) {
                committed = true;
                break;
            }
        }
        if (!committed) {
            result.push_back(txn_id);
        }
    }
    
    return result;
}

std::vector<txnid_t> ELRLog::getCascadeAbortTransactions() const {
    std::vector<txnid_t> result;
    
    std::lock_guard<std::mutex> lock(txn_logs_mutex_);
    for (const auto& [txn_id, records] : txn_logs_) {
        for (const auto& record : records) {
            if (record.type == ELRLogRecordType::CASCADE_ABORT) {
                result.push_back(txn_id);
                break;
            }
        }
    }
    
    return result;
}

// ============================================================================
// ELRRecoveryHelper Implementation
// ============================================================================

ELRRecoveryHelper::ELRRecoveryHelper(ELRLog& log) : log_(log) {}

void ELRRecoveryHelper::performRecovery() {
    std::cerr << "[ELR RECOVERY] Starting recovery..." << std::endl;
    
    // Set up recovery callback
    log_.setRecoveryCallback([this](const ELRLogRecord& record) {
        processRecord(record);
    });
    
    // Replay the log
    uint64_t count = log_.replayLog(0);
    std::cerr << "[ELR RECOVERY] Replayed " << count << " records" << std::endl;
    
    // Resolve uncommitted transactions
    resolveUncommitted();
    
    std::cerr << "[ELR RECOVERY] Recovery complete" << std::endl;
}

bool ELRRecoveryHelper::isRecoveryNeeded() const {
    auto uncommitted = log_.getUncommittedTransactions();
    return !uncommitted.empty();
}

void ELRRecoveryHelper::processRecord(const ELRLogRecord& record) {
    switch (record.type) {
        case ELRLogRecordType::COMMIT:
            committed_txns_.insert(record.txn_id);
            aborted_txns_.erase(record.txn_id);
            break;
            
        case ELRLogRecordType::ABORT:
        case ELRLogRecordType::CASCADE_ABORT:
            aborted_txns_.insert(record.txn_id);
            committed_txns_.erase(record.txn_id);
            break;
            
        case ELRLogRecordType::DEPENDENCY:
            dependencies_[record.txn_id].insert(record.related_txn);
            break;
            
        default:
            break;
    }
}

void ELRRecoveryHelper::resolveUncommitted() {
    auto uncommitted = log_.getUncommittedTransactions();
    
    for (txnid_t txn_id : uncommitted) {
        // If any dependency aborted, this transaction must abort
        bool must_abort = false;
        
        auto it = dependencies_.find(txn_id);
        if (it != dependencies_.end()) {
            for (txnid_t dep : it->second) {
                if (aborted_txns_.count(dep) > 0) {
                    must_abort = true;
                    break;
                }
            }
        }
        
        if (must_abort) {
            std::cerr << "[ELR RECOVERY] Aborting transaction " << txn_id
                      << " due to cascade abort" << std::endl;
            log_.logAbort(txn_id);
            aborted_txns_.insert(txn_id);
        }
    }
}

} // namespace elr
} // namespace mako
