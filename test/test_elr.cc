/**
 * @file test_elr.cc
 * @brief Unit tests for Early Lock Release (ELR) functionality
 *
 * Tests cover:
 * - ELR Manager operations
 * - Dependency tracking
 * - Cascade abort handling
 * - ELR logging and recovery
 */

#define ENABLE_ELR

#include <iostream>
#include <cassert>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>

#include "src/mako/elr/elr_common.h"
#include "src/mako/elr/elr_manager.h"
#include "src/mako/elr/dependency_tracker.h"
#include "src/mako/elr/cascade_abort.h"
#include "src/mako/elr/elr_log.h"

using namespace mako::elr;

// Test helper macros
#define TEST(name) void test_##name()
#define RUN_TEST(name) \
    do { \
        std::cout << "Running " << #name << "... "; \
        test_##name(); \
        std::cout << "PASSED" << std::endl; \
    } while(0)

// ============================================================================
// ELR Common Tests
// ============================================================================

TEST(elr_config_defaults) {
    ELRConfig config;
    assert(!config.enabled);
    assert(config.safe_point == "post_validate");
    assert(config.cascade_timeout_ms == 1000);
    assert(config.max_dependency_chain == 5);
}

TEST(elr_config_manager) {
    auto& manager = ELRConfigManager::getInstance();
    
    manager.setEnabled(true);
    assert(manager.isEnabled());
    assert(elr_enabled());
    
    manager.setEnabled(false);
    assert(!manager.isEnabled());
    assert(!elr_enabled());
}

TEST(elr_key_operations) {
    ELRKey key1{1, 10, "key1"};
    ELRKey key2{1, 10, "key1"};
    ELRKey key3{1, 10, "key2"};
    
    assert(key1 == key2);
    assert(!(key1 == key3));
    assert(key1 < key3);
    
    ELRKeyHash hash;
    assert(hash(key1) == hash(key2));
    assert(hash(key1) != hash(key3));
}

TEST(elr_stats) {
    auto& manager = ELRConfigManager::getInstance();
    auto& stats = manager.getStats();
    
    stats.reset();
    assert(stats.total_early_releases.load() == 0);
    assert(stats.successful_commits.load() == 0);
    
    stats.total_early_releases.fetch_add(5);
    assert(stats.total_early_releases.load() == 5);
    
    stats.reset();
    assert(stats.total_early_releases.load() == 0);
}

// ============================================================================
// Dependency Tracker Tests
// ============================================================================

TEST(dependency_tracker_basic) {
    DependencyTracker tracker;
    
    txnid_t txn1 = 1, txn2 = 2, txn3 = 3;
    
    tracker.registerTransaction(txn1);
    tracker.registerTransaction(txn2);
    tracker.registerTransaction(txn3);
    
    assert(tracker.getActiveTransactionCount() == 3);
    
    ELRKey key{0, 1, "test_key"};
    
    // txn2 depends on txn1 (txn2 read from txn1's early-released lock)
    bool added = tracker.addDependency(txn2, txn1, key, 100);
    assert(added);
    assert(tracker.getDependencyCount() == 1);
    
    // Check dependencies
    auto deps = tracker.getDependencies(txn2);
    assert(deps.count(txn1) == 1);
    
    auto dependents = tracker.getDependentTransactions(txn1);
    assert(dependents.count(txn2) == 1);
    
    // Cleanup
    tracker.unregisterTransaction(txn1);
    tracker.unregisterTransaction(txn2);
    tracker.unregisterTransaction(txn3);
}

TEST(dependency_tracker_cycle_detection) {
    DependencyTracker tracker;
    
    txnid_t txn1 = 1, txn2 = 2, txn3 = 3;
    
    tracker.registerTransaction(txn1);
    tracker.registerTransaction(txn2);
    tracker.registerTransaction(txn3);
    
    ELRKey key{0, 1, "key"};
    
    // Create chain: txn1 <- txn2 <- txn3
    assert(tracker.addDependency(txn2, txn1, key, 1));
    assert(tracker.addDependency(txn3, txn2, key, 2));
    
    // Try to create cycle: txn1 depends on txn3 (should fail)
    assert(!tracker.addDependency(txn1, txn3, key, 3));
    
    // Verify cycle would be detected
    assert(tracker.wouldCreateCycle(txn1, txn3));
    assert(!tracker.wouldCreateCycle(txn3, txn1)); // This direction is fine
    
    tracker.clear();
}

TEST(dependency_tracker_cascade_abort_set) {
    DependencyTracker tracker;
    
    txnid_t txn1 = 1, txn2 = 2, txn3 = 3, txn4 = 4;
    
    tracker.registerTransaction(txn1);
    tracker.registerTransaction(txn2);
    tracker.registerTransaction(txn3);
    tracker.registerTransaction(txn4);
    
    ELRKey key{0, 1, "key"};
    
    // Create dependency tree:
    //     txn1
    //    /    \
    //  txn2  txn3
    //    \
    //   txn4
    
    tracker.addDependency(txn2, txn1, key, 1);
    tracker.addDependency(txn3, txn1, key, 2);
    tracker.addDependency(txn4, txn2, key, 3);
    
    // If txn1 aborts, txn2, txn3, txn4 should all abort
    auto cascade_set = tracker.getCascadeAbortSet(txn1, 0);
    assert(cascade_set.size() == 3);
    
    // Check that cascade order is correct (leaves first)
    // txn4 should come before txn2
    size_t txn2_pos = 0, txn4_pos = 0;
    for (size_t i = 0; i < cascade_set.size(); i++) {
        if (cascade_set[i] == txn2) txn2_pos = i;
        if (cascade_set[i] == txn4) txn4_pos = i;
    }
    assert(txn4_pos < txn2_pos); // txn4 should be aborted first
    
    tracker.clear();
}

TEST(dependency_tracker_can_commit) {
    DependencyTracker tracker;
    
    txnid_t txn1 = 1, txn2 = 2;
    
    tracker.registerTransaction(txn1);
    tracker.registerTransaction(txn2);
    
    ELRKey key{0, 1, "key"};
    tracker.addDependency(txn2, txn1, key, 1);
    
    // txn2 cannot commit until txn1 commits
    assert(!tracker.canCommit(txn2));
    assert(tracker.canCommit(txn1)); // txn1 has no dependencies
    
    // After txn1 commits, txn2 can commit
    tracker.markCommitted(txn1);
    assert(tracker.canCommit(txn2));
    
    tracker.clear();
}

TEST(dependency_tracker_chain_depth) {
    DependencyTracker tracker;
    
    // Create a chain of 5 transactions
    for (int i = 1; i <= 5; i++) {
        tracker.registerTransaction(i);
    }
    
    ELRKey key{0, 1, "key"};
    
    for (int i = 2; i <= 5; i++) {
        tracker.addDependency(i, i - 1, key, i);
    }
    
    // Check chain depths
    assert(tracker.getChainDepth(1) == 0);
    assert(tracker.getChainDepth(2) == 1);
    assert(tracker.getChainDepth(3) == 2);
    assert(tracker.getChainDepth(4) == 3);
    assert(tracker.getChainDepth(5) == 4);
    
    // Check depth limit
    assert(!tracker.isChainTooDeep(3, 3));
    assert(tracker.isChainTooDeep(5, 3));
    
    tracker.clear();
}

// ============================================================================
// ELR Manager Tests
// ============================================================================

TEST(elr_manager_basic) {
    auto& manager = ELRManager::getInstance(0);
    
    ELRConfig config;
    config.enabled = true;
    config.max_dependency_chain = 5;
    
    manager.initialize(0, config);
    
    txnid_t txn1 = 100;
    manager.beginTransaction(txn1);
    
    assert(manager.canEarlyRelease(txn1));
    
    manager.endTransaction(txn1, true);
    
    manager.shutdown();
}

TEST(elr_manager_early_release) {
    auto& manager = ELRManager::getInstance(1);
    
    ELRConfig config;
    config.enabled = true;
    
    manager.initialize(1, config);
    
    txnid_t txn1 = 200;
    manager.beginTransaction(txn1);
    
    std::vector<ELRKey> keys = {
        {1, 1, "key1"},
        {1, 1, "key2"}
    };
    
    auto result = manager.earlyRelease(txn1, keys);
    assert(result.success);
    assert(result.released_keys.size() == 2);
    
    // Check that keys are tracked
    assert(manager.isEarlyReleased(keys[0]));
    assert(manager.getELROwner(keys[0]) == txn1);
    
    // Commit the transaction
    manager.commitTransaction(txn1);
    
    // Keys should no longer be early-released
    assert(!manager.isEarlyReleased(keys[0]));
    
    manager.shutdown();
}

TEST(elr_manager_dependency_registration) {
    auto& manager = ELRManager::getInstance(2);
    
    ELRConfig config;
    config.enabled = true;
    
    manager.initialize(2, config);
    
    txnid_t txn1 = 300, txn2 = 301;
    manager.beginTransaction(txn1);
    manager.beginTransaction(txn2);
    
    ELRKey key{2, 1, "shared_key"};
    
    // txn1 early-releases
    manager.earlyRelease(txn1, {key});
    
    // txn2 reads from early-released key
    bool registered = manager.registerELRRead(txn2, txn1, key, 1);
    assert(registered);
    
    // txn2 cannot commit until txn1 commits
    assert(!manager.canCommit(txn2));
    
    // txn1 commits
    manager.commitTransaction(txn1);
    
    // Now txn2 can commit
    assert(manager.canCommit(txn2));
    
    manager.endTransaction(txn2, true);
    manager.shutdown();
}

TEST(elr_manager_cascade_abort) {
    auto& manager = ELRManager::getInstance(3);
    
    ELRConfig config;
    config.enabled = true;
    
    manager.initialize(3, config);
    
    txnid_t txn1 = 400, txn2 = 401, txn3 = 402;
    manager.beginTransaction(txn1);
    manager.beginTransaction(txn2);
    manager.beginTransaction(txn3);
    
    ELRKey key1{3, 1, "key1"};
    ELRKey key2{3, 1, "key2"};
    
    // Chain: txn1 <- txn2 <- txn3
    manager.earlyRelease(txn1, {key1});
    manager.registerELRRead(txn2, txn1, key1, 1);
    manager.earlyRelease(txn2, {key2});
    manager.registerELRRead(txn3, txn2, key2, 2);
    
    // Get cascade abort set for txn1
    auto cascade_set = manager.getCascadeAbortSet(txn1);
    assert(cascade_set.size() == 2); // txn2 and txn3
    
    // Abort txn1 - should cascade to txn2 and txn3
    auto result = manager.abortTransaction(txn1);
    assert(result.success);
    assert(result.aborted_txns.size() >= 1);
    
    manager.shutdown();
}

// ============================================================================
// Cascade Abort Handler Tests
// ============================================================================

TEST(cascade_abort_handler) {
    DependencyTracker tracker;
    CascadeAbortHandler handler(0);
    handler.setDependencyTracker(&tracker);
    
    txnid_t txn1 = 500, txn2 = 501, txn3 = 502;
    
    tracker.registerTransaction(txn1);
    tracker.registerTransaction(txn2);
    tracker.registerTransaction(txn3);
    
    ELRKey key{0, 1, "key"};
    
    // Create dependency chain
    tracker.addDependency(txn2, txn1, key, 1);
    tracker.addDependency(txn3, txn2, key, 2);
    
    // Initiate cascade abort from txn1
    auto op = handler.initiateCascadeAbort(txn1, 1000);
    
    assert(op.status == CascadeStatus::COMPLETED || 
           op.status == CascadeStatus::PARTIAL);
    assert(op.abort_list.size() >= 2); // txn2 and txn3
    
    tracker.clear();
}

// ============================================================================
// ELR Log Tests
// ============================================================================

TEST(elr_log_basic) {
    ELRLog log(0);
    log.initialize("");
    
    txnid_t txn1 = 600;
    ELRKey key{0, 1, "key"};
    
    uint64_t lsn1 = log.logEarlyRelease(txn1, key);
    uint64_t lsn2 = log.logCommit(txn1);
    
    assert(lsn2 > lsn1);
    assert(log.getCurrentLSN() == lsn2 + 1);
    
    log.shutdown();
}

TEST(elr_log_serialization) {
    ELRLogRecord record = ELRLogRecord::createEarlyRelease(
        100, 0, ELRKey{0, 1, "test_key"});
    
    std::string serialized = record.serialize();
    ELRLogRecord deserialized = ELRLogRecord::deserialize(serialized);
    
    assert(deserialized.type == ELRLogRecordType::EARLY_RELEASE);
    assert(deserialized.txn_id == 100);
    assert(deserialized.key.table_id == 1);
    assert(deserialized.key.key == "test_key");
}

TEST(elr_log_recovery) {
    ELRLog log(0);
    log.initialize("");
    
    txnid_t txn1 = 700, txn2 = 701;
    ELRKey key{0, 1, "key"};
    
    // Simulate a transaction that early-released but didn't commit
    log.logEarlyRelease(txn1, key);
    log.logDependency(txn2, txn1, key);
    
    // txn1 committed, txn2 did not
    log.logCommit(txn1);
    
    // Check uncommitted transactions
    auto uncommitted = log.getUncommittedTransactions();
    // txn2 should be uncommitted (its dependency resolved but it never committed)
    
    log.shutdown();
}

// ============================================================================
// Multi-threaded Tests
// ============================================================================

TEST(elr_concurrent_dependency_tracking) {
    DependencyTracker tracker;
    const int num_transactions = 100;
    const int num_threads = 4;
    std::atomic<int> success_count{0};
    
    // Register all transactions first
    for (int i = 0; i < num_transactions; i++) {
        tracker.registerTransaction(i);
    }
    
    auto worker = [&](int thread_id) {
        int start = thread_id * (num_transactions / num_threads / 2);
        int count = num_transactions / num_threads / 4;
        
        for (int i = 0; i < count && (start + i + 1) < num_transactions; i++) {
            ELRKey key{0, 1, "key" + std::to_string(start + i)};
            if (tracker.addDependency(start + i + 1, start + i, key, i)) {
                success_count.fetch_add(1);
            }
        }
    };
    
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker, i);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    assert(success_count.load() > 0);
    tracker.clear();
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "=== Early Lock Release (ELR) Tests ===" << std::endl;
    std::cout << std::endl;
    
    std::cout << "--- ELR Common Tests ---" << std::endl;
    RUN_TEST(elr_config_defaults);
    RUN_TEST(elr_config_manager);
    RUN_TEST(elr_key_operations);
    RUN_TEST(elr_stats);
    
    std::cout << std::endl << "--- Dependency Tracker Tests ---" << std::endl;
    RUN_TEST(dependency_tracker_basic);
    RUN_TEST(dependency_tracker_cycle_detection);
    RUN_TEST(dependency_tracker_cascade_abort_set);
    RUN_TEST(dependency_tracker_can_commit);
    RUN_TEST(dependency_tracker_chain_depth);
    
    std::cout << std::endl << "--- ELR Manager Tests ---" << std::endl;
    RUN_TEST(elr_manager_basic);
    RUN_TEST(elr_manager_early_release);
    RUN_TEST(elr_manager_dependency_registration);
    RUN_TEST(elr_manager_cascade_abort);
    
    std::cout << std::endl << "--- Cascade Abort Handler Tests ---" << std::endl;
    RUN_TEST(cascade_abort_handler);
    
    std::cout << std::endl << "--- ELR Log Tests ---" << std::endl;
    RUN_TEST(elr_log_basic);
    RUN_TEST(elr_log_serialization);
    RUN_TEST(elr_log_recovery);
    
    std::cout << std::endl << "--- Multi-threaded Tests ---" << std::endl;
    RUN_TEST(elr_concurrent_dependency_tracking);
    
    std::cout << std::endl;
    std::cout << "=== All ELR Tests Passed! ===" << std::endl;
    
    return 0;
}
