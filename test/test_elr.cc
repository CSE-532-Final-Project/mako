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
#include <iomanip>
#include <cassert>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <algorithm>

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
    
    // Verify cascade set contains the correct transactions
    assert(std::find(cascade_set.begin(), cascade_set.end(), txn2) != cascade_set.end());
    assert(std::find(cascade_set.begin(), cascade_set.end(), txn3) != cascade_set.end());
    
    // Abort txn1 - should cascade to txn2 and txn3
    auto result = manager.abortTransaction(txn1);
    assert(result.success);
    // Should have aborted at least txn1 plus its dependents (txn2 and txn3)
    assert(result.aborted_txns.size() >= 3);
    
    // Verify all transactions are in the aborted list
    assert(std::find(result.aborted_txns.begin(), result.aborted_txns.end(), txn1) != result.aborted_txns.end());
    assert(std::find(result.aborted_txns.begin(), result.aborted_txns.end(), txn2) != result.aborted_txns.end());
    assert(std::find(result.aborted_txns.begin(), result.aborted_txns.end(), txn3) != result.aborted_txns.end());
    
    // Verify early-released keys are cleaned up after abort
    assert(!manager.isEarlyReleased(key1));
    assert(!manager.isEarlyReleased(key2));
    
    manager.shutdown();
}

TEST(elr_manager_chain_depth_limit) {
    auto& manager = ELRManager::getInstance(4);
    
    ELRConfig config;
    config.enabled = true;
    config.max_dependency_chain = 2;  // Very low limit for testing
    
    manager.initialize(4, config);
    
    txnid_t txn1 = 500, txn2 = 501, txn3 = 502, txn4 = 503;
    manager.beginTransaction(txn1);
    manager.beginTransaction(txn2);
    manager.beginTransaction(txn3);
    manager.beginTransaction(txn4);
    
    ELRKey key1{4, 1, "key1"};
    ELRKey key2{4, 1, "key2"};
    ELRKey key3{4, 1, "key3"};
    
    // Create chain: txn1 <- txn2 <- txn3
    manager.earlyRelease(txn1, {key1});
    manager.registerELRRead(txn2, txn1, key1, 1);
    manager.earlyRelease(txn2, {key2});
    manager.registerELRRead(txn3, txn2, key2, 2);
    
    // txn3 is at chain depth 2, which equals max_dependency_chain
    // txn4 trying to depend on txn3 would create depth 3, exceeding limit
    manager.registerELRRead(txn4, txn3, key3, 3);
    
    // txn4 should NOT be allowed to early release (chain too deep)
    assert(!manager.canEarlyRelease(txn4));
    
    // txn1 should still be allowed (at root of chain)
    // Note: txn1 already early-released, so this checks the logic
    
    manager.shutdown();
}

TEST(elr_manager_disabled) {
    auto& manager = ELRManager::getInstance(5);
    
    ELRConfig config;
    config.enabled = false;  // ELR disabled
    
    manager.initialize(5, config);
    
    txnid_t txn1 = 600;
    manager.beginTransaction(txn1);
    
    // When ELR is disabled, canEarlyRelease should return false
    assert(!manager.canEarlyRelease(txn1));
    
    // Early release should fail when disabled
    ELRKey key{5, 1, "key"};
    auto result = manager.earlyRelease(txn1, {key});
    assert(!result.success);
    
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
    
    // Create dependency chain: txn1 <- txn2 <- txn3
    tracker.addDependency(txn2, txn1, key, 1);
    tracker.addDependency(txn3, txn2, key, 2);
    
    // Initiate cascade abort from txn1
    auto op = handler.initiateCascadeAbort(txn1, 1000);
    
    // Must complete successfully (not partial or failed)
    assert(op.status == CascadeStatus::COMPLETED);
    
    // Must have exactly 2 transactions in abort list (txn2 and txn3)
    assert(op.abort_list.size() == 2);
    
    // Verify specific transactions are in the abort list
    bool found_txn2 = false, found_txn3 = false;
    for (const auto& info : op.abort_list) {
        if (info.txn_id == txn2) found_txn2 = true;
        if (info.txn_id == txn3) found_txn3 = true;
        // Each abort should have completed
        assert(info.status == CascadeStatus::COMPLETED);
    }
    assert(found_txn2);
    assert(found_txn3);
    
    // Verify cascade stats were updated
    assert(handler.getStats().total_cascades.load() >= 1);
    assert(handler.getStats().total_aborted_txns.load() >= 2);
    
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
    
    // Log should start at LSN 0
    uint64_t initial_lsn = log.getCurrentLSN();
    
    uint64_t lsn1 = log.logEarlyRelease(txn1, key);
    assert(lsn1 == initial_lsn);
    
    uint64_t lsn2 = log.logCommit(txn1);
    assert(lsn2 > lsn1);
    assert(lsn2 == lsn1 + 1);
    
    assert(log.getCurrentLSN() == lsn2 + 1);
    
    // After commit, txn1 should NOT be in uncommitted list
    auto uncommitted = log.getUncommittedTransactions();
    assert(std::find(uncommitted.begin(), uncommitted.end(), txn1) == uncommitted.end());
    
    log.shutdown();
}

TEST(elr_log_abort_tracking) {
    ELRLog log(0);
    log.initialize("");
    
    txnid_t txn1 = 650, txn2 = 651;
    ELRKey key{0, 1, "key"};
    
    // txn1 early releases, then aborts
    log.logEarlyRelease(txn1, key);
    log.logDependency(txn2, txn1, key);
    log.logAbort(txn1);
    
    // txn1 aborted, should not be in uncommitted (it's resolved as aborted)
    auto uncommitted = log.getUncommittedTransactions();
    assert(std::find(uncommitted.begin(), uncommitted.end(), txn1) == uncommitted.end());
    
    // txn2 depends on aborted txn1, should need cascade abort
    auto cascade = log.getCascadeAbortTransactions();
    // Note: txn2 itself isn't marked as cascade abort until we log it
    
    // Log cascade abort for txn2
    log.logCascadeAbort(txn2, txn1);
    cascade = log.getCascadeAbortTransactions();
    assert(std::find(cascade.begin(), cascade.end(), txn2) != cascade.end());
    
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
    // txn1 committed, so it should NOT be in uncommitted list
    assert(std::find(uncommitted.begin(), uncommitted.end(), txn1) == uncommitted.end());
    // txn2 never committed, so it SHOULD be in uncommitted list
    assert(std::find(uncommitted.begin(), uncommitted.end(), txn2) != uncommitted.end());
    
    log.shutdown();
}

// ============================================================================
// Multi-threaded Tests
// ============================================================================

TEST(elr_concurrent_dependency_tracking) {
    DependencyTracker tracker;
    const int num_transactions = 100;
    const int num_threads = 4;
    const int ops_per_thread = 20;
    std::atomic<int> success_count{0};
    
    // Register all transactions first
    for (int i = 0; i < num_transactions; i++) {
        tracker.registerTransaction(i);
    }
    
    auto worker = [&](int thread_id) {
        // Each thread works on a non-overlapping range to avoid false cycle detection
        int start = thread_id * ops_per_thread;
        
        for (int i = 0; i < ops_per_thread && (start + i + 1) < num_transactions; i++) {
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
    
    // Verify expected number of dependencies were added
    // Each thread adds ops_per_thread dependencies (or close to it)
    int expected_min = num_threads * (ops_per_thread - 1);
    assert(success_count.load() >= expected_min);
    
    // Verify the tracker state is consistent
    assert(tracker.getDependencyCount() == static_cast<size_t>(success_count.load()));
    
    tracker.clear();
}

TEST(elr_concurrent_contention) {
    // This test verifies thread safety under contention
    // Multiple threads try to register dependencies on SHARED transactions
    DependencyTracker tracker;
    const int num_threads = 8;
    const int ops_per_thread = 50;
    
    std::atomic<int> success_count{0};
    std::atomic<int> cycle_detected_count{0};
    std::atomic<int> total_ops{0};
    
    // Create a pool of transactions that all threads will compete for
    const int num_shared_txns = 20;
    for (int i = 0; i < num_shared_txns; i++) {
        tracker.registerTransaction(i);
    }
    
    auto worker = [&](int thread_id) {
        for (int op = 0; op < ops_per_thread; op++) {
            // Randomly pick two different transactions
            int txn1 = (thread_id + op) % num_shared_txns;
            int txn2 = (thread_id + op + 1 + (op % 3)) % num_shared_txns;
            
            if (txn1 == txn2) {
                txn2 = (txn2 + 1) % num_shared_txns;
            }
            
            ELRKey key{0, 1, "shared_key_" + std::to_string(op)};
            
            // Try to add dependency - may fail due to cycle detection
            if (tracker.addDependency(txn1, txn2, key, op)) {
                success_count.fetch_add(1);
            } else {
                // Cycle was detected - this is expected behavior
                cycle_detected_count.fetch_add(1);
            }
            total_ops.fetch_add(1);
        }
    };
    
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker, i);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // All operations should have completed
    assert(total_ops.load() == num_threads * ops_per_thread);
    
    // Some dependencies should have succeeded
    assert(success_count.load() > 0);
    
    // The sum of successes and cycle detections should equal total ops
    assert(success_count.load() + cycle_detected_count.load() == total_ops.load());
    
    // Verify no data corruption - dependency count should match success count
    // (accounting for possible duplicate dependencies on same txn pair)
    assert(tracker.getDependencyCount() <= static_cast<size_t>(success_count.load()));
    
    // Verify no cycles exist in the final graph
    for (int i = 0; i < num_shared_txns; i++) {
        for (int j = 0; j < num_shared_txns; j++) {
            if (i != j) {
                auto deps_i = tracker.getDependencies(i);
                auto deps_j = tracker.getDependencies(j);
                // If i depends on j, j should not depend on i (no cycle)
                if (deps_i.count(j) > 0) {
                    assert(deps_j.count(i) == 0);
                }
            }
        }
    }
    
    tracker.clear();
}

TEST(elr_concurrent_commit_abort) {
    // Test concurrent commits and aborts don't corrupt state
    DependencyTracker tracker;
    const int num_threads = 4;
    const int txns_per_thread = 25;
    const int total_txns = num_threads * txns_per_thread;
    
    std::atomic<int> commits{0};
    std::atomic<int> aborts{0};
    
    // Register all transactions
    for (int i = 0; i < total_txns; i++) {
        tracker.registerTransaction(i);
    }
    
    // Create some dependencies first (single-threaded to avoid complexity)
    for (int i = 1; i < total_txns; i += 2) {
        ELRKey key{0, 1, "key" + std::to_string(i)};
        tracker.addDependency(i, i - 1, key, i);
    }
    
    size_t initial_deps = tracker.getDependencyCount();
    assert(initial_deps > 0);
    
    auto worker = [&](int thread_id) {
        int start = thread_id * txns_per_thread;
        
        for (int i = 0; i < txns_per_thread; i++) {
            int txn_id = start + i;
            
            // Alternate between commit and abort
            if (i % 2 == 0) {
                tracker.markCommitted(txn_id);
                commits.fetch_add(1);
            } else {
                tracker.markAborted(txn_id);
                aborts.fetch_add(1);
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
    
    // All transactions should have been processed
    assert(commits.load() + aborts.load() == total_txns);
    
    // Verify we can still query the tracker without crashing
    for (int i = 0; i < total_txns; i++) {
        // These should not crash or hang
        tracker.canCommit(i);
        tracker.getChainDepth(i);
    }
    
    tracker.clear();
}

// ============================================================================
// Stress Tests (Intensive)
// ============================================================================

TEST(stress_dependency_tracker_high_volume) {
    // High volume test: 10,000 transactions, 16 threads
    DependencyTracker tracker;
    const int num_transactions = 10000;
    const int num_threads = 16;
    const int ops_per_thread = num_transactions / num_threads;
    
    std::atomic<int> success_count{0};
    std::atomic<int> failure_count{0};
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Register all transactions
    for (int i = 0; i < num_transactions; i++) {
        tracker.registerTransaction(i);
    }
    
    auto worker = [&](int thread_id) {
        int start = thread_id * ops_per_thread;
        int end = std::min(start + ops_per_thread, num_transactions - 1);
        
        for (int i = start; i < end; i++) {
            ELRKey key{0, 1, "key" + std::to_string(i)};
            if (tracker.addDependency(i + 1, i, key, i)) {
                success_count.fetch_add(1);
            } else {
                failure_count.fetch_add(1);
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
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "[" << success_count.load() << " deps, " << duration.count() << "ms] ";
    
    // Most operations should succeed (linear chain has no cycles)
    assert(success_count.load() > num_transactions / 2);
    
    // Verify data structure integrity
    assert(tracker.getActiveTransactionCount() == static_cast<size_t>(num_transactions));
    
    tracker.clear();
}

TEST(stress_contention_heavy) {
    // Heavy contention: many threads fighting over few transactions
    DependencyTracker tracker;
    const int num_shared_txns = 50;  // Small pool = high contention
    const int num_threads = 32;
    const int ops_per_thread = 500;
    
    std::atomic<int> total_ops{0};
    std::atomic<int> successes{0};
    std::atomic<int> cycles{0};
    
    for (int i = 0; i < num_shared_txns; i++) {
        tracker.registerTransaction(i);
    }
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    auto worker = [&](int thread_id) {
        // Use thread_id to create some variation in access patterns
        unsigned int seed = thread_id * 12345;
        
        for (int op = 0; op < ops_per_thread; op++) {
            // Simple pseudo-random selection
            seed = seed * 1103515245 + 12345;
            int txn1 = (seed >> 16) % num_shared_txns;
            seed = seed * 1103515245 + 12345;
            int txn2 = (seed >> 16) % num_shared_txns;
            
            if (txn1 == txn2) {
                txn2 = (txn2 + 1) % num_shared_txns;
            }
            
            ELRKey key{0, 1, "k" + std::to_string(op % 100)};
            
            if (tracker.addDependency(txn1, txn2, key, op)) {
                successes.fetch_add(1);
            } else {
                cycles.fetch_add(1);
            }
            total_ops.fetch_add(1);
        }
    };
    
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker, i);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "[" << total_ops.load() << " ops, " 
              << successes.load() << " ok, "
              << cycles.load() << " cycles, "
              << duration.count() << "ms] ";
    
    // All operations should complete
    assert(total_ops.load() == num_threads * ops_per_thread);
    
    // Accounting should match
    assert(successes.load() + cycles.load() == total_ops.load());
    
    // Verify no corruption: check we can traverse the graph
    for (int i = 0; i < num_shared_txns; i++) {
        auto deps = tracker.getDependencies(i);
        auto dependents = tracker.getDependentTransactions(i);
        // Just verify these don't crash
        (void)deps;
        (void)dependents;
    }
    
    tracker.clear();
}

TEST(stress_cascade_abort_deep_chains) {
    // Test cascade abort with deep dependency chains
    DependencyTracker tracker;
    CascadeAbortHandler handler(0);
    handler.setDependencyTracker(&tracker);
    
    const int chain_length = 100;
    const int num_chains = 10;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Create multiple parallel chains
    for (int chain = 0; chain < num_chains; chain++) {
        int base = chain * chain_length;
        
        for (int i = 0; i < chain_length; i++) {
            tracker.registerTransaction(base + i);
        }
        
        // Create chain: base <- base+1 <- base+2 <- ... <- base+chain_length-1
        for (int i = 1; i < chain_length; i++) {
            ELRKey key{0, 1, "chain" + std::to_string(chain) + "_" + std::to_string(i)};
            tracker.addDependency(base + i, base + i - 1, key, i);
        }
    }
    
    // Verify chains were created
    for (int chain = 0; chain < num_chains; chain++) {
        int base = chain * chain_length;
        auto cascade_set = tracker.getCascadeAbortSet(base, 0);
        // Aborting root should cascade to all chain_length-1 dependents
        assert(cascade_set.size() == static_cast<size_t>(chain_length - 1));
    }
    
    // Now abort the root of each chain and verify cascade
    for (int chain = 0; chain < num_chains; chain++) {
        int base = chain * chain_length;
        auto op = handler.initiateCascadeAbort(base, 5000);
        assert(op.status == CascadeStatus::COMPLETED);
        assert(op.abort_list.size() == static_cast<size_t>(chain_length - 1));
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "[" << num_chains << " chains x " << chain_length << " depth, "
              << duration.count() << "ms] ";
    
    tracker.clear();
}

TEST(stress_elr_manager_throughput) {
    // Measure ELR manager throughput under load
    auto& manager = ELRManager::getInstance(10);
    
    ELRConfig config;
    config.enabled = true;
    config.max_dependency_chain = 100;
    
    manager.initialize(10, config);
    
    const int num_transactions = 5000;
    const int num_threads = 8;
    const int txns_per_thread = num_transactions / num_threads;
    
    std::atomic<int> early_releases{0};
    std::atomic<int> dependencies{0};
    std::atomic<int> commits{0};
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    auto worker = [&](int thread_id) {
        int start = thread_id * txns_per_thread;
        
        for (int i = 0; i < txns_per_thread; i++) {
            txnid_t txn_id = start + i + 1000; // Offset to avoid conflicts with other tests
            
            manager.beginTransaction(txn_id);
            
            // Early release a key
            ELRKey key{10, 1, "throughput_key_" + std::to_string(txn_id)};
            auto result = manager.earlyRelease(txn_id, {key});
            if (result.success) {
                early_releases.fetch_add(1);
            }
            
            // If not the first in thread's range, create dependency on previous
            if (i > 0) {
                txnid_t prev_txn = start + i - 1 + 1000;
                ELRKey dep_key{10, 1, "dep_key_" + std::to_string(txn_id)};
                if (manager.registerELRRead(txn_id, prev_txn, dep_key, i)) {
                    dependencies.fetch_add(1);
                }
            }
            
            // Commit
            manager.commitTransaction(txn_id);
            commits.fetch_add(1);
        }
    };
    
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker, i);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    double txns_per_sec = (commits.load() * 1000.0) / std::max(1L, (long)duration.count());
    
    std::cout << "[" << commits.load() << " commits, "
              << early_releases.load() << " releases, "
              << std::fixed << std::setprecision(0) << txns_per_sec << " txn/s] ";
    
    assert(commits.load() == num_transactions);
    assert(early_releases.load() > 0);
    
    manager.shutdown();
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
    RUN_TEST(elr_manager_chain_depth_limit);
    RUN_TEST(elr_manager_disabled);
    
    std::cout << std::endl << "--- Cascade Abort Handler Tests ---" << std::endl;
    RUN_TEST(cascade_abort_handler);
    
    std::cout << std::endl << "--- ELR Log Tests ---" << std::endl;
    RUN_TEST(elr_log_basic);
    RUN_TEST(elr_log_abort_tracking);
    RUN_TEST(elr_log_serialization);
    RUN_TEST(elr_log_recovery);
    
    std::cout << std::endl << "--- Multi-threaded Tests ---" << std::endl;
    RUN_TEST(elr_concurrent_dependency_tracking);
    RUN_TEST(elr_concurrent_contention);
    RUN_TEST(elr_concurrent_commit_abort);
    
    std::cout << std::endl << "--- Stress Tests (Intensive) ---" << std::endl;
    RUN_TEST(stress_dependency_tracker_high_volume);
    RUN_TEST(stress_contention_heavy);
    RUN_TEST(stress_cascade_abort_deep_chains);
    RUN_TEST(stress_elr_manager_throughput);
    
    std::cout << std::endl;
    std::cout << "=== All ELR Tests Passed! ===" << std::endl;
    
    return 0;
}
