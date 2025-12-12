/**
 * @file dependency_tracker.cc
 * @brief Implementation of dependency tracking for ELR
 */

#include "dependency_tracker.h"
#include <sstream>
#include <algorithm>
#include <chrono>

namespace mako {
namespace elr {

// ============================================================================
// DependencyTracker Implementation
// ============================================================================

DependencyTracker::DependencyTracker() {}

DependencyTracker::~DependencyTracker() {
    clear();
}

void DependencyTracker::registerTransaction(txnid_t txn_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (nodes_.find(txn_id) == nodes_.end()) {
        nodes_[txn_id] = std::make_unique<DependencyNode>(txn_id);
    }
}

void DependencyTracker::unregisterTransaction(txnid_t txn_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = nodes_.find(txn_id);
    if (it == nodes_.end()) return;
    
    DependencyNode* node = it->second.get();
    
    // Remove this transaction from all dependencies
    for (txnid_t dep_id : node->depends_on) {
        auto dep_it = nodes_.find(dep_id);
        if (dep_it != nodes_.end()) {
            dep_it->second->depended_by.erase(txn_id);
            dependency_count_.fetch_sub(1);
        }
    }
    
    // Remove this transaction from all dependents
    for (txnid_t dependent_id : node->depended_by) {
        auto dep_it = nodes_.find(dependent_id);
        if (dep_it != nodes_.end()) {
            dep_it->second->depends_on.erase(txn_id);
            dependency_count_.fetch_sub(1);
        }
    }
    
    nodes_.erase(it);
}

DependencyNode* DependencyTracker::getOrCreateNode(txnid_t txn_id) {
    auto it = nodes_.find(txn_id);
    if (it == nodes_.end()) {
        nodes_[txn_id] = std::make_unique<DependencyNode>(txn_id);
        return nodes_[txn_id].get();
    }
    return it->second.get();
}

DependencyNode* DependencyTracker::getNode(txnid_t txn_id) const {
    auto it = nodes_.find(txn_id);
    if (it == nodes_.end()) return nullptr;
    return it->second.get();
}

bool DependencyTracker::addDependency(txnid_t reader_txn, txnid_t writer_txn,
                                       const ELRKey& key, tid_t version,
                                       DependencyType type) {
    if (reader_txn == writer_txn) return false;
    
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // Check for cycle before adding
    if (hasCycle(reader_txn, writer_txn)) {
        return false;
    }
    
    DependencyNode* reader_node = getOrCreateNode(reader_txn);
    DependencyNode* writer_node = getOrCreateNode(writer_txn);
    
    // Check if dependency already exists
    if (reader_node->depends_on.count(writer_txn) > 0) {
        // Dependency already exists, just add the detailed info
        ELRDependency dep;
        dep.reader_txn = reader_txn;
        dep.writer_txn = writer_txn;
        dep.key = key;
        dep.version = version;
        dep.type = type;
        dep.writer_committed = (writer_node->status == DependencyNode::Status::COMMITTED);
        dep.reader_committed = (reader_node->status == DependencyNode::Status::COMMITTED);
        dep.create_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        reader_node->dependencies.push_back(dep);
        return true;
    }
    
    // Add the dependency
    reader_node->depends_on.insert(writer_txn);
    writer_node->depended_by.insert(reader_txn);
    dependency_count_.fetch_add(1);
    
    // Create detailed dependency info
    ELRDependency dep;
    dep.reader_txn = reader_txn;
    dep.writer_txn = writer_txn;
    dep.key = key;
    dep.version = version;
    dep.type = type;
    dep.writer_committed = false;
    dep.reader_committed = false;
    dep.create_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    reader_node->dependencies.push_back(dep);
    
    // Update chain depths
    updateChainDepths(reader_txn);
    
    // Update stats
    ELRConfigManager::getInstance().getStats().dependencies_created.fetch_add(1);
    
    return true;
}

void DependencyTracker::removeDependency(txnid_t reader_txn, txnid_t writer_txn) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    DependencyNode* reader_node = getNode(reader_txn);
    DependencyNode* writer_node = getNode(writer_txn);
    
    if (reader_node && reader_node->depends_on.erase(writer_txn) > 0) {
        dependency_count_.fetch_sub(1);
        
        // Mark dependencies as resolved
        for (auto& dep : reader_node->dependencies) {
            if (dep.writer_txn == writer_txn) {
                dep.writer_committed = true;
            }
        }
        
        ELRConfigManager::getInstance().getStats().dependencies_resolved.fetch_add(1);
    }
    
    if (writer_node) {
        writer_node->depended_by.erase(reader_txn);
    }
}

bool DependencyTracker::wouldCreateCycle(txnid_t reader_txn, txnid_t writer_txn) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return hasCycle(reader_txn, writer_txn);
}

bool DependencyTracker::hasCycle(txnid_t from, txnid_t to) const {
    // Check if 'from' is reachable from 'to' (which would create a cycle)
    std::unordered_set<txnid_t> visited;
    std::queue<txnid_t> queue;
    queue.push(to);
    
    while (!queue.empty()) {
        txnid_t current = queue.front();
        queue.pop();
        
        if (current == from) {
            return true; // Cycle detected
        }
        
        if (visited.count(current) > 0) continue;
        visited.insert(current);
        
        DependencyNode* node = getNode(current);
        if (node) {
            for (txnid_t dep : node->depends_on) {
                if (visited.count(dep) == 0) {
                    queue.push(dep);
                }
            }
        }
    }
    
    return false;
}

void DependencyTracker::updateChainDepths(txnid_t start_txn) {
    // BFS to update chain depths
    std::queue<txnid_t> queue;
    queue.push(start_txn);
    
    while (!queue.empty()) {
        txnid_t current = queue.front();
        queue.pop();
        
        DependencyNode* node = getNode(current);
        if (!node) continue;
        
        // Calculate new depth as max of dependencies + 1
        uint32_t new_depth = 0;
        for (txnid_t dep : node->depends_on) {
            DependencyNode* dep_node = getNode(dep);
            if (dep_node) {
                new_depth = std::max(new_depth, dep_node->chain_depth + 1);
            }
        }
        
        if (new_depth != node->chain_depth) {
            node->chain_depth = new_depth;
            // Propagate to dependents
            for (txnid_t dependent : node->depended_by) {
                queue.push(dependent);
            }
        }
    }
}

std::unordered_set<txnid_t> DependencyTracker::getDependentTransactions(txnid_t txn_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    DependencyNode* node = getNode(txn_id);
    if (!node) return {};
    return node->depended_by;
}

std::unordered_set<txnid_t> DependencyTracker::getDependencies(txnid_t txn_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    DependencyNode* node = getNode(txn_id);
    if (!node) return {};
    return node->depends_on;
}

std::vector<txnid_t> DependencyTracker::getCascadeAbortSet(txnid_t txn_id, uint32_t max_depth) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<txnid_t> result;
    std::unordered_set<txnid_t> visited;
    std::queue<std::pair<txnid_t, uint32_t>> queue; // (txn_id, depth)
    
    // Start with all direct dependents
    DependencyNode* start_node = getNode(txn_id);
    if (!start_node) return result;
    
    for (txnid_t dependent : start_node->depended_by) {
        queue.push({dependent, 1});
    }
    
    while (!queue.empty()) {
        auto [current, depth] = queue.front();
        queue.pop();
        
        if (visited.count(current) > 0) continue;
        if (max_depth > 0 && depth > max_depth) continue;
        
        visited.insert(current);
        result.push_back(current);
        
        DependencyNode* node = getNode(current);
        if (node) {
            for (txnid_t dependent : node->depended_by) {
                if (visited.count(dependent) == 0) {
                    queue.push({dependent, depth + 1});
                }
            }
        }
    }
    
    return result;
}

bool DependencyTracker::canCommit(txnid_t txn_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    DependencyNode* node = getNode(txn_id);
    if (!node) return true; // Unknown transaction can commit
    
    // Check that all dependencies have committed
    for (txnid_t dep : node->depends_on) {
        DependencyNode* dep_node = getNode(dep);
        if (dep_node && dep_node->status != DependencyNode::Status::COMMITTED) {
            return false;
        }
    }
    
    return true;
}

void DependencyTracker::markCommitted(txnid_t txn_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    DependencyNode* node = getNode(txn_id);
    if (node) {
        node->status = DependencyNode::Status::COMMITTED;
        
        // Update all dependencies to reflect commit
        for (auto& dep : node->dependencies) {
            dep.reader_committed = true;
        }
        
        // Notify dependents that we committed
        for (txnid_t dependent : node->depended_by) {
            DependencyNode* dep_node = getNode(dependent);
            if (dep_node) {
                for (auto& dep : dep_node->dependencies) {
                    if (dep.writer_txn == txn_id) {
                        dep.writer_committed = true;
                    }
                }
            }
        }
    }
    
    ELRConfigManager::getInstance().getStats().successful_commits.fetch_add(1);
}

void DependencyTracker::markAborted(txnid_t txn_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    DependencyNode* node = getNode(txn_id);
    if (node) {
        node->status = DependencyNode::Status::ABORTED;
    }
}

uint32_t DependencyTracker::getChainDepth(txnid_t txn_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    DependencyNode* node = getNode(txn_id);
    return node ? node->chain_depth : 0;
}

bool DependencyTracker::isChainTooDeep(txnid_t txn_id, uint32_t max_depth) {
    return getChainDepth(txn_id) > max_depth;
}

std::vector<ELRDependency> DependencyTracker::getDetailedDependencies(txnid_t txn_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    DependencyNode* node = getNode(txn_id);
    if (!node) return {};
    return node->dependencies;
}

size_t DependencyTracker::getActiveTransactionCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return nodes_.size();
}

size_t DependencyTracker::getDependencyCount() const {
    return dependency_count_.load();
}

void DependencyTracker::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    nodes_.clear();
    dependency_count_.store(0);
}

std::string DependencyTracker::dumpGraph() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::ostringstream oss;
    oss << "Dependency Graph (" << nodes_.size() << " nodes, "
        << dependency_count_.load() << " dependencies):\n";
    
    for (const auto& [txn_id, node] : nodes_) {
        oss << "  TXN " << txn_id << " [depth=" << node->chain_depth << "]:\n";
        
        if (!node->depends_on.empty()) {
            oss << "    depends on: ";
            for (txnid_t dep : node->depends_on) {
                oss << dep << " ";
            }
            oss << "\n";
        }
        
        if (!node->depended_by.empty()) {
            oss << "    depended by: ";
            for (txnid_t dep : node->depended_by) {
                oss << dep << " ";
            }
            oss << "\n";
        }
    }
    
    return oss.str();
}

// ============================================================================
// ShardDependencyTracker Implementation
// ============================================================================

ShardDependencyTracker::ShardDependencyTracker(shardid_t shard_id)
    : shard_id_(shard_id) {}

void ShardDependencyTracker::addCrossShardDependency(txnid_t local_txn,
                                                      txnid_t remote_txn,
                                                      shardid_t remote_shard) {
    std::lock_guard<std::mutex> lock(cross_shard_mutex_);
    cross_shard_deps_[local_txn].push_back({remote_txn, remote_shard});
}

std::vector<std::pair<txnid_t, shardid_t>>
ShardDependencyTracker::getCrossShardDependencies(txnid_t txn_id) {
    std::lock_guard<std::mutex> lock(cross_shard_mutex_);
    auto it = cross_shard_deps_.find(txn_id);
    if (it == cross_shard_deps_.end()) return {};
    return it->second;
}

void ShardDependencyTracker::notifyRemoteCommit(txnid_t remote_txn,
                                                 shardid_t remote_shard) {
    std::lock_guard<std::mutex> lock(cross_shard_mutex_);
    
    // Find and update all local transactions that depend on this remote txn
    for (auto& [local_txn, deps] : cross_shard_deps_) {
        deps.erase(
            std::remove_if(deps.begin(), deps.end(),
                [remote_txn, remote_shard](const auto& dep) {
                    return dep.first == remote_txn && dep.second == remote_shard;
                }),
            deps.end()
        );
    }
}

void ShardDependencyTracker::notifyRemoteAbort(txnid_t remote_txn,
                                                shardid_t remote_shard) {
    // When a remote transaction aborts, we need to cascade abort
    // all local transactions that depend on it.
    // This is handled by the ELRManager.
    std::lock_guard<std::mutex> lock(cross_shard_mutex_);
    
    // Find all local transactions that depend on this remote txn
    std::vector<txnid_t> to_abort;
    for (const auto& [local_txn, deps] : cross_shard_deps_) {
        for (const auto& [rtxn, rshard] : deps) {
            if (rtxn == remote_txn && rshard == remote_shard) {
                to_abort.push_back(local_txn);
                break;
            }
        }
    }
    
    // Mark these for cascade abort in the local tracker
    for (txnid_t txn_id : to_abort) {
        local_tracker_.markAborted(txn_id);
    }
}

} // namespace elr
} // namespace mako
