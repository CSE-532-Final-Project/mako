//
// Created by shuai on 11/25/15.
//

#include "../__dep__.h"
#include "../constants.h"
#include "../tx.h"
#include "../scheduler.h"
#include "scheduler.h"
#include "tx.h"
#include <algorithm>

namespace janus {

Scheduler2pl::Scheduler2pl() : SchedulerClassic() {
  mdb_txn_mgr_ = make_shared<mdb::TxnMgr2PL>();
}

void Scheduler2pl::CleanupExpiredHolders(ALock* lock) {
  auto it = lock_table_.find(lock);
  if (it == lock_table_.end()) {
    return;
  }
  auto &holders = it->second;
  holders.erase(std::remove_if(holders.begin(),
                               holders.end(),
                               [](const LockHolder &holder) {
                                 return holder.tx.expired();
                               }),
                holders.end());
  if (holders.empty()) {
    lock_table_.erase(it);
  }
}

void Scheduler2pl::RegisterLockHolder(ALock* lock,
                                      const std::shared_ptr<Tx2pl>& tx,
                                      uint64_t req_id,
                                      bool physical) {
  if (!lock || !tx) {
    return;
  }
  std::lock_guard<std::mutex> guard(lock_table_mutex_);
  auto &holders = lock_table_[lock];
  holders.push_back(LockHolder{tx, physical, req_id});
}

void Scheduler2pl::RemoveLockHolder(ALock* lock, txnid_t tx_id, bool physical) {
  if (!lock) {
    return;
  }
  std::lock_guard<std::mutex> guard(lock_table_mutex_);
  auto it = lock_table_.find(lock);
  if (it == lock_table_.end()) {
    return;
  }
  auto &holders = it->second;
  holders.erase(std::remove_if(holders.begin(),
                               holders.end(),
                               [&](const LockHolder &holder) {
                                 auto sp = holder.tx.lock();
                                 if (!sp) {
                                   return true;
                                 }
                                 if (sp->tid_ == tx_id && holder.physical == physical) {
                                   return true;
                                 }
                                 return false;
                               }),
                holders.end());
  if (holders.empty()) {
    lock_table_.erase(it);
  }
}

bool Scheduler2pl::CanViolate(ALock* lock,
                              const std::shared_ptr<Tx2pl>& requesting_tx,
                              std::vector<std::shared_ptr<Tx2pl>>* blockers) {
  if (!lock || !requesting_tx || blockers == nullptr) {
    return false;
  }
  std::lock_guard<std::mutex> guard(lock_table_mutex_);
  auto it = lock_table_.find(lock);
  if (it == lock_table_.end()) {
    return false;
  }
  auto &holders = it->second;
  bool cleanup_needed = false;
  bool allowed = true;
  blockers->clear();
  for (const auto &holder : holders) {
    auto sp = holder.tx.lock();
    if (!sp) {
      cleanup_needed = true;
      continue;
    }
    if (sp->tid_ == requesting_tx->tid_) {
      allowed = false;
      break;
    }
    if (!sp->HasCommitRecord()) {
      allowed = false;
      break;
    }
    blockers->push_back(sp);
  }
  if (cleanup_needed) {
    auto &vec = holders;
    vec.erase(std::remove_if(vec.begin(),
                             vec.end(),
                             [](const LockHolder &entry) {
                               return entry.tx.expired();
                             }),
              vec.end());
    if (vec.empty()) {
      lock_table_.erase(it);
    }
  }
  if (!allowed || blockers->empty()) {
    blockers->clear();
    return false;
  }
  return true;
}

mdb::Txn* Scheduler2pl::del_mdb_txn(const i64 tid) {
  mdb::Txn *txn = NULL;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    verify(0);
  } else {
    txn = it->second;
  }
  mdb_txns_.erase(it);
  return txn;
}

mdb::Txn* Scheduler2pl::get_mdb_txn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    txn = mdb_txn_mgr_->start(tid);
    //XXX using occ lazy mode: increment version at commit time
    auto ret = mdb_txns_.insert(std::pair<i64, mdb::Txn *>(tid, txn));
    verify(ret.second);
  } else {
    txn = it->second;
  }

  verify(mdb_txn_mgr_->rtti() == mdb::symbol_t::TXN_2PL);
  verify(txn->rtti() == mdb::symbol_t::TXN_2PL);
  verify(txn != nullptr);
  return txn;
}


bool Scheduler2pl::Guard(Tx &tx_box, Row *row, int col_idx, bool write) {
  mdb::FineLockedRow* fl_row = (mdb::FineLockedRow*) row;
  ALock* lock = fl_row->get_alock(col_idx);
  auto sp_tx = dynamic_pointer_cast<Tx2pl>(tx_box.shared_from_this());
  verify(!sp_tx->aborted_);
  verify(!sp_tx->committed_);
  if (sp_tx->wounded_) {
    return false;
  }
  std::vector<std::shared_ptr<Tx2pl>> blockers;
  if (CanViolate(lock, sp_tx, &blockers)) {
    uint64_t dependency_lsn = 0;
    for (auto &blocker : blockers) {
      dependency_lsn = std::max(dependency_lsn, blocker->commit_lsn());
      blocker->AddDependent(sp_tx);
    }
    if (dependency_lsn > 0) {
      sp_tx->RecordDependencyLsn(dependency_lsn);
    }
    RegisterLockHolder(lock, sp_tx, 0 /*req_id*/, false /*physical*/);
    sp_tx->violated_locks_.push_back(lock);
    return true;
  }
  sp_tx->_debug_n_lock_requested_++;
  uint64_t lock_req_id = lock->Lock(0, ALock::WLOCK, tx_box.tid_, [sp_tx]()->int{
    if (sp_tx->woundable_) {
      sp_tx->wounded_ = true;
      return 0;
    } else {
      return 1;
    }
  });
  verify(!sp_tx->committed_);
  if (lock_req_id > 0) {
    sp_tx->_debug_n_lock_granted_++;
    if (sp_tx->aborted_) {
      lock->abort(lock_req_id);
      return false;
    } else {
      RegisterLockHolder(lock, sp_tx, lock_req_id, true);
      sp_tx->locked_locks_.emplace_back(lock, lock_req_id);
      return true;
    }
  } else {
    return false;
  }
}

bool Scheduler2pl::DoPrepare(txnid_t tx_id) {
  // do nothing here?
  auto tx_box = dynamic_pointer_cast<Tx2pl>(GetOrCreateTx(tx_id));
  verify(!tx_box->inuse);
  verify(tx_box->_debug_n_lock_granted_ == tx_box->_debug_n_lock_requested_);
  tx_box->inuse = true;
  bool ret = true;
  if (tx_box->wounded_) {
		Log_info("wounded???");
    return false;
  }
  tx_box->woundable_ = false;
  tx_box->inuse = false;
  return ret;
}

void Scheduler2pl::DoCommit(Tx& tx_box) {
  Tx2pl& tpl_tx_box = dynamic_cast<Tx2pl&>(tx_box);
  for (auto& pair : tpl_tx_box.locked_locks_) {
    pair.first->abort(pair.second);
    RemoveLockHolder(pair.first, tpl_tx_box.tid_, true);
  }
  for (auto* lock : tpl_tx_box.violated_locks_) {
    RemoveLockHolder(lock, tpl_tx_box.tid_, false);
  }
  tpl_tx_box.committed_ = true;
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->commit();
  auto t = dynamic_pointer_cast<Tx2pl>(GetOrCreateTx(tx_box.tid_));
  tx_box.mdb_txn_ = nullptr;
  delete mdb_txn;
  tx_box.ClearDependents();
}

void Scheduler2pl::DoAbort(Tx& tx_box) {
  Tx2pl& tpl_tx_box = dynamic_cast<Tx2pl&>(tx_box);
  tpl_tx_box.aborted_ = true;
  for (auto& pair : tpl_tx_box.locked_locks_) {
    pair.first->abort(pair.second);
    RemoveLockHolder(pair.first, tpl_tx_box.tid_, true);
  }
  for (auto* lock : tpl_tx_box.violated_locks_) {
    RemoveLockHolder(lock, tpl_tx_box.tid_, false);
  }
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->abort();
  delete mdb_txn;
  CascadeAbort(tx_box);
}

} // namespace janus
