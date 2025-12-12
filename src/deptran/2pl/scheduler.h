//
// Created by shuai on 11/25/15.
//

#pragma once

#include "deptran/classic/scheduler.h"
#include <mutex>
#include <unordered_map>

namespace janus {

class Executor;
class Tx2pl;
class Scheduler2pl: public SchedulerClassic {
 public:
  Scheduler2pl();

  virtual mdb::Txn *get_mdb_txn(const i64 tid);
  virtual mdb::Txn *del_mdb_txn(const i64 tid);

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) override {
    verify(0);
  };


  virtual bool DispatchPiece(Tx& tx,
                             SimpleCommand& cmd,
                             TxnOutput& ret_output) override {
    if (tx.aborted_) {
      return false;
    } else {
      auto ret = SchedulerClassic::DispatchPiece(tx, cmd, ret_output);
      if (!ret) {
        tx.aborted_ = true;
        return false;
      }
      ExecutePiece(tx, cmd, ret_output);
      return true;
    }
  }

  virtual bool Guard(Tx &tx_box, Row *row, int col_idx, bool write) override;

  virtual bool DoPrepare(txnid_t tx_id) override;

  virtual void DoCommit(Tx& tx_box) override;

  virtual void DoAbort(Tx& tx_box) override;

 private:
  struct LockHolder {
    std::weak_ptr<Tx2pl> tx;
    bool physical{false};
    uint64_t req_id{0};
  };

  std::unordered_map<ALock*, std::vector<LockHolder>> lock_table_;
  std::mutex lock_table_mutex_;

  void RegisterLockHolder(ALock* lock,
                          const std::shared_ptr<Tx2pl>& tx,
                          uint64_t req_id,
                          bool physical);
  void RemoveLockHolder(ALock* lock, txnid_t tx_id, bool physical);
  bool CanViolate(ALock* lock,
                  const std::shared_ptr<Tx2pl>& requesting_tx,
                  std::vector<std::shared_ptr<Tx2pl>>* blockers);
  void CleanupExpiredHolders(ALock* lock);
};

} // namespace janus
