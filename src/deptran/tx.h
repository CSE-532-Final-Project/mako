/**
 * This class is currently mixed with three kinds of information:
 *   1. concurrency control metadata.
 *   2. database transaction logic and workspace
 *   3. temporary workspace for database transactions (version, temp values..)
 *   4. database interface
 * Shall we split the three into three classes? Or is this fine?
 */

#pragma once

#include "__dep__.h"
#include "rcc/row.h"
#include "snow/ro6_row.h"
#include "multi_value.h"
#include "txn_reg.h"
#include "procedure.h"
#include <atomic>
#include <functional>
#include <mutex>
#include <vector>

namespace janus {

using mdb::Row;
using mdb::Table;
using mdb::colid_t;

#define IS_MODE_RCC (Config::GetConfig()->get_mode() == MODE_RCC)
#define IS_MODE_RO6 (Config::GetConfig()->get_mode() == MODE_RO6)
#define IS_MODE_2PL (Config::GetConfig()->get_mode() == MODE_2PL)
#define IS_MODE_OCC (Config::GetConfig()->get_mode() == MODE_OCC)
#define IS_MODE_NONE (Config::GetConfig()->get_mode() == MODE_NONE)

class TxLogServer;

/**
 * This is the data structure storing information used by
 * concurrency control; it does not store the transaction
 * (stored procedure) logic.
 * It now contains a workspace for procedure data as a
 * temporary solution.
 */
class Tx: public enable_shared_from_this<Tx> {
 public:
  shared_ptr<IntEvent> fully_dispatched_{Reactor::CreateSpEvent<IntEvent>()};
//  bool fully_dispatched_{false};
  shared_ptr<IntEvent> ev_execute_ready_{Reactor::CreateSpEvent<IntEvent>()};
  bool aborted_in_dispatch_{false};
  bool inuse = false;
  txnid_t tid_;
  epoch_t epoch_{0};
  TxLogServer *sched_{nullptr};
  int phase_;
  mdb::Txn *mdb_txn_{nullptr};
  Recorder *recorder_{nullptr};
  weak_ptr<TxnRegistry> txn_reg_{};
  TxWorkspace ws_{};
  // TODO at most one active coroutine runnable for a tx at a time
//  IntEvent running_{};
  shared_ptr<Marshallable> cmd_{};
  shared_ptr<ViewData> sp_view_data_ = nullptr;


#ifdef CHECK_ISO
  map<Row*, map<colid_t, int>> deltas_;
#endif

  bool committed_{false};
  bool aborted_{false};

  enum class CommitState : uint8_t {
    ACTIVE = 0,
    COMMIT_RECORD_STAGED,
    DURABLE
  };

  std::atomic<CommitState> commit_state_{CommitState::ACTIVE};
  std::atomic<uint64_t> commit_lsn_{0};
  std::atomic<uint64_t> durable_lsn_{0};
  std::atomic<uint64_t> required_commit_lsn_{0};
  mutable std::mutex commit_state_mutex_;
  std::vector<std::function<void(uint64_t)>> commit_lsn_waiters_{};
  mutable std::mutex dependency_mutex_;
  std::vector<std::weak_ptr<Tx>> dependents_{};

  map<innid_t, std::unique_ptr<IntEvent>> paused_pieces_{};
  map<int64_t, mdb::Row *> context_row_;
  map<int64_t, Value> context_value_;
  map<int64_t, mdb::ResultSet> context_rs_;

  Tx() = delete;

  Tx(epoch_t epoch, txnid_t tid, TxLogServer *mgr)
      : epoch_(epoch),
        tid_(tid),
        sched_(mgr),
        phase_(0),
        context_row_(),
        context_value_(),
        context_rs_(),
        mdb_txn_(nullptr) {
  }

  mdb::Txn *mdb_txn();

  virtual mdb::Row *CreateRow(const mdb::Schema *schema,
                              const std::vector<mdb::Value> &values);

  virtual bool ReadColumn(mdb::Row *row,
                          mdb::colid_t col_id,
                          Value *value,
                          int hint_flag = TXN_INSTANT);

  virtual bool ReadColumns(Row *row,
                           const std::vector<colid_t> &col_ids,
                           std::vector<Value> *values,
                           int hint_flag = TXN_INSTANT);

  virtual bool WriteColumn(Row *row,
                           colid_t col_id,
                           const Value &value,
                           int hint_flag = TXN_INSTANT);

  virtual bool WriteColumns(Row *row,
                            const std::vector<colid_t> &col_ids,
                            const std::vector<Value> &values,
                            int hint_flag = TXN_INSTANT);

  virtual bool InsertRow(Table *tbl, Row *row);

  virtual mdb::Row *Query(mdb::Table *tbl,
                          const mdb::MultiBlob &mb,
                          int64_t row_context_id = 0);

  virtual mdb::Row *Query(mdb::Table *tbl,
                          vector<Value> &primary_keys,
                          int64_t row_context_id = 0);

  virtual mdb::ResultSet QueryIn(Table *tbl,
                                 const mdb::MultiBlob &low,
                                 const mdb::MultiBlob &high,
                                 mdb::symbol_t order = mdb::symbol_t::ORD_ASC,
                                 int rs_context_id = 0);

  virtual mdb::Table *GetTable(const std::string &tbl_name) const;

  CommitState commit_state() const { return commit_state_.load(); }
  bool HasCommitRecord() const {
    auto state = commit_state_.load();
    return state == CommitState::COMMIT_RECORD_STAGED ||
           state == CommitState::DURABLE;
  }
  bool IsDurable() const { return commit_state_.load() == CommitState::DURABLE; }
  uint64_t commit_lsn() const { return commit_lsn_.load(); }
  uint64_t durable_lsn() const { return durable_lsn_.load(); }
  uint64_t required_commit_lsn() const { return required_commit_lsn_.load(); }
  void MarkCommitRecordStaged(uint64_t lsn);
  void MarkDurable(uint64_t lsn);
  void RegisterCommitLsnCallback(const std::function<void(uint64_t)> &cb);
  void RecordDependencyLsn(uint64_t lsn);
  void AddDependent(const std::shared_ptr<Tx>& dependent);
  std::vector<std::shared_ptr<Tx>> TakeDependents();
  void ClearDependents();

  virtual ~Tx();
};

class entry_t {
 public:
  shared_ptr<Tx> last_{nullptr}; // last transaction(write) that touches this
  unordered_set<shared_ptr<Tx>> active_{}; // last transaction(write) that touches this
  rank_t rank_ {RANK_UNDEFINED};
  // item. (arriving order)

  const entry_t &operator=(const entry_t &rhs) {
    last_ = rhs.last_;
    return *this;
  }

  entry_t() {
  }

  entry_t(const entry_t &o) {
    last_ = o.last_;
  }
};

} // namespace janus
