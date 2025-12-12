#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <cstring>
#include <cstdlib>
#include <unordered_map>

#include "deptran/tx.h"
#include "deptran/scheduler.h"
#include "deptran/classic/tx.h"
#include "deptran/classic/scheduler.h"
#include "deptran/2pl/scheduler.h"
#include "deptran/2pl/tx.h"
#include "deptran/config.h"
#include "memdb/row.h"

using namespace janus;

namespace {

std::shared_ptr<Tx> MakeTestTx(txnid_t tid, TxLogServer* sched) {
  return std::make_shared<Tx>(0, tid, sched);
}

class TestScheduler : public SchedulerClassic {
 public:
  using SchedulerClassic::CascadeAbort;
  std::vector<txnid_t> aborted;
  std::unordered_map<txnid_t, std::shared_ptr<TxClassic>> registry;

  TestScheduler() : SchedulerClassic() {}

  bool Guard(Tx&, Row*, int, bool) override {
    return true;
  }

  void RegisterTx(const std::shared_ptr<TxClassic>& tx) {
    registry[tx->tid_] = tx;
  }

  int OnEarlyAbort(txnid_t tx_id) override {
    aborted.push_back(tx_id);
    auto it = registry.find(tx_id);
    if (it != registry.end()) {
      CascadeAbort(*it->second);
    }
    return 0;
  }
};

class TrackingScheduler2pl : public Scheduler2pl {
 public:
  using SchedulerClassic::CascadeAbort;
  std::vector<txnid_t> aborted;

  int OnEarlyAbort(txnid_t tx_id) override {
    aborted.push_back(tx_id);
    return 0;
  }
};

class TestConfig : public Config {
 public:
  TestConfig(char *ctrl_hostname,
             uint32_t ctrl_port,
             uint32_t ctrl_timeout,
             char *ctrl_key,
             char *ctrl_init,
             int32_t tot_req_num,
             int16_t n_concurrent,
             uint32_t duration,
             bool heart_beat,
             single_server_t single_server,
             const string& logging_path,
             int jetpack_fastpath_attempt_rate)
      : Config(ctrl_hostname,
               ctrl_port,
               ctrl_timeout,
               ctrl_key,
               ctrl_init,
               tot_req_num,
               n_concurrent,
               duration,
               heart_beat,
               single_server,
               logging_path,
               jetpack_fastpath_attempt_rate) {}
};

class ConfigGuard {
 public:
  ConfigGuard() {
    if (Config::config_s != nullptr) {
      return;
    }
    ctrl_host_ = strdup("localhost");
    ctrl_key_ = strdup("");
    ctrl_init_ = strdup("");
    Config::config_s = new TestConfig(ctrl_host_,
                                      0,
                                      0,
                                      ctrl_key_,
                                      ctrl_init_,
                                      0,
                                      1,
                                      1,
                                      false,
                                      Config::single_server_t::SS_DISABLED,
                                      "",
                                      0);
    Config::config_s->tx_proto_ = MODE_2PL;
  }

  ~ConfigGuard() {
    Config::DestroyConfig();
    free(ctrl_host_);
    free(ctrl_key_);
    free(ctrl_init_);
  }

 private:
  char* ctrl_host_{nullptr};
  char* ctrl_key_{nullptr};
  char* ctrl_init_{nullptr};
};

void EnsureConfig() {
  static ConfigGuard guard;
}

}  // namespace

TEST(ControlledLockViolation, RecordsMaxDependencyLsn) {
  EnsureConfig();
  TxLogServer server;
  auto tx = MakeTestTx(1, &server);
  EXPECT_EQ(tx->required_commit_lsn(), 0u);

  tx->RecordDependencyLsn(5);
  EXPECT_EQ(tx->required_commit_lsn(), 5u);

  tx->RecordDependencyLsn(3);
  EXPECT_EQ(tx->required_commit_lsn(), 5u);

  tx->RecordDependencyLsn(7);
  EXPECT_EQ(tx->required_commit_lsn(), 7u);
}

TEST(ControlledLockViolation, WaitsForDurableDependency) {
  EnsureConfig();
  TxLogServer server;
  auto dependent = MakeTestTx(2, &server);
  dependent->RecordDependencyLsn(10);

  std::atomic<bool> finished{false};
  std::thread waiter([&]() {
    server.WaitForCommitDependencies(dependent);
    finished.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_FALSE(finished.load());

  server.AdvanceDurableLsn(5);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_FALSE(finished.load());

  server.AdvanceDurableLsn(12);
  waiter.join();
  EXPECT_TRUE(finished.load());
}

TEST(ControlledLockViolation, DependentsAreTrackedAndCleared) {
  EnsureConfig();
  TxLogServer server;
  auto parent = MakeTestTx(3, &server);
  auto child = MakeTestTx(4, &server);

  parent->AddDependent(child);
  auto dependents = parent->TakeDependents();
  ASSERT_EQ(dependents.size(), 1u);
  EXPECT_EQ(dependents[0], child);

  auto empty = parent->TakeDependents();
  EXPECT_TRUE(empty.empty());

  parent->AddDependent(child);
  parent->ClearDependents();
  auto after_clear = parent->TakeDependents();
  EXPECT_TRUE(after_clear.empty());
}

TEST(ControlledLockViolation, StageCommitAssignsLsn) {
  EnsureConfig();
  TxLogServer server;
  auto tx = MakeTestTx(5, &server);

  uint64_t lsn = server.StageCommitRecord(tx);
  EXPECT_TRUE(tx->HasCommitRecord());
  EXPECT_EQ(tx->commit_lsn(), lsn);
  EXPECT_GE(server.DurableLsn(), lsn);
}

TEST(ControlledLockViolation, CascadeAbortInvokesDependents) {
  EnsureConfig();
  TestScheduler sched;
  auto parent = std::make_shared<TxClassic>(0, 100, &sched);
  auto dep1 = std::make_shared<TxClassic>(0, 101, &sched);
  auto dep2 = std::make_shared<TxClassic>(0, 102, &sched);
  sched.RegisterTx(parent);
  sched.RegisterTx(dep1);
  sched.RegisterTx(dep2);

  parent->AddDependent(dep1);
  parent->AddDependent(dep2);

  sched.CascadeAbort(*parent);

  ASSERT_EQ(sched.aborted.size(), 2u);
  EXPECT_NE(std::find(sched.aborted.begin(), sched.aborted.end(), dep1->tid_), sched.aborted.end());
  EXPECT_NE(std::find(sched.aborted.begin(), sched.aborted.end(), dep2->tid_), sched.aborted.end());
}

TEST(ControlledLockViolation, CascadeAbortPropagatesThroughChains) {
  EnsureConfig();
  TestScheduler sched;
  auto parent = std::make_shared<TxClassic>(0, 200, &sched);
  auto mid = std::make_shared<TxClassic>(0, 201, &sched);
  auto leaf = std::make_shared<TxClassic>(0, 202, &sched);
  sched.RegisterTx(parent);
  sched.RegisterTx(mid);
  sched.RegisterTx(leaf);

  parent->AddDependent(mid);
  mid->AddDependent(leaf);

  sched.CascadeAbort(*parent);
  ASSERT_EQ(sched.aborted.size(), 2u);
  EXPECT_NE(std::find(sched.aborted.begin(), sched.aborted.end(), mid->tid_), sched.aborted.end());
  EXPECT_NE(std::find(sched.aborted.begin(), sched.aborted.end(), leaf->tid_), sched.aborted.end());
}

TEST(ControlledLockViolation, Scheduler2plViolatesCommitPhaseLocks) {
  EnsureConfig();
  Scheduler2pl sched;
  mdb::FineLockedRow::set_wound_wait();

  // Build a single-column row backed by FineLockedRow.
  mdb::Schema schema;
  schema.add_column("v", Value::I32);
  std::vector<mdb::Value> row_data(1);
  row_data[0].set_i32(0);
  auto row = mdb::FineLockedRow::create(&schema, row_data);

  auto tx1 = std::make_shared<Tx2pl>(0, 1, &sched);
  auto tx2 = std::make_shared<Tx2pl>(0, 2, &sched);

  ASSERT_TRUE(sched.Guard(*tx1, row, 0, true));
  EXPECT_EQ(tx1->locked_locks_.size(), 1u);

  uint64_t staged_lsn = 123;
  tx1->MarkCommitRecordStaged(staged_lsn);

  ASSERT_TRUE(sched.Guard(*tx2, row, 0, true));
  EXPECT_TRUE(tx2->locked_locks_.empty());
  ASSERT_EQ(tx2->violated_locks_.size(), 1u);
  EXPECT_EQ(tx2->required_commit_lsn(), staged_lsn);

  auto dependents = tx1->TakeDependents();
  ASSERT_EQ(dependents.size(), 1u);
  EXPECT_EQ(dependents[0], tx2);

  std::atomic<bool> ready{false};
  std::thread waiter([&]() {
    sched.WaitForCommitDependencies(tx2);
    ready.store(true);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_FALSE(ready.load());
  sched.AdvanceDurableLsn(staged_lsn);
  waiter.join();
  EXPECT_TRUE(ready.load());
}

TEST(ControlledLockViolation, Scheduler2plAbortCascadesToViolators) {
  EnsureConfig();
  TrackingScheduler2pl sched;
  mdb::FineLockedRow::set_wound_wait();

  mdb::Schema schema;
  schema.add_column("v", Value::I32);
  std::vector<mdb::Value> row_data(1);
  row_data[0].set_i32(0);
  auto row = mdb::FineLockedRow::create(&schema, row_data);

  auto tx1 = std::make_shared<Tx2pl>(0, 10, &sched);
  auto tx2 = std::make_shared<Tx2pl>(0, 11, &sched);

  ASSERT_TRUE(sched.Guard(*tx1, row, 0, true));
  tx1->MarkCommitRecordStaged(555);
  ASSERT_TRUE(sched.Guard(*tx2, row, 0, true));

  sched.CascadeAbort(*tx1);
  ASSERT_FALSE(sched.aborted.empty());
  EXPECT_NE(std::find(sched.aborted.begin(), sched.aborted.end(), tx2->tid_), sched.aborted.end());
}
