#include <gtest/gtest.h>

#include "mako/tuple.h"
#include "mako/elv.h"
#include "mako/allocator.h"
#include "mako/rcu.h"
#include "masstree/kvthread.hh"

#include <cstring>
#include <atomic>
#include <memory>
#include <thread>
#include <chrono>

// Provide global epoch for RCU helpers used by dbtuple.
volatile mrcu_epoch_type globalepoch = 1;

namespace {

struct StringReader {
  explicit StringReader(std::string *out) : out(out) {}
  typedef std::string value_type;

  template <typename StringAllocator>
  bool operator()(const uint8_t *data, size_t sz, StringAllocator &)
  {
    out->assign(reinterpret_cast<const char *>(data), sz);
    return true;
  }

  template <typename StringAllocator>
  void dup(const std::string &v, StringAllocator &)
  {
    *out = v;
  }

  std::string *out;
};

class ElvTupleTest : public ::testing::Test {
protected:
  void SetUp() override
  {
    // Initialize allocator once; idempotent for test environment.
    static bool initialized = false;
    if (!initialized) {
      allocator::Initialize(2, 32 * 1024 * 1024);
      initialized = true;
    }
    set_elv_enabled(true);
    guard_.reset(new scoped_rcu_region());
  }

  void TearDown() override
  {
    set_elv_enabled(false);
    guard_.reset(nullptr);
  }

private:
  std::unique_ptr<scoped_rcu_region> guard_;
};

// Use distinct fixtures so gtest reports separate suites.
class ElvReadTest : public ElvTupleTest {};
class ElvValidateTest : public ElvTupleTest {};
class ElvWriteTest : public ElvTupleTest {};
class ElvInsertTest : public ElvTupleTest {};

TEST_F(ElvReadTest, ReadBypassesIntentLockWithStableVersion)
{
  std::string value = "init";
  dbtuple *tuple = dbtuple::alloc_first(value.size(), true);
  memcpy(tuple->get_value_start(), value.data(), value.size());
  tuple->version = 1;

  util::default_string_allocator sa;
  std::string out;
  StringReader reader(&out);
  dbtuple::tid_t start_t = 0;
  const auto status =
      tuple->stable_read(dbtuple::MAX_TID, start_t, reader, sa, true);

  EXPECT_EQ(status, dbtuple::READ_RECORD);
  EXPECT_EQ(out, value);
  EXPECT_EQ(start_t, 1u);
}

TEST_F(ElvValidateTest, ValidationFailsAfterInstall)
{
  std::string value = "init";
  dbtuple *tuple = dbtuple::alloc_first(value.size(), true);
  memcpy(tuple->get_value_start(), value.data(), value.size());
  tuple->version = 10;

  util::default_string_allocator sa;
  std::string out;
  StringReader reader(&out);
  dbtuple::tid_t start_t = 0;
  ASSERT_EQ(tuple->stable_read(dbtuple::MAX_TID, start_t, reader, sa, true),
            dbtuple::READ_RECORD);

  // Simulate install of a new version.
  tuple->mark_modifying();
  tuple->version = 11;
  tuple->unlock();

  EXPECT_FALSE(tuple->stable_is_latest_version(start_t));
}

TEST_F(ElvWriteTest, WriteWriteConflictsSerializeInstalls)
{
  std::string value = "init";
  dbtuple *tuple = dbtuple::alloc_first(value.size(), false);
  memcpy(tuple->get_value_start(), value.data(), value.size());
  tuple->version = 5;

  std::atomic<bool> writer_locked{false};
  std::thread writer([&] {
    tuple->lock(true);
    writer_locked.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    tuple->unlock();
  });

  // Wait until the writer holds the intent lock.
  while (!writer_locked.load(std::memory_order_acquire))
    ;

  dbtuple::version_t observed = 0;
  EXPECT_FALSE(tuple->try_writer_stable_version(observed, 2));

  writer.join();
}

TEST_F(ElvReadTest, ElvDisabledBlocksOnIntent)
{
  set_elv_enabled(false);

  std::string value = "hold";
  dbtuple *tuple = dbtuple::alloc_first(value.size(), true);
  memcpy(tuple->get_value_start(), value.data(), value.size());
  tuple->version = 42;

  // Reader should not bypass while intent lock is held (no stable version access).
  std::atomic<bool> read_done{false};
  std::string out;
  std::thread reader([&] {
    util::default_string_allocator sa;
    StringReader reader(&out);
    dbtuple::tid_t start_t = 0;
    // Reflect transaction path when ELV is off: do not allow intent bypass.
    auto status = tuple->stable_read(dbtuple::MAX_TID, start_t, reader, sa, false);
    read_done.store(true, std::memory_order_release);
    EXPECT_EQ(status, dbtuple::READ_RECORD);
    EXPECT_EQ(start_t, 42u);
    EXPECT_EQ(out, value);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_FALSE(read_done.load(std::memory_order_acquire));

  tuple->unlock(); // releases intent lock
  reader.join();
}

TEST_F(ElvWriteTest, ReadersWaitForInstallToFinish)
{
  set_elv_enabled(true);

  std::string value = "old";
  dbtuple *tuple = dbtuple::alloc_first(value.size(), true);
  memcpy(tuple->get_value_start(), value.data(), value.size());
  tuple->version = 9;

  std::atomic<bool> writer_ready{false};
  std::thread writer([&] {
    tuple->mark_modifying();
    memcpy(tuple->get_value_start(), "new", 3);
    tuple->size = 3;
    tuple->version = 10;
    writer_ready.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    tuple->unlock();
  });

  while (!writer_ready.load(std::memory_order_acquire))
    ;

  util::default_string_allocator sa;
  std::string out;
  StringReader reader(&out);
  dbtuple::tid_t start_t = 0;
  auto status = tuple->stable_read(dbtuple::MAX_TID, start_t, reader, sa, true);

  writer.join();
  EXPECT_EQ(status, dbtuple::READ_RECORD);
  EXPECT_EQ(out, "new");
  EXPECT_EQ(start_t, 10u);
}

TEST_F(ElvInsertTest, InsertMarkerNotBypassed)
{
  set_elv_enabled(true);
  // MAX_TID means tentative insert; treat as no stable version.
  dbtuple *tuple = dbtuple::alloc_first(0, true);
  tuple->version = dbtuple::MAX_TID;

  util::default_string_allocator sa;
  std::string out;
  StringReader reader(&out);
  dbtuple::tid_t start_t = 123; // will be overwritten
  auto status = tuple->stable_read(dbtuple::MAX_TID, start_t, reader, sa, true);
  EXPECT_EQ(status, dbtuple::READ_EMPTY);
  EXPECT_EQ(start_t, 0u);

  tuple->unlock();
}

} // namespace
