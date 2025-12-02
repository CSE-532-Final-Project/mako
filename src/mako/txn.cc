#include "macros.h"
#include "amd64.h"
#include "txn.h"
#include "txn_proto2_impl.h"
#include "txn_btree.h"
#include "lockguard.h"
#include "scopedperf.hh"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>

using namespace std;
using namespace util;

static string
proto1_version_str(uint64_t v) UNUSED;
static string
proto1_version_str(uint64_t v)
{
  ostringstream b;
  b << v;
  return b.str();
}

static string
proto2_version_str(uint64_t v) UNUSED;
static string
proto2_version_str(uint64_t v)
{
  ostringstream b;
  b << "[core=" << transaction_proto2_static::CoreId(v) << " | n="
    << transaction_proto2_static::NumId(v) << " | epoch="
    << transaction_proto2_static::EpochId(v) << "]";
  return b.str();
}

// XXX(stephentu): hacky!
string (*g_proto_version_str)(uint64_t v) = proto2_version_str;

CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe0, g_txn_commit_probe0_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe1, g_txn_commit_probe1_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe2, g_txn_commit_probe2_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe3, g_txn_commit_probe3_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe4, g_txn_commit_probe4_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe5, g_txn_commit_probe5_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe6, g_txn_commit_probe6_cg);

#define EVENT_COUNTER_IMPL_X(x) \
  event_counter transaction_base::g_ ## x ## _ctr(#x);
ABORT_REASONS(EVENT_COUNTER_IMPL_X)
#undef EVENT_COUNTER_IMPL_X

event_counter transaction_base::g_evt_read_logical_deleted_node_search
    ("read_logical_deleted_node_search");
event_counter transaction_base::g_evt_read_logical_deleted_node_scan
    ("read_logical_deleted_node_scan");
event_counter transaction_base::g_evt_dbtuple_write_search_failed
    ("dbtuple_write_search_failed");
event_counter transaction_base::g_evt_dbtuple_write_insert_failed
    ("dbtuple_write_insert_failed");

event_counter transaction_base::evt_local_search_lookups("local_search_lookups");
event_counter transaction_base::evt_local_search_write_set_hits("local_search_write_set_hits");
event_counter transaction_base::evt_dbtuple_latest_replacement("dbtuple_latest_replacement");

#if ENABLE_EARLY_LOCK_VIOLATION && defined(CHECK_INVARIANTS)
namespace {
struct dependency_dummy_txn : public transaction_base {
  dependency_dummy_txn(uint64_t f = 0) : transaction_base(f) {}
  void request_abort_from_dependency(transaction_base *) override
  {
    state = TXN_ABRT;
  }
};

void run_dependency_sanity_checks()
{
  dependency_dummy_txn upstream(transaction_base::TXN_FLAG_EARLY_LOCK_VIOLATION);
  dependency_dummy_txn dependent(transaction_base::TXN_FLAG_EARLY_LOCK_VIOLATION);
  ALWAYS_ASSERT(dependent.add_dependency_on(&upstream));
  ALWAYS_ASSERT(dependent.dependency_waiting());
  upstream.notify_dependents_commit();
  ALWAYS_ASSERT(!dependent.dependency_waiting());
  dependency_dummy_txn upstream_abort(transaction_base::TXN_FLAG_EARLY_LOCK_VIOLATION);
  dependency_dummy_txn dependent_abort(transaction_base::TXN_FLAG_EARLY_LOCK_VIOLATION);
  dependent_abort.add_dependency_on(&upstream_abort);
  upstream_abort.notify_dependents_abort();
  ALWAYS_ASSERT(dependent_abort.dependency_canceled());
  ALWAYS_ASSERT(dependent_abort.state == transaction_base::TXN_ABRT);
}

struct dependency_test_hook {
  dependency_test_hook() { run_dependency_sanity_checks(); }
} g_dependency_test_hook;
} // anonymous namespace
#endif
