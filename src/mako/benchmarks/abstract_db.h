#ifndef _ABSTRACT_DB_H_
#define _ABSTRACT_DB_H_

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include <functional>
#include <map>
#include <string>

#include "abstract_ordered_index.h"
#include "../str_arena.h"

class mbta_sharded_ordered_index;

/**
 * Abstract interface for a DB. This is to facilitate writing
 * benchmarks for different systems, making each system present
 * a unified interface
 */
class abstract_db {
public:
  using shard_hash_fn = std::function<size_t(const lcdf::Str &)>;

  /**
   * both get() and put() can throw abstract_abort_exception. If thrown,
   * abort_txn() must be called (calling commit_txn() will result in undefined
   * behavior).  Also if thrown, subsequently calling get()/put() will also
   * result in undefined behavior)
   */
  class abstract_abort_exception {};

  // ctor should open db
  abstract_db() {}

  // dtor should close db
  virtual ~abstract_db();

  /**
   * an approximate max batch size for updates in a transaction.
   *
   * A return value of -1 indicates no maximum
   */
  virtual ssize_t txn_max_batch_size() const { return -1; }

  virtual bool index_has_stable_put_memory() const { return false; }

  // XXX(stephentu): laziness
  virtual size_t
  sizeof_txn_object(uint64_t txn_flags) const { NDB_UNIMPLEMENTED("sizeof_txn_object"); };

  /**
   * XXX(stephentu): hack
   */
  virtual void do_txn_epoch_sync() const {}

  /**
   * XXX(stephentu): hack
   */
  virtual void do_txn_finish() const {}

  /** loader should be used as a performance hint, not for correctness */
  virtual void thread_init(bool loader, int source=0) {}

  virtual void thread_end() {}

  // [ntxns_persisted, ntxns_committed, avg latency]
  virtual std::tuple<uint64_t, uint64_t, double>
    get_ntxn_persisted() const { return std::make_tuple(0, 0, 0.0); }

  virtual void reset_ntxn_persisted() { }

  enum TxnProfileHint {
    HINT_DEFAULT,

    // ycsb profiles
    HINT_KV_GET_PUT, // KV workloads over a single key
    HINT_KV_RMW, // get/put over a single key
    HINT_KV_SCAN, // KV scan workloads (~100 keys)

    // tpcc profiles
    HINT_TPCC_NEW_ORDER,
    HINT_TPCC_PAYMENT,
    HINT_TPCC_DELIVERY,
    HINT_TPCC_ORDER_STATUS,
    HINT_TPCC_ORDER_STATUS_READ_ONLY,
    HINT_TPCC_STOCK_LEVEL,
    HINT_TPCC_STOCK_LEVEL_READ_ONLY,

    // erpc server profiles
    HINT_TPCC_BASIC,
  };

  /**
   * Initializes a new txn object the space pointed to by buf
   *
   * Flags is only for the ndb protocol for now
   *
   * [buf, buf + sizeof_txn_object(txn_flags)) is a valid ptr
   */
  virtual void *new_txn(
      uint64_t txn_flags,
      str_arena &arena,
      void *buf,
      TxnProfileHint hint = HINT_DEFAULT) = 0;

  typedef std::map<std::string, uint64_t> counter_map;
  typedef std::map<std::string, counter_map> txn_counter_map;

  /**
   * Reports things like read/write set sizes
   */
  virtual counter_map
  get_txn_counters(void *txn) const
  {
    return counter_map();
  }

  /**
   * Returns true on successful commit.
   *
   * On failure, can either throw abstract_abort_exception, or
   * return false- caller should be prepared to deal with both cases
   */
  virtual bool commit_txn(void *txn) = 0;

  /**
   * don't send requests to Paxos groups
   * @param txn
   * @return
   */
  virtual bool commit_txn_no_paxos(void *txn) = 0;

  virtual void abort_txn(void *txn) = 0;

  virtual void abort_txn_local(void *txn) = 0;

  virtual void print_txn_debug(void *txn) const {}

  /**
   * maintain a hash ma from table_id to abstract_ordered_index(mbta_ordered_index)
   *
   * @param table_id
   * @return
   */
  virtual abstract_ordered_index *
  get_index_by_table_id(unsigned short table_id) = 0;

  virtual abstract_ordered_index *
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append = false,
	     bool use_hashtable = false) = 0;

  virtual void
  close_index(abstract_ordered_index *idx) = 0;

  virtual void preallocate_open_index() = 0;
  virtual void init() = 0;

  virtual abstract_ordered_index *
  open_index(const std::string &name, int shard_index = -1) = 0;

  virtual mbta_sharded_ordered_index *
  open_sharded_index(const std::string &name) {
    NDB_UNIMPLEMENTED("open_sharded_index");
  }

  virtual void shard_abort_txn(void *txn) = 0;
  virtual int shard_validate() = 0;
  virtual void shard_install(uint32_t timestamp) = 0;
  virtual void shard_serialize_util(uint32_t timestamp)  = 0;
  virtual void shard_unlock(bool committed) = 0;
  virtual void shard_reset() = 0;

  // =========================================================================
  // Early Lock Release (ELR) Interface
  // =========================================================================

  /**
   * @brief Perform early release of locks held by the transaction
   *
   * This marks locks as early-released, allowing other transactions
   * to read the uncommitted values while tracking dependencies.
   *
   * @param txn Transaction object
   * @param txn_id Unique transaction ID for ELR tracking
   * @return true if early release was successful
   */
  virtual bool shard_early_release(void *txn, uint64_t txn_id) {
    // Default implementation: no-op (ELR not supported)
    (void)txn;
    (void)txn_id;
    return false;
  }

  /**
   * @brief Handle a cascade abort request
   *
   * Called when a transaction that early-released locks aborts,
   * triggering cascade abort of dependent transactions.
   *
   * @param txn_id Transaction ID to abort
   * @param cause_txn_id Transaction that caused the cascade
   * @return Number of transactions cascade-aborted
   */
  virtual int shard_cascade_abort(uint64_t txn_id, uint64_t cause_txn_id) {
    // Default implementation: no-op (ELR not supported)
    (void)txn_id;
    (void)cause_txn_id;
    return 0;
  }

  /**
   * @brief Register an ELR dependency
   *
   * Called when a transaction reads an uncommitted value from
   * an early-released lock.
   *
   * @param reader_txn_id Transaction that is reading
   * @param writer_txn_id Transaction that early-released
   * @param table_id Table containing the key
   * @param key Key being read
   * @return true if dependency was registered successfully
   */
  virtual bool shard_register_elr_dependency(uint64_t reader_txn_id,
                                              uint64_t writer_txn_id,
                                              uint16_t table_id,
                                              const std::string& key) {
    // Default implementation: no-op (ELR not supported)
    (void)reader_txn_id;
    (void)writer_txn_id;
    (void)table_id;
    (void)key;
    return false;
  }

  /**
   * @brief Check if early release is safe for a transaction
   *
   * @param txn Transaction object
   * @return true if early release is allowed
   */
  virtual bool can_early_release(void *txn) {
    // Default implementation: ELR not supported
    (void)txn;
    return false;
  }

  /**
   * @brief Get ELR configuration
   * @return true if ELR is enabled for this database
   */
  virtual bool is_elr_enabled() const {
    return false;
  }
};

#endif /* _ABSTRACT_DB_H_ */
