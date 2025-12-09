#ifndef _MAKO_ELV_H_
#define _MAKO_ELV_H_

#include <atomic>

// Global feature flag for Early Lock Violation (ELV).
// Default is disabled; enable via --enable_elv to allow readers to bypass
// write-intent locks when it is safe to do so.
extern std::atomic<bool> g_enable_elv;

inline bool
elv_enabled()
{
  return g_enable_elv.load(std::memory_order_acquire);
}

inline void
set_elv_enabled(bool enabled)
{
  g_enable_elv.store(enabled, std::memory_order_release);
}

#endif /* _MAKO_ELV_H_ */
