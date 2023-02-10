// Copyright (c) renjj - All Rights Reserved
#pragma once

#include <cstdint>

namespace jbase {

#if defined(__x86_64__) || defined(__i386__)
/* Note: only x86 CPUs which have rdtsc instruction are supported. */
static inline uint64_t get_cycles() {
  unsigned low, high;
  unsigned long long val;
  asm volatile("rdtsc" : "=a"(low), "=d"(high));
  val = high;
  val = (val << 32) | low;
  return val;
}
#elif defined(__aarch64__)

static inline uint64_t get_cycles() {
  uint64_t val;
  asm volatile("isb" : : : "memory");
  asm volatile("mrs %0, cntvct_el0" : "=r"(val));
  return val;
}

#endif

double get_cpu_mhz(int no_cpu_freq_warn = 0);

}  // namespace jbase
