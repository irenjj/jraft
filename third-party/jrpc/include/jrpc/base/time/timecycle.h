// Copyright (c) renjj - All Rights Reserved
#pragma once

#include <cstdint>
#include <algorithm>
#include "get_clock.h"

namespace jbase {

class TimeCycle {
 public:
  TimeCycle() : tsc_since_epoch_(0) {}
  explicit TimeCycle(uint64_t tsc) : tsc_since_epoch_(tsc) {}

  void Swap(TimeCycle &that) {
    std::swap(tsc_since_epoch_, that.tsc_since_epoch_);
  }
  bool Valid() const { return tsc_since_epoch_ > 0; }
  uint64_t tsc_since_epoch() const { return tsc_since_epoch_; }
  uint64_t MicroSecSinceEpoch() const {
    return tsc_since_epoch_ / tsc_rate_mhz_;
  }
  uint64_t SecondSinceEpoch() const { return tsc_since_epoch_ / tsc_rate_; }
  static TimeCycle Now() { return TimeCycle(jbase::get_cycles()); }
  static uint64_t GetTimeCycle() { return jbase::get_cycles(); }
  static uint64_t MicroSecNow() {
    // jbase::get_cycles() * kMicroSecondsPerSecond / tsc_rate_ 会溢出
    return jbase::get_cycles() / tsc_rate_mhz_;
  }
  static uint64_t ToMicroSec(uint64_t cycle) { return cycle / tsc_rate_mhz_; }
  static uint64_t ToNanoSec(uint64_t cycle) {
    return cycle * 1000 / tsc_rate_mhz_;
  }
  static uint64_t SecondNow() { return jbase::get_cycles() / tsc_rate_; }
  static uint64_t ToCycles(uint64_t micro_seconds) {
    return micro_seconds * tsc_rate_mhz_;
  }

  static TimeCycle Invalid() { return TimeCycle(); }
  static uint64_t tsc_rate() { return tsc_rate_; }
  static double TimeDifferenceSecond(TimeCycle high, TimeCycle low) {
    int64_t diff = high.tsc_since_epoch() - low.tsc_since_epoch();
    return static_cast<double>(diff) / tsc_rate_;
  }
  static int64_t TimeDifferenceMicroSec(TimeCycle high, TimeCycle low) {
    int64_t diff = high.tsc_since_epoch() - low.tsc_since_epoch();
    return diff / tsc_rate_mhz_;
  }
  static TimeCycle AddTime(TimeCycle time_cycle, double seconds) {
    uint64_t delta = seconds * tsc_rate_;
    return TimeCycle(time_cycle.tsc_since_epoch() + delta);
  }

  static const int kMicroSecondsPerSecond = 1000 * 1000;

 private:
  uint64_t tsc_since_epoch_;
  static uint64_t tsc_rate_;
  static uint64_t tsc_rate_mhz_;
};

inline bool operator<(TimeCycle lhs, TimeCycle rhs) {
  return lhs.tsc_since_epoch() < rhs.tsc_since_epoch();
}

inline bool operator==(TimeCycle lhs, TimeCycle rhs) {
  return lhs.tsc_since_epoch() == rhs.tsc_since_epoch();
}

}  // namespace jbase
