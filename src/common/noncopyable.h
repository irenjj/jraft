// Copyright (c) renjj - All Rights Reserved
#pragma once

namespace jraft {

class Noncopyable {
 public:
  Noncopyable() = default;
  ~Noncopyable() = default;

 protected:
  Noncopyable(const Noncopyable&) = delete;
  void operator=(const Noncopyable&) = delete;
};

}  // namespace jraft
