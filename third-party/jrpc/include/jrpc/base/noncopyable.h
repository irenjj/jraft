// Copyright (c) renjj - All Rights Reserved
#pragma once

namespace jbase {

class noncopyable {
 public:
  noncopyable() = default;
  ~noncopyable() = default;

 protected:
  noncopyable(const noncopyable &) = delete;
  void operator=(const noncopyable &) = delete;
};

}  // namespace jbase
