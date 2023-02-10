// Copyright (c) renjj - All Rights Reserved
#pragma once

#include <exception>
#include <string>

namespace jbase {

class Exception : public std::exception {
 public:
  explicit Exception(const char *what);
  explicit Exception(const std::string &what);
  virtual ~Exception() throw();
  virtual const char *what() const throw();
  const char *stackTrace() const throw();

 private:
  void fillStackTrace();

  std::string message_;
  std::string stack_;
};

}  // namespace jbase
