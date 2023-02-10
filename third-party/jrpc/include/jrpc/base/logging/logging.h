// Copyright (c) renjj - All Rights Reserved
#pragma once

#include "base/logging/log_stream.h"
#include "base/time/timestamp.h"

namespace jbase {

class TimeZone;

class Logger {
 public:
  enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL,
    NUM_LOG_LEVELS,
  };

  // compile time calculation of basename of source file
  class SourceFile {
   public:
    template <int N>
    inline SourceFile(const char (&arr)[N])
        : data_(arr), size_(N - 1) {
      const char *slash = strrchr(data_, '/');  // builtin function
      if (slash) {
        data_ = slash + 1;
        size_ -= static_cast<int>(data_ - arr);
      }
    }

    explicit SourceFile(const char *filename) : data_(filename) {
      const char *slash = strrchr(filename, '/');
      if (slash) {
        data_ = slash + 1;
      }
      size_ = static_cast<int>(strlen(data_));
    }

    const char *data_;
    int size_;
  };

  Logger(SourceFile file, int line);
  Logger(SourceFile file, int line, LogLevel level);
  Logger(SourceFile file, int line, LogLevel level, const char *func);
  Logger(SourceFile file, int line, bool toAbort);
  ~Logger();

  LogStream &stream() { return impl_.stream_; }

  static LogLevel logLevel();
  static void setLogLevel(LogLevel level);
  static void setLogLevel(const std::string &level);

  typedef void (*OutputFunc)(const char *msg, int len);
  typedef void (*FlushFunc)();
  static void setOutput(OutputFunc);
  static void setFlush(FlushFunc);
  static void setTimeZone(const TimeZone &tz);

 private:
  class Impl {
   public:
    typedef Logger::LogLevel LogLevel;
    Impl(LogLevel level, int old_errno, const SourceFile &file, int line);
    void formatTime();
    void finish();

    Timestamp time_;
    LogStream stream_;
    LogLevel level_;
    int line_;
    SourceFile basename_;
  };

  Impl impl_;
};

extern Logger::LogLevel g_logLevel;

inline Logger::LogLevel Logger::logLevel() { return g_logLevel; }

#define JLOG_TRACE                                     \
  if (jbase::Logger::logLevel() <= jbase::Logger::TRACE) \
  jbase::Logger(__FILE__, __LINE__, jbase::Logger::TRACE, __func__).stream()
#define JLOG_DEBUG                                     \
  if (jbase::Logger::logLevel() <= jbase::Logger::DEBUG) \
  jbase::Logger(__FILE__, __LINE__, jbase::Logger::DEBUG, __func__).stream()
#define JLOG_INFO                                     \
  if (jbase::Logger::logLevel() <= jbase::Logger::INFO) \
  jbase::Logger(__FILE__, __LINE__).stream()
#define JLOG_WARN jbase::Logger(__FILE__, __LINE__, jbase::Logger::WARN).stream()
#define JLOG_ERROR \
  jbase::Logger(__FILE__, __LINE__, jbase::Logger::ERROR).stream()
#define JLOG_FATAL \
  jbase::Logger(__FILE__, __LINE__, jbase::Logger::FATAL).stream()
#define JLOG_SYSERR jbase::Logger(__FILE__, __LINE__, false).stream()
#define JLOG_SYSFATAL jbase::Logger(__FILE__, __LINE__, true).stream()

const char *strerror_tl(int savedErrno);

// Taken from glog/logging.h
//
// Check that the input is non NULL.  This very useful in constructor
// initializer lists.

#define CHECK_NOTNULL(val) \
  jbase::CheckNotNull(__FILE__, __LINE__, "'" #val "' Must be non NULL", (val))

// A small helper for CHECK_NOTNULL().
template <typename T>
T *CheckNotNull(Logger::SourceFile file, int line, const char *names, T *ptr) {
  if (ptr == NULL) {
    Logger(file, line, Logger::FATAL).stream() << names;
  }
  return ptr;
}

}  // namespace jbase
