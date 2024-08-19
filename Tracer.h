#pragma once

#include <atomic>
#include <thread>
#include <format>
#include <string_view>

namespace std {
class global_trace_dumper {
  thread t;
  atomic<bool> done{false};

  static unsigned get_seq();
  static void log(unsigned seq, std::size_t size, std::string_view, std::string_view, std::format_args);

public:
  global_trace_dumper();
  ~global_trace_dumper();

  template <class... Args>
  friend void log_info_with_prefix(std::string_view prefix, std::format_string<Args...> fmt, Args &&...args);
};

template <class... Args>
void log_info_with_prefix(std::string_view prefix, std::format_string<Args...> fmt, Args &&...args) {
  unsigned seq = global_trace_dumper::get_seq();
  auto size = formatted_size(fmt, std::forward<Args>(args)...);
  global_trace_dumper::log(seq, size, prefix, fmt.get(), std::make_format_args(args...));
}
template <class... Args>
void log_info(std::format_string<Args...> fmt, Args &&...args) {
    log_info_with_prefix("", fmt, std::forward<Args>(args)...);
}

} // namespace std
