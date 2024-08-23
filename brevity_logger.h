#pragma once
#include <string_view>
#include <string>
#include <source_location>
#include <type_traits>
#include <tuple>
#include "brevity.h"

template <size_t N>
struct string_literal {
  consteval string_literal(const char *str, int len) {
    std::copy_n(str, len + 1, value);
  }

  consteval string_literal(const char (&str)[N]) {
    std::copy_n(str, N, value);
  }

  char value[N];

  consteval const char *c_str() const {
    return value;
  }

  consteval operator std::string_view() const // implicit intentional
  {
    return {value, N - 1};
  }
};

// TODO: take template parameter, configuring the output stream and whether to include thread_id, timestamp or seqno.
struct brevity_logger {
  brevity_logger();
  ~brevity_logger();

  struct LocationInfo : std::source_location {
    using base = std::source_location;
    using Clock = std::chrono::system_clock;
    std::string_view fmt;
    LocationInfo *next{};
    using formatter_t = void (*)(std::string& output, std::string_view fmt, brevity_reader &r);
    formatter_t formatter;
    Clock::time_point time;
    ptrdiff_t handle{};

    LocationInfo(std::source_location loc, std::string_view fmt, formatter_t fn);
    static const LocationInfo *from_handle(ptrdiff_t handle);
    static void debug_dump_all();
  };

  static brevity_writer &writer();

#if 0
  template <typename... Args>
  static void fff(std::string& output, std::string_view fmt, brevity_reader &r) {
    std::tuple<Args...> args(r.read<Args>()...);
    std::vformat_to(
      std::back_inserter(output),
      fmt,
      std::apply(
        [](auto &&...unpackedArgs) { return std::make_format_args(unpackedArgs...); }, args));
  }
#endif
template <typename... Args>
static void fff(std::string& output, std::string_view fmt, brevity_reader &r) {
    std::tuple<std::optional<Args>...> args;

    std::apply([&r](auto&... opt_args) {
        ((opt_args = r.read<Args>()), ...);
    }, args);

    std::vformat_to(
        std::back_inserter(output),
        fmt,
        std::apply(
            [](auto &&...unpackedArgs) { return std::make_format_args(*unpackedArgs...); }, args));
}

  template <class T>
  struct stored_as {
    using type = decltype(brevity_formatter<T>::read(std::declval<brevity_reader &>()));
  };

  template <class T>
  using stored_as_t = typename stored_as<std::remove_cvref_t<T>>::type;

  static void write_common_header(const LocationInfo &loc_info);

  template <string_literal str, int counter, typename... Args>
  static void log(std::source_location loc, std::format_string<Args...> fmt, Args &&...args) {
    static LocationInfo loc_info{loc, fmt.get(), &fff<stored_as_t<Args>...>};
    write_common_header(loc_info);
    (writer().write(args), ...);
    writer().record_end();
  }
};

#define brevity_logger_log(fmt, ...)                                                               \
  brevity_logger::log<__FILE__, __COUNTER__>(std::source_location::current(), fmt, __VA_ARGS__)

#ifndef log_info
#  define log_info(fmt, ...) brevity_logger_log(fmt, __VA_ARGS__)
#endif