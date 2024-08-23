#pragma once
#include <cstdint>
#include <array>
#include <span>
#include <format>
#include <chrono>
#include <optional>
#include <vector>

template <typename T>
struct brevity_formatter;

struct brevity_writer;
struct brevity_reader;

template <class T, class Formatter = brevity_formatter<std::remove_const_t<T>>>
concept brevity_formattable_with =
  std::semiregular<Formatter>
  && requires(const Formatter &__cf, T &&__t, brevity_writer &w, brevity_reader &r) {
       { Formatter::write(__t, w) };
       { Formatter::read(r) }; // -> std::same_as<T>;
     };

template <class T>
concept brevity_formattable = brevity_formattable_with<std::remove_reference_t<T>>;

struct brevity_writer {

  template <brevity_formattable T>
  void write(const T &value) {
    brevity_formatter<std::remove_cvref_t<T>>::write(value, *this);
  }

  template <typename T>
    requires std::is_trivially_copyable_v<T>
  void write_bytes(const T &value) {
    constexpr size_t size = sizeof(T);
    _ensure(size);
    std::memcpy(_cur, &value, size);
    _cur += size;
  }

  void write_string(std::string_view value) {
    write_varint(value.size());
    _ensure(value.size());
    std::memcpy(_cur, value.data(), value.size());
    _cur += value.size();
  }

  void write_varint_with_metabit(bool meta, uint64_t value);
  void write_varint(uint64_t value);
  void write_varint_signed(int64_t value);

  virtual void record_start(size_t ensure = 20) {
  }

  virtual void record_end() {
  }

  uint8_t *current() {
    return _cur;
  }

  void assign(std::span<uint8_t> buffer) {
    _cur = buffer.data();
    _end = buffer.data() + buffer.size();
  }

 protected:
  void _ensure_enough_for_varint() {
    if (_cur + 10 > _end)
      _grow_by(10);
  }

  void _ensure(size_t extra) {
    if (static_cast<size_t>(_end - _cur) < extra)
      _grow_by(extra);
  }
 private:
  void write_varint_with_metabit_internal(bool meta, uint64_t value);

 protected:
  virtual void _grow_by(size_t value) = 0;

  brevity_writer() = default;

  brevity_writer(std::span<uint8_t> buffer)
    : _cur(buffer.data())
    , _end(buffer.data() + buffer.size()) {
  }

  uint8_t *_cur{};
  uint8_t *_end{};
};

struct vector_brevity_writer : brevity_writer {

  vector_brevity_writer() {
  }

  size_t size() const {
    return buffer.size();
  }

  std::span<uint8_t> span() {
    return {_cur ? &buffer[0] : 0, _cur};
  }

 private:
  void _grow_by(size_t value) override {
    auto top = _cur ? _cur - buffer.data() : 0;
    buffer.resize(top + value);
    buffer.resize(buffer.capacity());
    _cur = buffer.data() + top;
    _end = buffer.data() + buffer.size();
  }

  std::vector<uint8_t> buffer;
};

struct brevity_reader {
  brevity_reader(std::span<const uint8_t> data)
    : buf(data) {
  }

  bool empty() const {
    return buf.empty();
  }

  size_t size() const {
    return buf.size();
  }

  // implement read in terms of brevity_formatter<T>::read


  template <brevity_formattable T>
  std::remove_cvref_t<T> read() {
    return brevity_formatter<std::remove_cvref_t<T>>::read(*this);
  }

  std::string_view read_string() {
    size_t len = read_varint();
    std::string_view result((const char *) buf.data(), len);
    buf = buf.subspan(len);
    return result;
  }

  template <typename T>
    requires std::is_trivially_copyable_v<T>
  T read_bytes() {
    constexpr size_t size = sizeof(T);
    T value{};
    if (buf.size() < size)
      throw_parse_error("requested {}, available {}", size, buf.size());
    std::memcpy(&value, buf.data(), size);
    buf = buf.subspan(size);
    return value;
  }

  uint64_t read_varint();
  int64_t read_varint_signed();
  std::pair<uint64_t, bool> read_varint_with_metabit();

  std::optional<uint64_t> try_read_varint();
  std::optional<int64_t> try_read_varint_signed();

 private:
  [[noreturn]]
  void throw_parse_error_v(std::string_view message, std::format_args args);

  template <typename... Args>
  [[noreturn]]
  void throw_parse_error(std::format_string<Args...> fmt, Args... args) {
    throw_parse_error_v(fmt.get(), std::make_format_args(args...));
  }

  uint64_t accumulate(unsigned char count);
  std::pair<uint64_t, bool> read_varint_n_check_len_every_byte();
 private:
  std::span<const uint8_t> buf;
};

// Define a formatter for std::optional<T>
template <typename T, typename Ch>
  requires std::formattable<T, Ch>
struct std::formatter<std::optional<T>, Ch> : std::formatter<T, Ch> {
  template <typename FormatContext>
  auto format(const std::optional<T> &opt, FormatContext &ctx) const {
    return opt ? std::formatter<T, Ch>::format(*opt, ctx) : format_to(ctx.out(), "nullopt");
  }
};

template <typename T>
  requires std::is_integral_v<T> && std::is_signed_v<T>
struct brevity_formatter<T> {
  static void write(const T &value, brevity_writer &w) {
    w.write_varint_signed(value);
  }

  static T read(brevity_reader &r) {
    return static_cast<T>(r.read_varint_signed());
  }
};

template <typename T>
  requires std::is_integral_v<T>
        && !std::is_signed_v<T>
           struct brevity_formatter<T> {
  static void write(const T &value, brevity_writer &w) {
    w.write_varint(value);
  }

  static T read(brevity_reader &r) {
    return static_cast<T>(r.read_varint());
  }
};

template <>
struct brevity_formatter<std::string_view> {
  static void write(const std::string_view &value, brevity_writer &w) {
    w.write_string(value);
  }

  static std::string_view read(brevity_reader &r) {
    return r.read_string();
  }
};

template <>
struct brevity_formatter<const char *> {
  static void write(const char *value, brevity_writer &w) {
    w.write_string(value);
  }

  static std::string_view read(brevity_reader &r);
};

template <>
struct brevity_formatter<std::string> {
  static void write(const std::string &value, brevity_writer &w) {
    w.write_string(value);
  }

  static std::string_view read(brevity_reader &r);
};

template <size_t N>
struct brevity_formatter<char[N]> {
  static void write(const char (&value)[N], brevity_writer &w) {
    if (N > 0 && value[N - 1] == '\0') {
      w.write_string(value);
    } else {
      w.write_string({value, N});
    }
  }

  static std::string_view read(brevity_reader &r);
};

template <typename Rep, typename Period>
struct brevity_formatter<std::chrono::duration<Rep, Period>> {
  using type = std::chrono::duration<Rep, Period>;

  static void write(const type &value, brevity_writer &w) {
    w.write_bytes(value.count());
  }

  static type read(brevity_reader &r) {
    return r.read_bytes<type>();
  }
};

template <typename Clock, typename Duration>
struct brevity_formatter<std::chrono::time_point<Clock, Duration>> {
  static void write(const std::chrono::time_point<Clock, Duration> &value, brevity_writer &w) {
    w.write_bytes(value.time_since_epoch().count());
  }

  static std::chrono::time_point<Clock, Duration> read(brevity_reader &r) {
    return std::chrono::time_point<Clock, Duration>{Duration{r.read_bytes<Duration>()}};
  }
};
