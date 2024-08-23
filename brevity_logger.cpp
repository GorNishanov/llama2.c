#include "brevity_logger.h"
#include <print>
#include "atomic_intrusive_queue.h"

//--------------------------------------------------------------------
// Extract the filename from a path.
std::string_view get_filename(std::string_view path) {
  // Find the last occurrence of '/' or '\\'
  auto pos = path.find_last_of("/\\");
  if (pos == std::string_view::npos) {
    // No separator found, return the whole path
    return path;
  }
  // Return the substring after the last separator
  return path.substr(pos + 1);
}

//--------------------------------------------------------------------
template <auto Next>
using atomic_intrusive_queue = exec::__atomic_intrusive_queue<Next>;

//--------------------------------------------------------------------
struct Buffer {
  Buffer *next{};
  unsigned size{};
  unsigned count{};
  unsigned thread_id{};
  unsigned filled{};

  static unsigned buffer_count;

  Buffer(const Buffer &) = delete;
  Buffer(Buffer &&) = delete;
  Buffer &operator=(const Buffer &) = delete;
  Buffer &operator=(Buffer &&) = delete;

  static Buffer *Allocate(size_t size = 16 * 1024 * 1024) {
    assert(size >= (sizeof(Buffer) + 16)); // Assume the size is sane.
    // TODO: put into all queue.
    return new (malloc(size)) Buffer(buffer_count++, size);
  }

  std::span<uint8_t> span() {
    auto *end = reinterpret_cast<uint8_t *>(this) + size;
    return {begin(), end};
  }

  uint8_t *begin() {
    return reinterpret_cast<uint8_t *>(this + 1);
  }

  uint8_t *end() {
    return reinterpret_cast<uint8_t *>(this) + size;
  }

  void dump() {
    if (filled == 0)
      return;

    std::string str;
    using LocationInfo = brevity_logger::LocationInfo;
    auto r = brevity_reader({begin(), begin() + filled});
    std::println("--- reader size is {} ---", r.size());
    while (!r.empty()) {
      ptrdiff_t handle = r.read_varint_signed();
      auto *loc = LocationInfo::from_handle(handle);
      LocationInfo::Clock::duration diff{r.read_varint_signed()};
      auto log_time = loc->time + diff;

      print("{} {} <-> ", handle, diff);

      // auto loc_time = std::chrono::zoned_time{std::chrono::current_zone(), log_time};
      print("{} {}:{} ", log_time, get_filename(loc->file_name()), loc->line());
      str.clear();
      loc->formatter(str, loc->fmt, r);
      puts(str.c_str());
    }
  }

 private:

  Buffer(int count, size_t size)
    : size((unsigned)size)
    , count(count) {
  }
};

//--------------------------------------------------------------------

struct GlobalTracer {
  atomic_intrusive_queue<&brevity_logger::LocationInfo::next> loc_queue;

  atomic_intrusive_queue<&Buffer::next> free;
  atomic_intrusive_queue<&Buffer::next> filled;

  ~GlobalTracer() {
    puts("--------------------------------------");
    brevity_logger::LocationInfo::debug_dump_all();
    std::println("total buffers {}", Buffer::buffer_count);
    auto q = filled.pop_all_reversed();
    int count = 0;
    while (auto *buffer = q.try_pop_front()) {
      buffer->dump();
      std::free(buffer);
      ++count;
    }

    auto f = free.pop_all_reversed();
    while (auto *buffer = f.try_pop_front()) {
      std::free(buffer);
      ++count;
    }

    std::println("freed {} allocated {}", count, Buffer::buffer_count);
  }
};

GlobalTracer global_tracer;

//--------------------------------------------------------------------

struct local_brevity : brevity_writer 
{
  local_brevity() = default;

  std::span<uint8_t> current_span() 
    {
      if (!buffer)
        return {};

      // Do not show partial records.
      if (record_ptr)
        return {buffer->begin(), record_ptr};

      return {buffer->begin(), current()};
  }

  void flush_buffer() {
    if (buffer) {
      auto *end = record_ptr ? record_ptr : current();
      buffer->filled = end - buffer->begin();
      global_tracer.filled.push_front(buffer);
      buffer = nullptr;
      exit(1);
    }
  }

 private:

  void install_buffer(Buffer *new_buffer) {
    size_t partial = 0;
    if (buffer) {
      flush_buffer();
      if (record_ptr) {
        // copy partial record bytes to new buffer.
        partial = current() - record_ptr;
        std::memcpy(new_buffer->begin(), record_ptr, partial);
        record_ptr = new_buffer->begin();
      }
    }
    buffer = new_buffer;
    assign({new_buffer->begin() + partial, new_buffer->end()});
  }

  void _grow_by(size_t how_much) override {
    install_buffer(Buffer::Allocate());
  }

  void record_start(size_t ensure) override {
    _ensure(std::max<size_t>(1, ensure));
    record_ptr = current();
  }

  void record_end() override {
    assert(record_ptr && "record_end without record start");
    record_ptr = {};
  }

 protected:
  uint8_t* record_ptr{};
  Buffer *buffer{};
};

unsigned Buffer::buffer_count;

struct LocalTracer : local_brevity {

  ~LocalTracer() {
    flush_buffer();
  }
};

thread_local LocalTracer local_tracer;

brevity_writer &brevity_logger::writer() {
  return local_tracer;
}

void brevity_logger::write_common_header(const LocationInfo &loc_info) {
  auto now = LocationInfo::Clock::now();
  auto diff = now - loc_info.time;
  auto& w = writer();
  w.record_start();
  w.write_varint_signed(loc_info.handle);
  w.write_varint_signed(diff.count());
}

brevity_logger::LocationInfo::LocationInfo(
  std::source_location loc,
  std::string_view fmt,
  formatter_t fn)
  : base(loc)
  , fmt(fmt)
  , formatter(fn)
  , time(Clock::now()) {
  global_tracer.loc_queue.push_front(this);
  handle =
    reinterpret_cast<ptrdiff_t>(this) - reinterpret_cast<ptrdiff_t>(&global_tracer.loc_queue);
}

const brevity_logger::LocationInfo *brevity_logger::LocationInfo::from_handle(ptrdiff_t handle) {
  auto *result = reinterpret_cast<LocationInfo *>(
    reinterpret_cast<ptrdiff_t>(&global_tracer.loc_queue) + handle);
  assert(result->handle == handle);
  return result;
}

void brevity_logger::LocationInfo::debug_dump_all() {
  std::println("dumping location info");
  for (auto *info = global_tracer.loc_queue.pop_all_reversed().front(); info != nullptr; info = info->next) {
    std::println("0x{:x} {}:{}:{}:{}", (uintptr_t)info, info->file_name(), info->line(), info->column(), info->fmt);
  }
}

brevity_logger::brevity_logger() {
}

brevity_logger::~brevity_logger() {
}
