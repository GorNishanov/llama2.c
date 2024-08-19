#include "Tracer.h"

#include <memory>
#include <mutex>
#include <print>

#include "intrusive_slist.h"
#include <thread>

#define NOMINMAX
#include "Windows.h"

using namespace std;

/*
 * thread-id comes from header.
 *
 * 8-byte timestamp
 * 4-byte seq-no
 * 1-byte level
 * 3-byte message length
 * bytes.
 *
 */

struct Buffer {
  struct Header {
    unsigned seq;
    FILETIME time;
    unsigned size;
  };

  struct Message : Header {
    std::string_view message() const {
      return {reinterpret_cast<const char *>(this + 1), size};
    };
    const Message *next() const {
      return reinterpret_cast<const Message *>(
          reinterpret_cast<const char *>(this + 1) + size);
    }
    void dump() const
    {
        dump(0);
    }

    void dump(unsigned thread_id) const
    {
        SYSTEMTIME st;
        FileTimeToSystemTime(&time, &st);
        println("{:04x}.{:05}::{:04}/{:02}/{:02}-{:02}:{:02}:{:02}.{:03} {}",
                thread_id, seq, st.wYear, st.wMonth, st.wDay, st.wHour,
                st.wMinute, st.wSecond, st.wMilliseconds, message());
    }
  };

  Buffer *next{};
  Buffer *next_all{};
  unsigned thread_id{};
  const Message *next_message_to_process{};
  std::atomic<unsigned> current{};
  unsigned size{};

  Buffer() { init(); }
  ~Buffer() { _aligned_free(this); }

  void dump() const
  {
      auto *end = this->end();
      for (auto *m = this->message_at(sizeof(Buffer)); m < end; m = m->next())
          m->dump(this->thread_id);
  }

  const Message* message_at(unsigned offset) const
  {
    return reinterpret_cast<const Message *>(
        reinterpret_cast<const char *>(this) + offset);
  }

  const Message* end() const
  {
    return message_at(current.load(std::memory_order::acquire));
  }

  Buffer* init() {
    next_message_to_process = message_at(sizeof(Buffer));
    thread_id = GetCurrentThreadId();
    current.store(sizeof(Buffer), memory_order::release);
    return this;
  }
};

struct Tracer {
  std::mutex mutex;
    stdexec::__intrusive_slist<&Buffer::next_all> all;
  stdexec::__intrusive_slist<&Buffer::next> free;
  stdexec::__intrusive_slist<&Buffer::next> filled;

  Buffer* last_processed{};
  unsigned printed_seq{};
  unsigned buffer_count{};

  alignas(
      std::hardware_destructive_interference_size) std::atomic<unsigned> seq;
  std::atomic<int> dropped{0};

  const Buffer::Message *find_in_buffer(unsigned seq, Buffer *v) {
    auto *start = v->next_message_to_process;
    if (start == v->end())
        return nullptr;

    if (start->seq < seq)
    {
        std::println("unexpectedly, start->seq < seq: {:x} < {:x}", start->seq,
                        seq);
        exit(0);
    }

    if (start->seq == seq)
    {
        v->next_message_to_process = start->next();
        last_processed = v;
        return start;
    }

    return nullptr;
  }

  const Buffer::Message *find(unsigned seq, Buffer* b) {
    if (last_processed)
    {
      if (auto *m = find_in_buffer(seq, last_processed))
        return m;
    }

    for (auto *v = b; v; v = v->next_all)
      if (auto *m = find_in_buffer(seq, v))
        return m;

    return nullptr;
  }


  bool print(unsigned seq, Buffer *front) {
    if (auto *m = find(seq, front)) {
        m->dump(last_processed->thread_id);
        return true;
    }
    return false;
  }

  void dump() {
    Buffer *p = nullptr;
    {
      std::lock_guard grab(mutex);
      p = all.front();
    }
    if (!p) {
      println("No buffers");
      return;
    }
    auto last_seq = seq.load(std::memory_order_acquire);
    for (printed_seq; printed_seq < last_seq; ++printed_seq) {
      if (!print(printed_seq, p))
        break;
    }
  }

  Buffer *allocate_buffer(size_t size = 1024 * 1024) {
    auto *buffer = reinterpret_cast<Buffer *>(
        _aligned_malloc(size, std::hardware_destructive_interference_size));
    buffer->size = (unsigned)size;
    buffer->init();
    std::lock_guard grab(mutex);
    all.push_front(buffer);
    ++buffer_count;
    return buffer;
  }

  ~Tracer() {
    dump();
    while (auto *p = all.try_pop_front())
      p->~Buffer();
  }

  void init(int n = std::thread::hardware_concurrency()) {
    for (int i = 0; i < n; ++i)
      free.push_front(allocate_buffer());
  }

  Buffer *get_free(Buffer *previous = nullptr) {
    Buffer *result = nullptr;
    {
      std::lock_guard grab(mutex);
      if (previous)
        filled.push_front(previous);
      result = free.try_pop_front();
    }
    if (!result)
      result = allocate_buffer();

    return result->init();
  }

  void push_filled(Buffer *b) {
    std::lock_guard grab(mutex);
    filled.push_front(b);
  }

  static Tracer global_tracer;
};

template <typename T>
concept PodType = std::is_pod_v<T>;

Tracer Tracer::global_tracer;

struct LocalTracer {
  Buffer *buffer{};
  ~LocalTracer() {
    if (buffer)
      Tracer::global_tracer.push_filled(buffer);
  }

  struct reservation {
    char *ptr{};
    Buffer *buffer{};
    unsigned next_current{};

    reservation() = default;
    reservation(Buffer *buffer, unsigned next_current)
        : buffer(buffer), next_current(next_current),
          ptr(reinterpret_cast<char *>(buffer) +
              buffer->current.load(std::memory_order_relaxed)) {}

    template <PodType T> void write(const T &v) {
      memcpy(ptr, &v, sizeof(v));
      ptr += sizeof(v);
    }

    void write(std::string_view v) {
      memcpy(ptr, v.data(), v.size());
      ptr += v.size();
    }

    explicit operator bool() const { return buffer; }

    ~reservation() {
      if (buffer)
        buffer->current.store(next_current, std::memory_order_release);
    }
  };

  reservation reserve(size_t bytes) {
    bool new_buffer = false;
    for (;;) {
      if (buffer) {
        auto current = buffer->current.load(std::memory_order_relaxed);
        auto new_current = current + (unsigned)bytes;
        if (new_current <= buffer->size)
          return {buffer, new_current};

        if (new_buffer) {
          Tracer::global_tracer.dropped.fetch_add(1, std::memory_order_relaxed);
          return {};
        }
      }

      buffer = Tracer::global_tracer.get_free(buffer);
      new_buffer = true;
    }
  }
};

thread_local LocalTracer local_tracer;

namespace std {
global_trace_dumper::global_trace_dumper()
    //: t(std::thread([this] {
    //    using namespace std::chrono_literals;
    //    while (!done.load()) {
    //      std::this_thread::sleep_for(1ms);
    //      Tracer::global_tracer.dump();
    //    }
    //    Tracer::global_tracer.dump();
    //  }))
{
  Tracer::global_tracer.init();
}

void dump(const Buffer::Message* m)
{
    m->dump();
}

void dump(const Buffer* b)
{
    b->dump();
}

global_trace_dumper::~global_trace_dumper() {
  //done = true;
  //t.join();
  Tracer::global_tracer.dump();
}
void global_trace_dumper::log(unsigned seq, size_t size, std::string_view prefix, std::string_view fmt,
                              std::format_args args) {

  size += prefix.size();

  if (auto r = local_tracer.reserve(sizeof(Buffer::Header) + size)) {
    Buffer::Header h{seq, {}, (unsigned)size};
    GetSystemTimeAsFileTime(&h.time);
    r.write(h);
    r.write(prefix);
    r.ptr = std::vformat_to(r.ptr, fmt, args);
  }
}

unsigned global_trace_dumper::get_seq() {
  return Tracer::global_tracer.seq.fetch_add(1, std::memory_order_relaxed);
}

} // namespace std