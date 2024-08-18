#pragma once
#include <atomic>
#include <optional>
#include <print>
#include <exception>
#include <execution>
#include <deque>
#include <thread>

#define NOMINMAX
#include <Windows.h>

namespace std::details {

struct Team {
  struct Partition {
    alignas(std::hardware_destructive_interference_size)
        std::atomic<uint64_t> raw{};

    union view {
      uint64_t value;
      struct {
        unsigned start : 32;
        unsigned end : 32;
      } parts;
    };

    std::optional<std::pair<int, int>> grab(unsigned batch) {
      view previous{raw.fetch_add(batch)};
      auto end = std::min(previous.parts.start + batch, previous.parts.end);
      if (previous.parts.start < end)
        return std::make_pair(+previous.parts.start, end);

      return std::nullopt;
    }

    std::optional<std::pair<int, int>> steal(unsigned batch) {
      view previous{raw.load(memory_order_relaxed)};
      for (;;) {
        view desired{previous.value};
        unsigned start = desired.parts.start;
        unsigned end = desired.parts.end;
        if (start >= end)
          return std::nullopt;

        auto available = end - start;
        desired.parts.end -= std::min(available, batch);

        if (raw.compare_exchange_weak(previous.value, desired.value))
          return std::make_pair(+desired.parts.end, end);
      }
    }
  };

  Team(unsigned s, unsigned N)
      : size(s), members(std::make_unique<Partition[]>(s)) {
    unsigned step = (N + s - 1) / s;
    unsigned current = 0;
    for (unsigned i = 0; i < s; ++i) {
      Partition::view desired;
      desired.parts.start = current;
      current = std::min(current + step, N);
      desired.parts.end = current;
      // std::println("{}: initialized to [{}..{})", i, +desired.parts.start,
      //              +desired.parts.end);
      members[i].raw.store(desired.value);
    }
  }

  std::optional<std::pair<int, int>> grab_for(unsigned i, unsigned batch) {
    if (auto p = members[i].grab(batch)) {
      //      std::println("{}: grabbed [{}..{})", i, p->first, p->second);
      return p;
    }

    for (unsigned j = (i + 1) % size; j != i; j = (j + 1) % size)
      if (auto p = members[j].steal(batch)) {
        //        std::println("{}: stole [{}..{}) from {}", i, p->first,
        //        p->second, j);
        return p;
      }

    // std::println("{}: nothing for me", i);
    return std::nullopt;
  }

private:
  unsigned size{};
  std::unique_ptr<Partition[]> members;
};

struct Assignment {
  atomic<int> current;
  int max;

  std::optional<std::pair<int, int>> get(int batch) {
    std::pair<int, int> result;
    result.first = current.fetch_add(batch, memory_order_relaxed);
    result.second = min(result.first + batch, max);
    if (result.second > result.first)
      return result;

    return nullopt;
  }
};

class WorkerBase {
  struct Env : TP_CALLBACK_ENVIRON {
    Env(TP_CALLBACK_PRIORITY pri) {
      InitializeThreadpoolEnvironment(this);
      SetThreadpoolCallbackPriority(this, pri);
    }
  };

  alignas(std::hardware_destructive_interference_size) Env env;
  PTP_WORK handle{};

public:
  WorkerBase(TP_CALLBACK_PRIORITY pri, PTP_WORK_CALLBACK cb, void *ctx)
      : env(pri), handle(CreateThreadpoolWork(cb, ctx, &env)) {
    if (!handle)
      throw std::runtime_error("failed to create threadpool work");
  }

  WorkerBase(const WorkerBase &) = delete;
  WorkerBase &operator=(const WorkerBase &) = delete;

  void wait() { WaitForThreadpoolWorkCallbacks(handle, false); }

  ~WorkerBase() noexcept {
    WaitForThreadpoolWorkCallbacks(handle, true);
    CloseThreadpoolWork(handle);
  }

  void submit() const noexcept { SubmitThreadpoolWork(handle); }
};

struct WorkerBrainBase {
  Team &team;
  unsigned no{};
  int batch = 1;
  int increases{};
  int decreases{};

  auto get(int b) { return team.grab_for(no, b);
  }

  WorkerBrainBase(Team &t, unsigned no) : team(t), no(no) {}
};

template <typename F> struct WorkerBrain : WorkerBrainBase {
  F &f;

  WorkerBrain(F &f, Team &t, unsigned no) : f(f), WorkerBrainBase(t, no) {}

  bool run_some() {
    if (auto a = get(batch)) {
      auto started = std::chrono::steady_clock::now();
      auto start = a->first;
      auto end = a->second;
#pragma loop(ivdep)
      for (int i = start; i < end; ++i) {
        f(i);
      }
      auto elapsed = std::chrono::steady_clock::now() - started;
      if (elapsed < 5ms) {
        batch += batch;
        increases++;
      } else if (elapsed > 10ms) {
        batch = std::min(1, batch / 2);
        decreases++;
      }
      return true;
    }
    return false;
  }
};

template <typename F> struct Worker : WorkerBase, WorkerBrain<F> {
  using Brain = WorkerBrain<F>;
  using Self = Worker<F>;

  Worker(F &f, Team &t, unsigned no)
      : WorkerBase(TP_CALLBACK_PRIORITY_NORMAL, &run, this), Brain(f, t, no) {}

  static void __stdcall run(PTP_CALLBACK_INSTANCE, void *ctx, PTP_WORK) {
    auto &self = *reinterpret_cast<Self *>(ctx);
    if (self.run_some())
      self.submit();
  }
};
} // namespace std::details

namespace std {
template <typename F> void bulk_schedule_first(int N, F f) {
  using namespace details;
  if (N < 1)
    return;
  //Assignment ass{0, N};
  auto n_workers = std::min<unsigned>(N, std::thread::hardware_concurrency());
  Team team(n_workers, (unsigned)N);

  deque<Worker<F>> workers;
  for (unsigned i = 1; i < n_workers; ++i)
    workers.emplace_back(f, team, i);

  for (auto &w : workers)
    w.submit();

  WorkerBrain<F> brain(f, team, 0);
  brain.batch = std::max<unsigned>(N / n_workers, 1);
  while (brain.run_some())
    ;

  for (auto &w : workers)
    w.wait();
}
} // namespace std


namespace std {
template <typename F>
void bulk_schedule_17(int N, F f) {
    // Custom iterator to generate range [0, N)
    class RangeIterator {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = int;
        using difference_type = std::ptrdiff_t;
        using pointer = int*;
        using reference = int&;

        RangeIterator() : value(0) {} // Default constructor
        RangeIterator(int value) : value(value) {}

        int operator*() const { return value; }
        RangeIterator& operator++() { ++value; return *this; }
        RangeIterator operator++(int) { RangeIterator tmp = *this; ++(*this); return tmp; }
        bool operator==(const RangeIterator& other) const { return value == other.value; }
        bool operator!=(const RangeIterator& other) const { return value != other.value; }
        difference_type operator-(const RangeIterator& other) const { return value - other.value; }
        RangeIterator operator+(difference_type n) const { return RangeIterator(value + n); }
        RangeIterator& operator+=(difference_type n) { value += n; return *this; }

    private:
        int value;
    };

    RangeIterator begin(0);
    RangeIterator end(N);

    std::for_each(std::execution::par_unseq, begin, end, f);
}
} // namespace std


