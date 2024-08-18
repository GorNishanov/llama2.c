#pragma once
#include <atomic>
#include <optional>
#include <print>
#include <exception>
#include <deque>
#include <thread>

#define NOMINMAX
#include <Windows.h>

namespace std::details {

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
  Assignment &ass;
  int batch = 1;
  int increases{};
  int decreases{};

  WorkerBrainBase(Assignment &ass) : ass(ass) {}

  ~WorkerBrainBase() {
  //  println("batch size {} +{} -{}", batch, increases, decreases);
  }
};

template <typename F> struct WorkerBrain : WorkerBrainBase {
  F &f;

  WorkerBrain(F &f, Assignment &ass) : f(f), WorkerBrainBase(ass) {}

  bool run_some() {
    if (auto a = ass.get(batch)) {
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

  Worker(F &f, Assignment &ass)
      : WorkerBase(TP_CALLBACK_PRIORITY_NORMAL, &run, this), Brain(f, ass) {}

  static void __stdcall run(PTP_CALLBACK_INSTANCE, void *ctx, PTP_WORK) {
    auto &self = *reinterpret_cast<Self *>(ctx);
    if (self.run_some())
      self.submit();
  }
};
} // namespace std::details

namespace std {
template <typename F> void bulk_schedule(int N, F f) {
  using namespace details;
  if (N < 1)
    return;
  Assignment ass{0, N};
  deque<Worker<F>> workers;
  auto n_workers = std::min<unsigned>(N, std::thread::hardware_concurrency());
  for (unsigned i = 1; i < n_workers; ++i)
    workers.emplace_back(f, ass);

  for (auto &w : workers)
    w.submit();

  WorkerBrain<F> brain(f, ass);
  while (brain.run_some())
    ;

  for (auto &w : workers)
    w.wait();
}
} // namespace std
