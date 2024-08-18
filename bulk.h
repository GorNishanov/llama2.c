#pragma once

#include <array>
#include <atomic>
#include <print>
#include <string_view>
#include <thread>
#include <chrono>

#define NOMINMAX
#include <Windows.h>

namespace std::chrono
{
    struct fast_clock
    { // wraps GetSystemTimeAsFileTime
        using rep = long long;
        using period = ratio<1, 10'000'000>; // 100 nanoseconds
        using duration = ::std::chrono::duration<rep, period>;
        using time_point = ::std::chrono::time_point<fast_clock>;
        static constexpr bool is_steady = false;

        [[nodiscard]] static time_point now() noexcept
        { // get current time
            __int64 ticks;
            GetSystemTimeAsFileTime((LPFILETIME)&ticks);
            return time_point(duration(ticks));
        }
    };
} // namespace std::chrono

namespace std::details
{

    struct partitioner
    {
        alignas(std::hardware_destructive_interference_size)
            std::atomic<int> active_workers{};

        std::atomic<bool> done{true};

        PTP_WORK_CALLBACK cb{};
        unsigned N{};
        void *context{};
        std::chrono::high_resolution_clock::time_point started;

        struct assignment
        {
            unsigned start{};
            unsigned end{};

            unsigned size() const { return end - start; }

            bool empty() const { return size() == 0; }

            explicit operator bool() const { return !empty(); }

            assignment consume(unsigned batch)
            {
                auto result = *this;
                auto new_start = start + batch;
                if (new_start >= end)
                {
                    start = end;
                }
                else
                {
                    result.end = new_start;
                    start = new_start;
                }
                return result;
            }
        };

        struct common_data
        {
            partitioner *parent{};
            void *context{};
            unsigned total{}; // total number of partitions
        };

        struct partition
        {
            alignas(std::hardware_destructive_interference_size)
                std::atomic<uint64_t> raw{0};
#pragma warning(disable : 4324) // structure was padded
            alignas(std::hardware_destructive_interference_size) unsigned batch{1};
            unsigned batch_increases{};
            unsigned batch_decreases{};
            common_data common;
            unsigned no{}; // my number

            union view
            {
                uint64_t value;
                assignment parts;

                view(uint64_t v) : value{v} {}
                view(assignment a) : parts{a} {}
            };

            assignment start(unsigned total, partitioner &parent)
            {
                common.context = parent.context;
                common.total = total;
                common.parent = &parent;

                parent.started = std::chrono::high_resolution_clock::now();
                log("----- start bulk_schedule {}", parent.N);

                batch = std::max(1u, my_fair_share() / total);
                return install_work_and_start_another_worker({0, parent.N}, view{0});
            }

            partition *begin() { return this - no; }

#if 0
            template <class... Args>
            void log(std::format_string<Args...> fmt, Args &&...args)
            {
                auto elapsed = duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - common.parent->started);
                std::println("{:x}::{} {} {}", GetCurrentThreadId(), elapsed, no, std::vformat(fmt.get(), std::make_format_args(args...)));
            }
#else
            template <class... Args>
            void log(std::format_string<Args...>, Args &&...) {}
#endif

            assignment trace(assignment a, std::string_view reason, int who = -1)
            {
                log("[{}..{}): {}{}", a.start, a.end, reason,
                    who != -1 ? std::format(" by {}", who) : "");
                return a;
            }

            int adjust_worker_count(int diff, std::string_view reason)
            {
                auto count = common.parent->active_workers.fetch_add(diff);
                log("workers {} => {}: {}", count,
                    count + diff, reason);
                return count + diff;
            }

            void increment_workers(std::string_view reason)
            {
                if (adjust_worker_count(1, reason) == 1)
                    common.parent->done.store(false);
            }

            void decrement_workers(std::string_view reason)
            {
                if (adjust_worker_count(-1, reason) == 0)
                {
                    common.parent->done.store(true);
                    common.parent->done.notify_all();
                }
            }

            auto start_batch() const { return std::chrono::fast_clock::now(); }

            bool end_batch(std::chrono::fast_clock::time_point started)
            {
                bool done = false;
                auto elapsed = start_batch() - started;
                auto old_batch = batch;
                if (elapsed < 15ms)
                {
                    batch += batch;
                    batch_increases++;
                }
                else if (elapsed > 20ms)
                {
                    batch = std::min<unsigned>(1, batch / 2);
                    batch_decreases++;
                }
                log("end_batch => {}, ({} => {})", done, old_batch, batch);
                return done;
            }

            assignment install_work_and_start_another_worker(assignment a,
                                                             view previous)
            {
                auto result = a.consume(batch);

                for (;;)
                {
                    view desired{a};
                    if (raw.compare_exchange_weak(previous.value, desired.value))
                        break;
                }

                trace(a, "installed remaining work");

                if (a.empty())
                    return trace(result, "consumed entire fair share in one go");

                if (no + 1 < common.total)
                {
                    auto &next = *(this + 1);
                    next.common = common;
                    next.no = no + 1;

                    // TODO: this goes to cpp
                    increment_workers("starting new worker");
                    if (auto p = CreateThreadpoolWork(common.parent->cb, &next, nullptr))
                    {
                        common.parent->done.store(false);
                        SubmitThreadpoolWork(p);
                    }
                    else
                    {
                        decrement_workers("failed to start a new worker");
                        common.parent->active_workers.fetch_sub(1);
                    }
                }

                return trace(result, "consumed a batch from my fair share");
            }

            unsigned my_fair_share() const
            {
                return (common.parent->N + common.total - 1) / common.total;
            }

            assignment grab_initial_work(view previous)
            {
                if (auto p = begin()->steal(my_fair_share(), no))
                {
                    // if (auto incr = begin()->batch_increases; incr > 0)
                    // {
                    //     this->batch = begin()->batch / 2;
                    //     log("given that main already had {} batch increases, upgrade starting batch to {}", incr, this->batch);
                    // }

                    return install_work_and_start_another_worker(p, previous);
                }

                return trace({}, "grab_initial_work");
            }

            assignment steal(unsigned how_much, unsigned who)
            {
                view previous{raw.load(memory_order_relaxed)};
                for (;;)
                {
                    view desired{previous.value};
                    unsigned start = desired.parts.start;
                    unsigned end = desired.parts.end;
                    if (start >= end)
                        return {};

                    auto available = end - start;
                    desired.parts.end -= std::min(available, how_much);

                    if (raw.compare_exchange_weak(previous.value, desired.value))
                        return trace({desired.parts.end, end}, "stolen", who);
                }
            }

            assignment steal_work_from_somebody_else()
            {
                auto members = begin();
                for (unsigned j = (no + 1) % common.total; j != no;
                     j = (j + 1) % common.total)
                    if (auto p = members[j].steal(batch, no))
                        return p;

                return trace({}, "nothing to steal");
            }

            assignment grab()
            {
                view previous{raw.fetch_add(batch)};
                auto end = std::min(previous.parts.start + batch, previous.parts.end);

                if (end == 0)
                    return grab_initial_work({previous.value + batch});

                if (previous.parts.start < end)
                    return trace({previous.parts.start, end}, "grab");

                return steal_work_from_somebody_else();
            }
        };
    };
} // namespace std::details

namespace std
{
    template <typename F>
    void bulk_schedule_second(unsigned N, F f)
    {
        using namespace std::details;

        std::array<partitioner::partition, 64> partitions;
        partitioner p;
        p.N = N;
        p.context = &f;
        p.cb = [](PTP_CALLBACK_INSTANCE, void *context, PTP_WORK work)
        {
            auto &me = *reinterpret_cast<partitioner::partition *>(context);
            auto started = me.start_batch();
        grab_again:
            if (auto a = me.grab())
            {
                auto &f = *reinterpret_cast<F *>(me.common.context);

#pragma loop(ivdep)
                for (auto i = a.start; i < a.end; ++i)
                    f(i);

                if (!me.end_batch(started))
                    ; //goto grab_again;

                return SubmitThreadpoolWork(work);
            }
            CloseThreadpoolWork(work);
            me.decrement_workers("no more work");
        };

        auto &me = partitions[0];
        auto a = me.start(std::thread::hardware_concurrency(), p);
        while (a)
        {

            auto started = me.start_batch();
#pragma loop(ivdep)
            for (auto i = a.start; i < a.end; ++i)
                f(i);
            me.end_batch(started);

            a = me.grab();
        }

        me.log("----- start wait {}", p.active_workers.load());
        p.done.wait(false);
        me.log("----- wait done: {}", p.active_workers.load());
    }
} // namespace std