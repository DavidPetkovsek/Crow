#pragma once

#ifdef CROW_USE_BOOST
#include <boost/asio.hpp>
#else
#ifndef ASIO_STANDALONE
#define ASIO_STANDALONE
#endif
#include <asio.hpp>
#endif
#include <functional>
#include <memory>
#include <thread>
#include <deque>
#include <stdexcept>
#include <atomic>
#include <optional>
#include "crow/logging.h"


namespace crow
{

#ifdef CROW_USE_BOOST
    namespace asio = boost::asio;
#endif


template <typename T, typename Lock = std::mutex>
class ThreadSafeQueue {
    public:
    using value_type = T;
    using size_type = typename std::deque<T>::size_type;

    ThreadSafeQueue() = default;

    void push_back(T&& value) {
        std::scoped_lock lock(mutex_);
        data_.push_back(std::forward<T>(value));
    }

    void push_front(T&& value) {
        std::scoped_lock lock(mutex_);
        data_.push_front(std::forward<T>(value));
    }

    [[nodiscard]] bool empty() const {
        std::scoped_lock lock(mutex_);
        return data_.empty();
    }

    size_type clear() {
        std::scoped_lock lock(mutex_);
        auto size = data_.size();
        data_.clear();

        return size;
    }

    [[nodiscard]] std::optional<T> pop_front() {
        std::scoped_lock lock(mutex_);
        if (data_.empty()) return std::nullopt;

        auto front = std::move(data_.front());
        data_.pop_front();
        return front;
    }

    [[nodiscard]] std::optional<T> pop_back() {
        std::scoped_lock lock(mutex_);
        if (data_.empty()) return std::nullopt;

        auto back = std::move(data_.back());
        data_.pop_back();
        return back;
    }

    [[nodiscard]] std::optional<T> steal() {
        std::scoped_lock lock(mutex_);
        if (data_.empty()) return std::nullopt;

        auto back = std::move(data_.back());
        data_.pop_back();
        return back;
    }

    void rotate_to_front(const T& item) {
        std::scoped_lock lock(mutex_);
        auto iter = std::find(data_.begin(), data_.end(), item);

        if (iter != data_.end()) {
            std::ignore = data_.erase(iter);
        }

        data_.push_front(item);
    }

    [[nodiscard]] std::optional<T> copy_front_and_rotate_to_back() {
        std::scoped_lock lock(mutex_);

        if (data_.empty()) return std::nullopt;

        auto front = data_.front();
        data_.pop_front();

        data_.push_back(front);

        return front;
    }

    private:
    std::deque<T> data_{};
    mutable Lock mutex_{};
};

struct ThreadPool {

    explicit ThreadPool(size_t number_of_threads = std::thread::hardware_concurrency()):
        ThreadPool([](std::size_t) {}, number_of_threads)
        {}

    template <typename InitializationFunction = std::function<void(std::size_t)>>
        requires std::invocable<InitializationFunction, std::size_t> &&
                    std::is_same_v<void, std::invoke_result_t<InitializationFunction, std::size_t>>
    explicit ThreadPool(InitializationFunction init, size_t number_of_threads = std::thread::hardware_concurrency())
        : stopped_{false}, tasks_(number_of_threads) {
        if(!init) throw std::invalid_argument("Init function must be a valid function");
        if(number_of_threads <= 0) throw std::invalid_argument("Threadpool must have at least 1 thread");
        std::size_t current_id = 0;
        for (std::size_t i = 0; i < number_of_threads; ++i) {
            priority_queue_.push_back(size_t(current_id));
            try {
                threads_.emplace_back([&, id = current_id, num_threads = number_of_threads, init](const std::stop_token &stop_tok) {
                    // invoke the init function on the thread
                    try{
                        std::invoke(init, id);
                    }catch(...){
                        // suppress exceptions
                    }
                    do {
                        // wait until signaled
                        tasks_[id].signal.acquire();

                        do {
                            try{
                                // invoke as many tasks as available in our task queue
                                if (tasks_[id].tasks.poll() == 0){
                                    break;
                                }
                            }catch (std::exception& e){
                                CROW_LOG_ERROR << "Worker Crash: An uncaught exception occurred: " << e.what();
                            }

                            // try to steal a task from another queue
                            for (std::size_t j = 1; j < tasks_.size(); ++j) {
                                const std::size_t index = (id + j) % tasks_.size();
                                try{
                                    if (tasks_[index].tasks.poll_one()) {
                                        break;
                                    }
                                }catch (std::exception& e){
                                    CROW_LOG_ERROR << "Worker Crash: An uncaught exception occurred: " << e.what();
                                }
                            }
                            // check if there are any unassigned tasks before rotating to the
                            // front and waiting for more work
                        } while (unassigned_tasks_.load(std::memory_order_acquire) > 0);

                        priority_queue_.rotate_to_front(id);
                        // check if all tasks are completed and release the barrier (binary semaphore)
                        if (in_flight_tasks_.load(std::memory_order_acquire) == 0) {
                            threads_complete_signal_.store(true, std::memory_order_release);
                            threads_complete_signal_.notify_one();
                        }

                    } while (!stop_tok.stop_requested());
                });
                // increment the thread id
                ++current_id;

            } catch (...) {
                // catch all

                // remove one item from the tasks
                tasks_.pop_back();

                // remove our thread from the priority queue
                std::ignore = priority_queue_.pop_back();
            }
        }
    }

    bool stopped() const { return stopped_; }

    void stop() {
        if(stopped_) return;

        // stop all threads
        stopped_ = true;
        for (std::size_t i = 0; i < threads_.size(); ++i) {
            threads_[i].request_stop();
            tasks_[i].tasks.stop();
            tasks_[i].signal.release();
        }
        wait_for_tasks();
        for (std::size_t i = 0; i < threads_.size(); ++i) {
            threads_[i].join();
        }
    }

    ~ThreadPool() { stop(); }

    /// thread pool is non-copyable
    ThreadPool(const ThreadPool &) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;

    template <typename F>
    void post(F&& f) {
        auto i_opt = priority_queue_.copy_front_and_rotate_to_back();
        if (!i_opt.has_value()) {
            // would only be a problem if there are zero threads
            return;
        }
        // get the index
        auto i = *(i_opt);

        unassigned_tasks_.fetch_add(1, std::memory_order_release);
        const auto prev_in_flight = in_flight_tasks_.fetch_add(1, std::memory_order_release);
        if (prev_in_flight == 0) {
            threads_complete_signal_.store(false, std::memory_order_release);
        }
        asio::post(tasks_[i].tasks.get_executor(), [this, f = std::forward<F>(f)]() mutable {
            unassigned_tasks_.fetch_sub(1, std::memory_order_release);
            f(); // run the actual handler
            in_flight_tasks_.fetch_sub(1, std::memory_order_release);
        });
        tasks_[i].signal.release();
    }

    template <typename F>
    void dispatch(F&& f) {
        auto i_opt = priority_queue_.copy_front_and_rotate_to_back();
        if (!i_opt.has_value()) {
            // would only be a problem if there are zero threads
            return;
        }
        // get the index
        auto i = *(i_opt);

        unassigned_tasks_.fetch_add(1, std::memory_order_release);
        const auto prev_in_flight = in_flight_tasks_.fetch_add(1, std::memory_order_release);
        if (prev_in_flight == 0) {
            threads_complete_signal_.store(false, std::memory_order_release);
        }
        asio::dispatch(tasks_[i].tasks.get_executor(), [this, f = std::forward<F>(f)]() mutable {
            unassigned_tasks_.fetch_sub(1, std::memory_order_release);
            f(); // run the actual handler
            in_flight_tasks_.fetch_sub(1, std::memory_order_release);
        });
        tasks_[i].signal.release();
    }

    template <typename F>
    void defer(F&& f) {
        auto i_opt = priority_queue_.copy_front_and_rotate_to_back();
        if (!i_opt.has_value()) {
            // would only be a problem if there are zero threads
            return;
        }
        // get the index
        auto i = *(i_opt);

        unassigned_tasks_.fetch_add(1, std::memory_order_release);
        const auto prev_in_flight = in_flight_tasks_.fetch_add(1, std::memory_order_release);
        if (prev_in_flight == 0) {
            threads_complete_signal_.store(false, std::memory_order_release);
        }
        asio::defer(tasks_[i].tasks.get_executor(), [this, f = std::forward<F>(f)]() mutable {
            unassigned_tasks_.fetch_sub(1, std::memory_order_release);
            f(); // run the actual handler
            in_flight_tasks_.fetch_sub(1, std::memory_order_release);
        });
        tasks_[i].signal.release();
    }


    /**
        * @brief Returns the number of threads in the pool.
        *
        * @return std::size_t The number of threads in the pool.
        */
    [[nodiscard]] auto size() const { return threads_.size(); }

    /**
        * @brief Returns the number of in flight tasks being executed by the pool.
        *
        * @return std::int_fast64_t The number of in flight tasks being executed by the pool.
        */
    [[nodiscard]] auto numInflight() const { return in_flight_tasks_.load(std::memory_order_relaxed); }

    /**
        * @brief Returns the number of queued/pending/unassigned tasks the pool has not executed yet.
        *
        * @return std::int_fast64_t Returns the number of queued/pending/unassigned tasks the pool has not executed yet.
        */
    [[nodiscard]] auto queueSize() const { return unassigned_tasks_.load(std::memory_order_relaxed); }

    /**
        * @brief Wait for all tasks to finish.
        * @details This function will block until all tasks have been completed.
        */
    void wait_for_tasks() {
        if (in_flight_tasks_.load(std::memory_order_acquire) > 0) {
            // wait for all tasks to finish
            threads_complete_signal_.wait(false);
        }
    }

private:
    struct task_item {
        asio::io_context tasks{};
        std::binary_semaphore signal{0};
    };

    bool stopped_;
    std::vector<std::jthread> threads_;
    std::deque<task_item> tasks_;
    ThreadSafeQueue<std::size_t> priority_queue_;
    // guarantee these get zero-initialized
    std::atomic_int_fast64_t unassigned_tasks_{0}, in_flight_tasks_{0};
    std::atomic_bool threads_complete_signal_{false};
};

} // namespace crow