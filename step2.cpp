#include <iostream>
#include <coroutine>
#include <optional>
#include <thread>
#include <chrono>

// C++20 generator for lazy evaluation
template <typename T>
struct Generator {
    struct promise_type {
        std::optional<T> current_value;

        Generator get_return_object() { return Generator{ std::coroutine_handle<promise_type>::from_promise(*this) }; }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        std::suspend_always yield_value(T value) noexcept {
            current_value = std::move(value);
            return {};
        }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };

    using Handle = std::coroutine_handle<promise_type>;
    Handle handle;

    explicit Generator(Handle h) : handle(h) {}
    ~Generator() { if (handle) handle.destroy(); }

    Generator(const Generator&) = delete;
    Generator& operator=(const Generator&) = delete;

    Generator(Generator&& other) noexcept : handle(other.handle) { other.handle = nullptr; }
    Generator& operator=(Generator&& other) noexcept {
        if (this != &other) {
            if (handle) handle.destroy();
            handle = other.handle;
            other.handle = nullptr;
        }
        return *this;
    }

    std::optional<T> next() {
        if (!handle || handle.done()) return std::nullopt;
        handle.resume();
        return handle.promise().current_value;
    }
};

// Stage 1: Data Source (Generates numbers lazily)
Generator<int> data_source() {
    for (int i = 1; i <= 5; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // Simulate delay
        co_yield i; // Yield values lazily
    }
}

// Stage 2: Processing (Doubles the input value)
Generator<int> process_data(Generator<int>& source) {
    while (auto val = source.next()) {
        co_yield *val * 2;
    }
}

// Stage 3: Data Sink (Consumes the pipeline lazily)
void data_sink(Generator<int>& processed) {
    while (auto result = processed.next()) {
        std::cout << "Result: " << *result << std::endl;
    }
}

int main() {
    auto source = data_source();  // Pull from data source
    auto processed = process_data(source);  // Pull from processing stage
    data_sink(processed);  // Consume lazily

    return 0;
}
