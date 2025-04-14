#include <iostream>
#include <future>
#include <thread>
#include <functional>

// Stage 1: Data Source (Generates input)
std::future<int> data_source() {
    std::promise<int> p;
    auto f = p.get_future();

    std::thread([p = std::move(p)]() mutable {
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // Simulate delay
        p.set_value(42); // Generate data
        }).detach();

    return f;
}

// Stage 2: Data Processing (Transformation)
std::future<int> process_data(std::future<int> input) {
    std::packaged_task<int(int)> task([](int x) {
        return x * 2; // Simple transformation
        });

    auto f = task.get_future();
    std::thread([t = std::move(task), f = std::move(input)]() mutable {
        t(f.get()); // Wait and execute
        }).detach();

    return f;
}

// Stage 3: Data Sink (Consumes output)
void data_sink(std::future<int> result) {
    std::thread([r = std::move(result)]() mutable {
        std::cout << "Result: " << r.get() << std::endl;
        }).detach();
}

int main() {
    auto source = data_source();   // Stage 1
    auto processed = process_data(std::move(source)); // Stage 2
    data_sink(std::move(processed)); // Stage 3

    std::this_thread::sleep_for(std::chrono::seconds(2)); // Allow threads to complete
    return 0;
}
