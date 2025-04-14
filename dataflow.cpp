#include <iostream>
#include <chrono>
#include <thread>
#include <functional>
#include <atomic>
#include <jthread>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <memory>
#include <vector>
#include <string>
#include <cstdlib>
#include <algorithm>

// -----------------------
// Event Type Definition
// -----------------------
struct Event {
    int priority;           // Higher numbers indicate higher priority.
    std::string payload;    // Payload data carried with the event.

    // For use in priority_queue: events with higher priority come first.
    bool operator<(const Event& other) const {
        return priority < other.priority;
    }
};

// -----------------------
// System Monitoring
// -----------------------
class SystemMonitor {
public:
    void updateQueueSize(const std::string& nodeName, size_t size) {
        std::lock_guard<std::mutex> lock(mutex);
        queueSizes[nodeName] = size;
    }

    void incrementDroppedData(const std::string& nodeName) {
        std::lock_guard<std::mutex> lock(mutex);
        droppedData[nodeName]++;
    }

    void startMonitoring() {
        monitorThread = std::jthread([this](std::stop_token stoken) {
            while (!stoken.stop_requested()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                printStatus();
            }
            });
    }

    ~SystemMonitor() {
        if (monitorThread.joinable())
            monitorThread.request_stop();
    }

private:
    std::mutex mutex;
    std::unordered_map<std::string, size_t> queueSizes;
    std::unordered_map<std::string, int> droppedData;
    std::jthread monitorThread;

    void printStatus() {
        std::lock_guard<std::mutex> lock(mutex);
        std::cout << "\n[System Monitor] Queue Status:\n";
        for (const auto& [node, size] : queueSizes) {
            std::cout << "  " << node << ": Queue Size = " << size;
            if (droppedData[node] > 0)
                std::cout << " | Dropped: " << droppedData[node];
            std::cout << "\n";
        }
        std::cout << "------------------------------------\n";
    }
};

// -----------------------
// Graph Model Base Class
// -----------------------
class GraphNode {
public:
    virtual ~GraphNode() = default;
    virtual void execute() = 0;
};

// -----------------------
// Generator Node (Data Source)
// -----------------------
class GeneratorNode : public GraphNode {
public:
    // minRate and maxRate (in milliseconds) control dynamic rate adjustment.
    explicit GeneratorNode(std::string name, SystemMonitor& monitor, int minRate = 200, int maxRate = 1000)
        : name(std::move(name)), monitor(monitor), currentRate(minRate), minRate(minRate), maxRate(maxRate) {
    }

    void execute() override {
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(currentRate));
            // Generate an event with random priority and a payload string.
            int priority = std::rand() % 5 + 1;
            std::string payload = "Data_" + std::to_string(std::rand() % 100);
            Event ev{ priority, payload };
            pushData(ev);
        }
    }

    // Connect this generator's output to a downstream node.
    void connect(std::shared_ptr<GraphNode> node) {
        outputs.push_back(node);
    }

private:
    std::string name;
    SystemMonitor& monitor;
    std::vector<std::shared_ptr<GraphNode>> outputs;
    int currentRate, minRate, maxRate;

    void pushData(const Event& ev) {
        for (auto& output : outputs) {
            // Downstream node is assumed to be a ProcessingNode.
            auto procNode = std::dynamic_pointer_cast<class ProcessingNode>(output);
            if (procNode) {
                if (!procNode->receiveData(ev)) {
                    monitor.incrementDroppedData(name);
                    adjustRate(true);
                }
                else {
                    adjustRate(false);
                }
            }
        }
    }

    // Adjust production rate based on backpressure.
    void adjustRate(bool queueFull) {
        if (queueFull) {
            currentRate = std::min(currentRate + 100, maxRate);
        }
        else {
            currentRate = std::max(currentRate - 100, minRate);
        }
    }
};

// -----------------------
// Processing Node with Multi-Output, Caching, and Sophisticated Payload Processing
// -----------------------
class ProcessingNode : public GraphNode {
public:
    // The processing function takes an Event and returns a transformed Event.
    explicit ProcessingNode(std::string name, SystemMonitor& monitor,
        std::function<Event(const Event&)> func, int maxQueueSize = 5)
        : name(std::move(name)), monitor(monitor), func(std::move(func)),
        queueLimit(maxQueueSize), running(true) {
    }

    // Called by upstream nodes to deliver an event.
    bool receiveData(const Event& ev) {
        std::lock_guard<std::mutex> lock(queueMutex);
        if (queue.size() >= static_cast<size_t>(queueLimit)) {
            // If the incoming event has higher priority than the lowest in the queue, replace it.
            if (queue.top().priority < ev.priority) {
                queue.pop();
                queue.push(ev);
                return true;
            }
            return false;
        }
        queue.push(ev);
        queueCV.notify_one();
        return true;
    }

    void execute() override {
        processingThread = std::jthread([this](std::stop_token stoken) {
            while (!stoken.stop_requested()) {
                Event ev;
                {
                    std::unique_lock<std::mutex> lock(queueMutex);
                    queueCV.wait(lock, [this] { return !queue.empty() || !running; });
                    if (!running) return;
                    ev = queue.top();
                    queue.pop();
                    monitor.updateQueueSize(name, queue.size());
                }
                // Check the cache: if the same payload was processed before, reuse its result.
                Event result;
                {
                    auto it = cache.find(ev.payload);
                    if (it != cache.end()) {
                        result = it->second;
                        std::cout << "[Cache Hit] " << name << " serving cached result for payload '"
                            << ev.payload << "': " << result.payload << "\n";
                    }
                    else {
                        result = func(ev);
                        cache[ev.payload] = result;
                    }
                }
                pushData(result);
            }
            });
    }

    // Connect this processing node's output to a downstream node.
    void connect(std::shared_ptr<GraphNode> node) {
        outputs.push_back(node);
    }

    ~ProcessingNode() {
        running = false;
        queueCV.notify_all();
        if (processingThread.joinable())
            processingThread.request_stop();
    }

private:
    std::string name;
    SystemMonitor& monitor;
    std::function<Event(const Event&)> func;
    std::priority_queue<Event> queue;
    std::mutex queueMutex;
    std::condition_variable queueCV;
    std::atomic<bool> running;
    std::vector<std::shared_ptr<GraphNode>> outputs;
    std::jthread processingThread;
    int queueLimit;
    std::unordered_map<std::string, Event> cache; // Cache keyed by payload.

    void pushData(const Event& ev) {
        for (auto& output : outputs) {
            // Downstream node could be a ProcessingNode or a SinkNode.
            if (auto procNode = std::dynamic_pointer_cast<ProcessingNode>(output)) {
                procNode->receiveData(ev);
            }
            else if (auto sink = std::dynamic_pointer_cast<class SinkNode>(output)) {
                sink->receiveData(ev);
            }
        }
    }
};

// -----------------------
// Sink Node (Final Output)
// -----------------------
class SinkNode : public GraphNode {
public:
    void receiveData(const Event& ev) {
        std::cout << "Sink received (Priority: " << ev.priority << "): " << ev.payload << std::endl;
    }
    void execute() override {}  // No processing loop needed.
};

// -----------------------
// MAIN PROGRAM
// -----------------------
int main() {
    SystemMonitor monitor;
    monitor.startMonitoring();

    // Build the graph.
    // Create two generator nodes (simulated sensors producing events with payloads).
    auto sensorA = std::make_shared<GeneratorNode>("SensorA", monitor, 200, 1000);
    auto sensorB = std::make_shared<GeneratorNode>("SensorB", monitor, 300, 1200);

    // Create a merge processing node with sophisticated payload processing.
    // This stage enriches the payload by appending its length.
    auto mergeStage = std::make_shared<ProcessingNode>(
        "MergeStage", monitor,
        [](const Event& ev) -> Event {
            Event out;
            out.priority = ev.priority;
            out.payload = "Merge[" + ev.payload + "]|size:" + std::to_string(ev.payload.size());
            return out;
        }, 10);

    // Create two branch nodes for further sophisticated processing.
    // Branch1 converts the payload to uppercase.
    auto branch1 = std::make_shared<ProcessingNode>(
        "Branch1", monitor,
        [](const Event& ev) -> Event {
            Event out;
            out.priority = ev.priority;
            std::string upperPayload = ev.payload;
            std::transform(upperPayload.begin(), upperPayload.end(), upperPayload.begin(), ::toupper);
            out.payload = "Branch1[UPPERCASE:" + upperPayload + "]";
            return out;
        }, 5);

    // Branch2 reverses the payload and computes a simple checksum.
    auto branch2 = std::make_shared<ProcessingNode>(
        "Branch2", monitor,
        [](const Event& ev) -> Event {
            Event out;
            out.priority = ev.priority;
            std::string reversed = ev.payload;
            std::reverse(reversed.begin(), reversed.end());
            int checksum = 0;
            for (char c : ev.payload)
                checksum += c;
            out.payload = "Branch2[REVERSED:" + reversed + "|CHK:" + std::to_string(checksum) + "]";
            return out;
        }, 5);

    // Create sink nodes.
    auto sink1 = std::make_shared<SinkNode>();
    auto sink2 = std::make_shared<SinkNode>();

    // Build graph connections.
    // Both SensorA and SensorB feed into MergeStage.
    sensorA->connect(mergeStage);
    sensorB->connect(mergeStage);

    // MergeStage outputs to both Branch1 and Branch2 (multi-output).
    mergeStage->connect(branch1);
    mergeStage->connect(branch2);

    // Branch nodes output to sink nodes.
    branch1->connect(sink1);
    branch2->connect(sink2);

    // Start all nodes (graph execution).
    sensorA->execute();
    sensorB->execute();
    mergeStage->execute();
    branch1->execute();
    branch2->execute();
    sink1->execute();
    sink2->execute();

    return 0;
}
