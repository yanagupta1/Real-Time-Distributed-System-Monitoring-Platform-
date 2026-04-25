#include "thread_pool.hpp"
#include "metrics_collector.hpp"
#include "kafka_producer.hpp"
#include <nlohmann/json.hpp>
#include <iostream>
#include <csignal>
#include <atomic>
#include <chrono>
#include <thread>
#include <string>
#include <vector>
#include <sstream>

using json = nlohmann::json;
static std::atomic<bool> g_running{true};

void handle_signal(int) { g_running = false; }

// Serialize SystemMetrics to JSON
json metrics_to_json(const SystemMetrics& m) {
    json j;
    j["timestamp_ms"]      = m.timestamp_ms;
    j["hostname"]          = m.hostname;
    j["agent_id"]          = m.agent_id;
    j["cpu_total_percent"] = m.cpu_total_percent;
    j["cpu_per_core"]      = m.cpu_per_core;
    j["mem_total_kb"]      = m.mem_total_kb;
    j["mem_used_kb"]       = m.mem_used_kb;
    j["mem_available_kb"]  = m.mem_available_kb;
    j["mem_percent"]       = m.mem_percent;
    j["net_bytes_sent"]    = m.net_bytes_sent;
    j["net_bytes_recv"]    = m.net_bytes_recv;
    j["disk_read_bytes"]   = m.disk_read_bytes;
    j["disk_write_bytes"]  = m.disk_write_bytes;

    json procs = json::array();
    for (const auto& p : m.processes) {
        procs.push_back({
            {"pid",         p.pid},
            {"name",        p.name},
            {"cpu_percent", p.cpu_percent},
            {"mem_rss_kb",  p.mem_rss_kb},
            {"mem_vms_kb",  p.mem_vms_kb},
            {"num_threads", p.num_threads},
            {"read_bytes",  p.read_bytes},
            {"write_bytes", p.write_bytes},
            {"status",      p.status}
        });
    }
    j["processes"] = procs;
    return j;
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    // ---------- Configuration (env-overridable) ----------
    const char* brokers_env  = std::getenv("KAFKA_BROKERS");
    const char* topic_env    = std::getenv("KAFKA_TOPIC");
    const char* interval_env = std::getenv("COLLECT_INTERVAL_MS");
    const char* threads_env  = std::getenv("AGENT_THREADS");

    std::string brokers  = brokers_env  ? brokers_env  : "localhost:9092";
    std::string topic    = topic_env    ? topic_env    : "sysmon.metrics";
    int interval_ms      = interval_env ? std::stoi(interval_env) : 1000;
    int num_threads      = threads_env  ? std::stoi(threads_env)  : 4;

    std::cout << "[SysMon Agent] Starting\n"
              << "  Brokers:  " << brokers  << "\n"
              << "  Topic:    " << topic    << "\n"
              << "  Interval: " << interval_ms << " ms\n"
              << "  Threads:  " << num_threads << "\n";

    // ---------- Kafka ----------
    KafkaConfig kafka_cfg;
    kafka_cfg.brokers          = brokers;
    kafka_cfg.topic            = topic;
    kafka_cfg.compression_type = "snappy";
    kafka_cfg.linger_ms        = 5;
    kafka_cfg.batch_size       = 50;

    KafkaProducer producer(kafka_cfg);

    // ---------- Thread pool ----------
    ThreadPool pool(static_cast<size_t>(num_threads));

    // ---------- Per-core collectors (one per thread for independent deltas) ----------
    // We use one MetricsCollector per logical thread for lock-free concurrent collection
    int num_collectors = num_threads;
    std::vector<MetricsCollector> collectors(num_collectors);

    // ---------- Collection loop ----------
    uint64_t cycle = 0;
    while (g_running) {
        auto loop_start = std::chrono::steady_clock::now();

        // Discover PIDs once per cycle, then split across threads
        std::vector<int> all_pids = collectors[0].discover_pids();
        int chunk = std::max(1, static_cast<int>(all_pids.size()) / num_collectors);

        // Submit parallel process-collection tasks
        std::vector<std::future<std::vector<ProcessMetrics>>> futures;
        for (int i = 0; i < num_collectors; ++i) {
            int start = i * chunk;
            int end   = (i == num_collectors - 1) ? static_cast<int>(all_pids.size()) : start + chunk;
            if (start >= static_cast<int>(all_pids.size())) break;

            std::vector<int> my_pids(all_pids.begin() + start, all_pids.begin() + end);
            futures.push_back(pool.enqueue([&collectors, i, my_pids]() {
                return collectors[i].collect_processes(my_pids);
            }));
        }

        // Collect system-level metrics on main thread (fast)
        SystemMetrics snap = collectors[0].collect();

        // Gather all process results
        snap.processes.clear();
        for (auto& f : futures) {
            auto procs = f.get();
            snap.processes.insert(snap.processes.end(), procs.begin(), procs.end());
        }

        // Sort top-20 by CPU descending for payload efficiency
        std::sort(snap.processes.begin(), snap.processes.end(),
            [](const ProcessMetrics& a, const ProcessMetrics& b) {
                return a.cpu_percent > b.cpu_percent;
            });
        if (snap.processes.size() > 100) snap.processes.resize(100);

        // Serialize and publish
        std::string key     = snap.agent_id + "-" + std::to_string(cycle++);
        std::string payload = metrics_to_json(snap).dump();
        producer.publish(key, payload);

        // Log stats every 10 cycles
        if (cycle % 10 == 0) {
            const auto& s = producer.stats();
            std::cout << "[cycle=" << cycle
                      << "] procs=" << snap.processes.size()
                      << " cpu=" << snap.cpu_total_percent << "%"
                      << " mem=" << snap.mem_percent << "%"
                      << " sent=" << s.messages_sent.load()
                      << " failed=" << s.messages_failed.load()
                      << "\n";
        }

        // Sleep remainder of interval
        auto elapsed = std::chrono::steady_clock::now() - loop_start;
        auto sleep_for = std::chrono::milliseconds(interval_ms) - elapsed;
        if (sleep_for > std::chrono::milliseconds(0))
            std::this_thread::sleep_for(sleep_for);
    }

    std::cout << "[SysMon Agent] Shutting down, flushing Kafka...\n";
    producer.flush(5000);
    std::cout << "[SysMon Agent] Done. Messages sent: "
              << producer.stats().messages_sent.load() << "\n";
    return 0;
}
