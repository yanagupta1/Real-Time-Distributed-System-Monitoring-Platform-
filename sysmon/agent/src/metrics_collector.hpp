#pragma once
#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <chrono>

struct ProcessMetrics {
    int pid;
    std::string name;
    double cpu_percent;
    uint64_t mem_rss_kb;
    uint64_t mem_vms_kb;
    uint32_t num_threads;
    uint64_t read_bytes;
    uint64_t write_bytes;
    std::string status;
    int64_t timestamp_ms;
};

struct SystemMetrics {
    double cpu_total_percent;
    std::vector<double> cpu_per_core;
    uint64_t mem_total_kb;
    uint64_t mem_used_kb;
    uint64_t mem_available_kb;
    double mem_percent;
    uint64_t swap_total_kb;
    uint64_t swap_used_kb;
    uint64_t net_bytes_sent;
    uint64_t net_bytes_recv;
    uint64_t disk_read_bytes;
    uint64_t disk_write_bytes;
    std::vector<ProcessMetrics> processes;
    int64_t timestamp_ms;
    std::string hostname;
    std::string agent_id;
};

class MetricsCollector {
public:
    MetricsCollector();

    // Collect full system snapshot
    SystemMetrics collect();

    // Collect CPU metrics only (fast path)
    double collect_cpu_total();
    std::vector<double> collect_cpu_per_core();

    // Collect memory metrics
    std::pair<uint64_t, uint64_t> collect_memory(); // (used_kb, total_kb)

    // Collect per-process metrics for a given PID list
    std::vector<ProcessMetrics> collect_processes(const std::vector<int>& pids);

    // Discover all running PIDs
    std::vector<int> discover_pids();

    // Network and disk I/O snapshots
    std::pair<uint64_t, uint64_t> collect_net_io();   // (sent, recv)
    std::pair<uint64_t, uint64_t> collect_disk_io();  // (read, write)

private:
    std::optional<ProcessMetrics> read_proc_stat(int pid);
    double parse_cpu_from_stat(const std::string& line);
    uint64_t parse_mem_value(const std::string& line);

    // Previous CPU tick snapshots for delta calculations
    uint64_t prev_total_ticks_ = 0;
    uint64_t prev_idle_ticks_  = 0;
    std::string hostname_;
    std::string agent_id_;

    // Snapshot for delta I/O
    uint64_t prev_net_sent_  = 0;
    uint64_t prev_net_recv_  = 0;
    uint64_t prev_disk_read_ = 0;
    uint64_t prev_disk_write_= 0;
};
