#include "metrics_collector.hpp"
#include <fstream>
#include <sstream>
#include <filesystem>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <unistd.h>
#include <sys/sysinfo.h>
#include <random>

namespace fs = std::filesystem;

static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

static std::string read_file(const std::string& path) {
    std::ifstream f(path);
    if (!f) return "";
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

static std::string gen_agent_id() {
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(0, 15);
    const char* hex = "0123456789abcdef";
    std::string id = "agent-";
    for (int i = 0; i < 8; ++i) id += hex[dist(rng)];
    return id;
}

MetricsCollector::MetricsCollector() {
    char buf[256];
    if (gethostname(buf, sizeof(buf)) == 0) hostname_ = buf;
    else hostname_ = "unknown";
    agent_id_ = gen_agent_id();
}

double MetricsCollector::collect_cpu_total() {
    std::string content = read_file("/proc/stat");
    if (content.empty()) return 0.0;

    std::istringstream ss(content);
    std::string cpu_label;
    uint64_t user, nice, system, idle, iowait, irq, softirq, steal;
    ss >> cpu_label >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;

    uint64_t total = user + nice + system + idle + iowait + irq + softirq + steal;
    uint64_t total_idle = idle + iowait;

    uint64_t delta_total = total - prev_total_ticks_;
    uint64_t delta_idle  = total_idle - prev_idle_ticks_;

    prev_total_ticks_ = total;
    prev_idle_ticks_  = total_idle;

    if (delta_total == 0) return 0.0;
    return 100.0 * (1.0 - static_cast<double>(delta_idle) / delta_total);
}

std::vector<double> MetricsCollector::collect_cpu_per_core() {
    std::vector<double> result;
    std::string content = read_file("/proc/stat");
    std::istringstream ss(content);
    std::string line;
    while (std::getline(ss, line)) {
        if (line.rfind("cpu", 0) == 0 && line.size() > 3 && std::isdigit(line[3])) {
            std::istringstream ls(line);
            std::string label;
            uint64_t user, nice, system, idle, iowait, irq, softirq, steal;
            ls >> label >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
            uint64_t total = user + nice + system + idle + iowait + irq + softirq + steal;
            uint64_t busy  = total - idle - iowait;
            result.push_back(total > 0 ? 100.0 * busy / total : 0.0);
        }
    }
    return result;
}

std::pair<uint64_t, uint64_t> MetricsCollector::collect_memory() {
    std::string content = read_file("/proc/meminfo");
    uint64_t total = 0, available = 0;
    std::istringstream ss(content);
    std::string line;
    while (std::getline(ss, line)) {
        if (line.rfind("MemTotal:", 0) == 0) {
            std::istringstream ls(line);
            std::string k; uint64_t v;
            ls >> k >> v; total = v;
        } else if (line.rfind("MemAvailable:", 0) == 0) {
            std::istringstream ls(line);
            std::string k; uint64_t v;
            ls >> k >> v; available = v;
        }
    }
    return {total - available, total};
}

std::vector<int> MetricsCollector::discover_pids() {
    std::vector<int> pids;
    for (auto& entry : fs::directory_iterator("/proc")) {
        const auto& p = entry.path().filename().string();
        if (std::all_of(p.begin(), p.end(), ::isdigit)) {
            pids.push_back(std::stoi(p));
        }
    }
    return pids;
}

std::optional<ProcessMetrics> MetricsCollector::read_proc_stat(int pid) {
    std::string stat_path = "/proc/" + std::to_string(pid) + "/stat";
    std::string status_path = "/proc/" + std::to_string(pid) + "/status";
    std::string io_path = "/proc/" + std::to_string(pid) + "/io";

    std::string stat_content = read_file(stat_path);
    if (stat_content.empty()) return std::nullopt;

    ProcessMetrics m;
    m.pid = pid;
    m.timestamp_ms = now_ms();

    // Parse /proc/pid/stat
    std::istringstream ss(stat_content);
    std::string token;
    std::vector<std::string> fields;
    while (ss >> token) fields.push_back(token);
    if (fields.size() < 24) return std::nullopt;

    m.name = fields[1];
    if (m.name.front() == '(') m.name = m.name.substr(1, m.name.size() - 2);
    m.status = std::string(1, fields[2][0]);

    long utime = std::stol(fields[13]);
    long stime = std::stol(fields[14]);
    long num_threads = std::stol(fields[19]);
    m.num_threads = static_cast<uint32_t>(num_threads);

    double hz = sysconf(_SC_CLK_TCK);
    m.cpu_percent = 100.0 * (utime + stime) / hz / (now_ms() / 1000.0 + 1);

    // Parse /proc/pid/status for memory
    std::string status_content = read_file(status_path);
    std::istringstream ss2(status_content);
    std::string line;
    while (std::getline(ss2, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            std::istringstream ls(line); std::string k; uint64_t v;
            ls >> k >> v; m.mem_rss_kb = v;
        } else if (line.rfind("VmSize:", 0) == 0) {
            std::istringstream ls(line); std::string k; uint64_t v;
            ls >> k >> v; m.mem_vms_kb = v;
        }
    }

    // Parse /proc/pid/io for disk I/O
    std::string io_content = read_file(io_path);
    std::istringstream ss3(io_content);
    while (std::getline(ss3, line)) {
        if (line.rfind("read_bytes:", 0) == 0) {
            std::istringstream ls(line); std::string k; uint64_t v;
            ls >> k >> v; m.read_bytes = v;
        } else if (line.rfind("write_bytes:", 0) == 0) {
            std::istringstream ls(line); std::string k; uint64_t v;
            ls >> k >> v; m.write_bytes = v;
        }
    }

    return m;
}

std::vector<ProcessMetrics> MetricsCollector::collect_processes(const std::vector<int>& pids) {
    std::vector<ProcessMetrics> results;
    results.reserve(pids.size());
    for (int pid : pids) {
        auto m = read_proc_stat(pid);
        if (m) results.push_back(*m);
    }
    return results;
}

std::pair<uint64_t, uint64_t> MetricsCollector::collect_net_io() {
    std::string content = read_file("/proc/net/dev");
    uint64_t total_recv = 0, total_sent = 0;
    std::istringstream ss(content);
    std::string line;
    std::getline(ss, line); std::getline(ss, line); // skip headers
    while (std::getline(ss, line)) {
        if (line.find(':') == std::string::npos) continue;
        std::string iface = line.substr(0, line.find(':'));
        // Trim whitespace
        iface.erase(0, iface.find_first_not_of(" \t"));
        if (iface == "lo") continue;
        std::istringstream ls(line.substr(line.find(':') + 1));
        uint64_t recv, p1, p2, p3, p4, p5, p6, p7, sent;
        ls >> recv >> p1 >> p2 >> p3 >> p4 >> p5 >> p6 >> p7 >> sent;
        total_recv += recv;
        total_sent += sent;
    }
    uint64_t delta_sent = total_sent - prev_net_sent_;
    uint64_t delta_recv = total_recv - prev_net_recv_;
    prev_net_sent_ = total_sent;
    prev_net_recv_ = total_recv;
    return {delta_sent, delta_recv};
}

std::pair<uint64_t, uint64_t> MetricsCollector::collect_disk_io() {
    std::string content = read_file("/proc/diskstats");
    uint64_t total_read = 0, total_write = 0;
    std::istringstream ss(content);
    std::string line;
    while (std::getline(ss, line)) {
        std::istringstream ls(line);
        int maj, min; std::string dev;
        uint64_t rd_ios, rd_merges, rd_sectors, rd_ticks;
        uint64_t wr_ios, wr_merges, wr_sectors, wr_ticks;
        ls >> maj >> min >> dev >> rd_ios >> rd_merges >> rd_sectors >> rd_ticks
           >> wr_ios >> wr_merges >> wr_sectors >> wr_ticks;
        if (dev.rfind("sd", 0) == 0 || dev.rfind("nvme", 0) == 0 || dev.rfind("vd", 0) == 0) {
            total_read  += rd_sectors * 512;
            total_write += wr_sectors * 512;
        }
    }
    uint64_t delta_read  = total_read  - prev_disk_read_;
    uint64_t delta_write = total_write - prev_disk_write_;
    prev_disk_read_  = total_read;
    prev_disk_write_ = total_write;
    return {delta_read, delta_write};
}

SystemMetrics MetricsCollector::collect() {
    SystemMetrics s;
    s.timestamp_ms = now_ms();
    s.hostname  = hostname_;
    s.agent_id  = agent_id_;

    s.cpu_total_percent = collect_cpu_total();
    s.cpu_per_core      = collect_cpu_per_core();

    auto [mem_used, mem_total] = collect_memory();
    s.mem_used_kb   = mem_used;
    s.mem_total_kb  = mem_total;
    s.mem_available_kb = mem_total - mem_used;
    s.mem_percent   = mem_total > 0 ? 100.0 * mem_used / mem_total : 0.0;

    auto [net_sent, net_recv] = collect_net_io();
    s.net_bytes_sent = net_sent;
    s.net_bytes_recv = net_recv;

    auto [disk_read, disk_write] = collect_disk_io();
    s.disk_read_bytes  = disk_read;
    s.disk_write_bytes = disk_write;

    auto pids = discover_pids();
    s.processes = collect_processes(pids);

    return s;
}
