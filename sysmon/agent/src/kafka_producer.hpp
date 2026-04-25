#pragma once
#include <string>
#include <functional>
#include <memory>
#include <vector>
#include <atomic>
#include <mutex>
#include <queue>
#include <thread>
#include <condition_variable>

// Thin wrapper around librdkafka (or mock-able for testing)
// Provides async, batched message publishing with retry logic

struct KafkaConfig {
    std::string brokers;
    std::string topic;
    int         batch_size        = 100;
    int         linger_ms         = 5;
    int         queue_depth        = 10000;
    int         retry_max          = 3;
    std::string compression_type   = "snappy"; // snappy | lz4 | gzip | none
    bool        enable_idempotence = true;
};

class KafkaProducer {
public:
    explicit KafkaProducer(const KafkaConfig& cfg);
    ~KafkaProducer();

    // Async publish — returns immediately, queues internally
    bool publish(const std::string& key, const std::string& payload);

    // Publish a batch
    bool publish_batch(const std::vector<std::pair<std::string, std::string>>& messages);

    // Block until all queued messages are flushed
    void flush(int timeout_ms = 5000);

    // Delivery statistics
    struct Stats {
        std::atomic<uint64_t> messages_sent{0};
        std::atomic<uint64_t> messages_failed{0};
        std::atomic<uint64_t> bytes_sent{0};
        std::atomic<uint64_t> retries{0};
    };
    const Stats& stats() const { return stats_; }

    bool is_connected() const { return connected_; }

private:
    void delivery_callback(const std::string& key, bool success, const std::string& error);
    void flush_loop();

    KafkaConfig cfg_;
    Stats stats_;
    std::atomic<bool> connected_{false};
    std::atomic<bool> stop_{false};

    // Internal buffer for batching
    std::mutex buf_mutex_;
    std::queue<std::pair<std::string,std::string>> buffer_;
    std::condition_variable buf_cv_;
    std::thread flush_thread_;

    // Opaque handle to underlying rdkafka producer
    void* rk_handle_ = nullptr;
};
