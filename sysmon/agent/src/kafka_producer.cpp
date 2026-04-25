#include "kafka_producer.hpp"
#include <librdkafka/rdkafka.h>
#include <stdexcept>
#include <iostream>
#include <cstring>
#include <chrono>
#include <thread>

// Delivery report callback (called by librdkafka poll thread)
static void dr_msg_cb(rd_kafka_t* /*rk*/, const rd_kafka_message_t* rkmessage, void* opaque) {
    if (!opaque) return;
    auto* producer = static_cast<KafkaProducer*>(opaque);
    std::string key;
    if (rkmessage->key && rkmessage->key_len > 0)
        key.assign(static_cast<const char*>(rkmessage->key), rkmessage->key_len);
    bool success = (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR);
    std::string err_str = success ? "" : rd_kafka_err2str(rkmessage->err);
    producer->delivery_callback(key, success, err_str);
}

KafkaProducer::KafkaProducer(const KafkaConfig& cfg) : cfg_(cfg) {
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    auto set_conf = [&](const char* key, const std::string& val) {
        if (rd_kafka_conf_set(conf, key, val.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            rd_kafka_conf_destroy(conf);
            throw std::runtime_error(std::string("Kafka conf error [") + key + "]: " + errstr);
        }
    };

    set_conf("bootstrap.servers",           cfg_.brokers);
    set_conf("compression.type",            cfg_.compression_type);
    set_conf("linger.ms",                   std::to_string(cfg_.linger_ms));
    set_conf("queue.buffering.max.messages", std::to_string(cfg_.queue_depth));
    set_conf("batch.num.messages",          std::to_string(cfg_.batch_size));
    if (cfg_.enable_idempotence)
        set_conf("enable.idempotence", "true");

    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
    rd_kafka_conf_set_opaque(conf, this);

    rk_handle_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk_handle_) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("Failed to create Kafka producer: ") + errstr);
    }

    connected_ = true;

    // Start background flush thread
    flush_thread_ = std::thread(&KafkaProducer::flush_loop, this);
}

KafkaProducer::~KafkaProducer() {
    stop_ = true;
    buf_cv_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();

    if (rk_handle_) {
        rd_kafka_flush(static_cast<rd_kafka_t*>(rk_handle_), 10000);
        rd_kafka_destroy(static_cast<rd_kafka_t*>(rk_handle_));
    }
}

bool KafkaProducer::publish(const std::string& key, const std::string& payload) {
    std::lock_guard<std::mutex> lk(buf_mutex_);
    if (static_cast<int>(buffer_.size()) >= cfg_.queue_depth) return false;
    buffer_.emplace(key, payload);
    buf_cv_.notify_one();
    return true;
}

bool KafkaProducer::publish_batch(const std::vector<std::pair<std::string, std::string>>& messages) {
    std::lock_guard<std::mutex> lk(buf_mutex_);
    for (auto& m : messages) {
        if (static_cast<int>(buffer_.size()) >= cfg_.queue_depth) return false;
        buffer_.push(m);
    }
    buf_cv_.notify_one();
    return true;
}

void KafkaProducer::flush_loop() {
    auto* rk = static_cast<rd_kafka_t*>(rk_handle_);
    while (!stop_) {
        std::unique_lock<std::mutex> lk(buf_mutex_);
        buf_cv_.wait_for(lk, std::chrono::milliseconds(cfg_.linger_ms),
            [this] { return !buffer_.empty() || stop_; });

        // Drain buffer into rdkafka
        while (!buffer_.empty()) {
            auto [key, payload] = buffer_.front();
            buffer_.pop();
            lk.unlock();

            int retries = 0;
            bool sent = false;
            while (!sent && retries <= cfg_.retry_max) {
                int rc = rd_kafka_producev(
                    rk,
                    RD_KAFKA_V_TOPIC(cfg_.topic.c_str()),
                    RD_KAFKA_V_KEY(key.c_str(), key.size()),
                    RD_KAFKA_V_VALUE(const_cast<char*>(payload.c_str()), payload.size()),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_END
                );
                if (rc == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    rd_kafka_poll(rk, 50);
                    ++retries;
                    ++stats_.retries;
                } else {
                    sent = (rc == RD_KAFKA_RESP_ERR_NO_ERROR);
                    if (!sent) ++stats_.messages_failed;
                    break;
                }
            }

            rd_kafka_poll(rk, 0); // non-blocking poll for delivery callbacks
            lk.lock();
        }
    }
}

void KafkaProducer::flush(int timeout_ms) {
    if (rk_handle_)
        rd_kafka_flush(static_cast<rd_kafka_t*>(rk_handle_), timeout_ms);
}

void KafkaProducer::delivery_callback(const std::string& /*key*/, bool success, const std::string& error) {
    if (success) {
        ++stats_.messages_sent;
    } else {
        ++stats_.messages_failed;
        std::cerr << "[KafkaProducer] delivery failed: " << error << "\n";
    }
}
