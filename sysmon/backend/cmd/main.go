package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/yourusername/sysmon/internal/alert"
	"github.com/yourusername/sysmon/internal/api"
	kafkaconsumer "github.com/yourusername/sysmon/internal/kafka"
	"github.com/yourusername/sysmon/internal/metrics"
	rediscache "github.com/yourusername/sysmon/internal/redis"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	// ---------- Logger ----------
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("SysMon Backend starting")

	// ---------- Config ----------
	kafkaBrokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic   := getenv("KAFKA_TOPIC", "sysmon.metrics")
	redisAddr    := getenv("REDIS_ADDR", "localhost:6379")
	redisPass    := getenv("REDIS_PASSWORD", "")
	listenAddr   := getenv("LISTEN_ADDR", ":8080")

	// ---------- Redis ----------
	cache := rediscache.New(redisAddr, redisPass, 0, logger)
	if err := cache.Health(context.Background()); err != nil {
		logger.Fatal("Redis connection failed", zap.Error(err))
	}
	logger.Info("Redis connected", zap.String("addr", redisAddr))
	defer cache.Close()

	// ---------- Alert Engine ----------
	alertEngine := alert.NewEngine(logger)
	for _, rule := range alert.DefaultRules() {
		alertEngine.UpsertRule(rule)
	}
	logger.Info("Alert engine ready", zap.Int("rules", len(alertEngine.ListRules())))

	// ---------- Kafka Consumer ----------
	kafkaCfg := kafkaconsumer.DefaultConsumerConfig()
	kafkaCfg.Brokers = kafkaBrokers
	kafkaCfg.Topic   = kafkaTopic
	kafkaCfg.NumWorkers = 8

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := kafkaconsumer.NewConsumer(kafkaCfg,
		func(ctx context.Context, snap *metrics.SystemSnapshot) error {
			if err := cache.Ingest(ctx, snap); err != nil {
				logger.Error("cache ingest failed", zap.Error(err))
			}
			alertEngine.Evaluate(ctx, snap)
			return nil
		},
		logger,
	)
	consumer.Start(ctx)
	logger.Info("Kafka consumer started",
		zap.Strings("brokers", kafkaBrokers),
		zap.String("topic", kafkaTopic),
		zap.Int("workers", kafkaCfg.NumWorkers),
	)

	// ---------- HTTP Server ----------
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logger.Info("request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
		)
	})

	// CORS for dashboard
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type,Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	h := api.NewHandler(cache, alertEngine, logger)
	h.RegisterRoutes(r)

	// Prometheus metrics endpoint
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	srv := &http.Server{Addr: listenAddr, Handler: r}

	go func() {
		logger.Info("HTTP server listening", zap.String("addr", listenAddr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error", zap.Error(err))
		}
	}()

	// ---------- Graceful shutdown ----------
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutdown signal received")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	consumer.Stop()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", zap.Error(err))
	}
	logger.Info("SysMon Backend stopped")
}
