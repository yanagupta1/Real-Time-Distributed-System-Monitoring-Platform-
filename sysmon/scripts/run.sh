#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker/docker-compose.yml"

usage() {
  echo "Usage: $0 {up|down|dev|logs|status|load-test}"
  exit 1
}

cmd="${1:-up}"

case "$cmd" in
  up)
    echo "🚀 Starting SysMon stack (core services)..."
    docker compose -f "$COMPOSE" up -d --build
    echo ""
    echo "✅ Stack is up!"
    echo "   API:       http://localhost:8080/api/v1/health"
    echo "   Summary:   http://localhost:8080/api/v1/summary"
    echo "   Metrics:   http://localhost:8080/metrics"
    ;;

  dev)
    echo "🔧 Starting SysMon in dev mode (with Kafka UI + Redis Insight)..."
    docker compose -f "$COMPOSE" --profile dev up -d --build
    echo ""
    echo "✅ Dev stack is up!"
    echo "   Kafka UI:      http://localhost:8090"
    echo "   Redis Insight: http://localhost:8001"
    echo "   API:           http://localhost:8080/api/v1/health"
    ;;

  down)
    echo "🛑 Stopping SysMon..."
    docker compose -f "$COMPOSE" --profile dev down -v
    ;;

  logs)
    service="${2:-backend}"
    docker compose -f "$COMPOSE" logs -f "$service"
    ;;

  status)
    echo "📊 SysMon Service Status"
    docker compose -f "$COMPOSE" ps
    echo ""
    echo "📡 API Health:"
    curl -s http://localhost:8080/api/v1/health | python3 -m json.tool 2>/dev/null || echo "API not reachable"
    ;;

  load-test)
    echo "⚡ Running load test against API..."
    if ! command -v hey &>/dev/null; then
      echo "Install hey: go install github.com/rakyll/hey@latest"
      exit 1
    fi
    hey -n 10000 -c 100 http://localhost:8080/api/v1/summary
    ;;

  *)
    usage
    ;;
esac
