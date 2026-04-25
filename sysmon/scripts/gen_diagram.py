#!/usr/bin/env python3
"""
Generate architecture diagram for README (outputs docs/architecture.png)
Requires: pip install diagrams
"""
from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.compute import Server
from diagrams.onprem.queue import Kafka
from diagrams.onprem.inmemory import Redis
from diagrams.programming.language import Cpp, Go
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.client import Users

with Diagram("SysMon Architecture", filename="docs/architecture", show=False, direction="LR"):
    users = Users("Clients / Dashboard")

    with Cluster("Host Machine(s)"):
        with Cluster("C++ Agent (multithreaded)"):
            agent = Cpp("sysmon_agent")
            pool  = Server("ThreadPool\n(4-16 workers)")
            collector = Server("MetricsCollector\n/proc reader")
            pool >> collector >> agent

    with Cluster("Message Layer"):
        kafka = Kafka("Kafka\nsysmon.metrics\n(6 partitions)")

    with Cluster("Go Backend"):
        consumer = Go("KafkaConsumer\n(8 goroutines)")
        agg      = Go("Incremental\nAggregator")
        alerts   = Go("AlertEngine")
        api      = Go("REST API\n(Gin)")
        prom     = Prometheus("Prometheus\n/metrics")

    with Cluster("Cache Layer"):
        redis = Redis("Redis\n(LRU, 512MB)")

    agent >> Edge(label="JSON / snappy") >> kafka
    kafka >> consumer >> agg >> redis
    consumer >> alerts
    redis >> api
    alerts >> api
    api >> prom
    api >> Edge(label="HTTP/JSON") >> users

print("Architecture diagram generated: docs/architecture.png")
