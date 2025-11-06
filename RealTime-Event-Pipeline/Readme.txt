Real-Time Event Pipeline
Quick Start

Build the Docker image

docker build -t zeal-pipeline:latest .


Configure docker-compose.yml

Set ports and environment variables for each Producer and Consumer.

Adjust metrics ports (e.g. 8001–8005) and database/Kafka settings as needed.

Run the full stack

docker compose up -d


Check main services

Kafka UI: http://localhost:8080

PostgreSQL: localhost:5432

Prometheus: http://localhost:9090

Grafana: http://localhost:3000
 (admin / admin)

Monitor and verify

Check producers and consumers metrics:

http://localhost:8001/metrics
http://localhost:8002/metrics


View active targets in Prometheus: http://localhost:9090/targets