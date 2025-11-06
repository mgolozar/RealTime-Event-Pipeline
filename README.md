#  Data Pipeline

A real-time data processing pipeline that ingests user events from Kafka, transforms and aggregates them, and stores the results in PostgreSQL. The system includes comprehensive monitoring with Prometheus metrics and Grafana dashboards.


## Overview

This pipeline processes user events (page views, clicks, purchases, logins, etc.) in real-time:

1. **Event Producer**: Simulates user events and publishes them to Kafka
2. **Event Consumer**: Consumes events from Kafka, validates/transforms them, and aggregates them by hour
3. **Storage**: Stores raw events and hourly aggregations in PostgreSQL
4. **Monitoring**: Exposes Prometheus metrics and provides Grafana dashboards for observability


Please check real_time_pipeline_vector.drawio


### Key Features

- **Real-time Processing**: Kafka-based event streaming with batch processing
- **Data Aggregation**: Hourly aggregations with metrics (total events, unique users, average duration)
- **Fault Tolerance**: Connection pooling, error handling, and dead letter queue tracking
- **Observability**: Comprehensive metrics for events, processing latency, database operations, and consumer lag
- **Scalability**: Designed to handle high-throughput event processing

## Architecture

Here's how the system works in simple terms:

**The Flow:**
1. **Event Producer** simulates user activity (like page views, clicks, purchases) and sends these events to Kafka
2. **Kafka** acts as a message queue, reliably storing events until they're processed
3. **Event Consumer** picks up events from Kafka, validates them, groups them by hour, and saves the results
4. **PostgreSQL** stores everything - the raw events, the hourly summaries, and performance metrics
5. **Prometheus & Grafana** watch everything and give you dashboards to see how things are running

**Why this design?**
- Kafka ensures we don't lose events even if the consumer is busy or down
- Processing in batches makes things faster and reduces database load
- Hourly aggregations make it easy to query "how many page views did we have last hour?" without scanning millions of events
- The monitoring stack helps you spot problems before they become big issues

### Component Details

**Producer** (`main_producer.py`)
- Creates realistic user events that mimic real website activity
- You can control how fast events are generated (great for testing different load levels)
- Exposes metrics so you can see how many events it's creating

**Consumer** (`main_consumer.py`)
- Reads events from Kafka in batches (default: 100 at a time)
- Validates each event to make sure it has all the required fields
- Groups events by hour and event type (e.g., "page_view events from 2pm-3pm")
- Saves the summaries to the database periodically
- Tracks its own performance and health

**Kafka Cluster**
- Three Kafka brokers working together for reliability
- If one broker goes down, the others keep things running
- Uses Zookeeper (also 3 nodes) to coordinate everything

**PostgreSQL Database**
- `raw_events` table: Every single event that comes through (optional, can be disabled)
- `hourly_aggregations` table: The summarized data (total events, unique users, average duration per hour)
- `pipeline_metrics` table: Internal performance data (how fast we're processing, etc.)

**Monitoring Stack**
- **Prometheus**: Collects metrics from the producer and consumer every few seconds
- **Grafana**: Beautiful dashboards showing event rates, processing times, errors, and more
- Pre-configured alerts notify you if something goes wrong (too many errors, consumer falling behind, etc.)

## Prerequisites

### Required Software

- **Docker** and **Docker Compose** (v3.8+)
- **Python** 3.11 or 3.12
- **pip** (Python package manager)
- **PgAdmin** (Work with database)

### System Requirements

- **RAM**: Minimum 4GB (8GB recommended for all services)
- **Disk Space**: At least 5GB free space
- **Ports**: The following ports must be available:
  - `2181-2183`: Zookeeper
  - `9091-9093`: Kafka brokers
  - `5432`: PostgreSQL
  - `8001`: Producer metrics
  - `8002`: Consumer metrics
  - `9090`: Prometheus
  - `3000`: Grafana
  - `8080`: Kafka UI (optional)

## Quick Start

1. **Clone and navigate to the project directory**

2. **Set up environment variables**:
   ```bash
   cp config.example.env .env
   # Edit .env if needed (defaults work for local development - please check database parameter)
   ```

3. **Start infrastructure services**:
   ```bash
   docker-compose up -d
   ```

4. **Wait for services to be ready** (about 30-60 seconds):
   ```bash
   # Check service health (check all of Containers are run)
   docker-compose ps
   ```

5. **Install Python dependencies**:
   ```bash
   pip install -e .
   ```

6. **Run the producer** (in one terminal):
   ```bash
   python main_producer.py
   ```

7. **Run the consumer** (in another terminal):
   ```bash
   python main_consumer.py
   ```

8. **Access monitoring**:
   - Grafana: http://localhost:3000 (admin/admin)
   - Prometheus: http://localhost:9090
   - Kafka UI: http://localhost:8080

## Detailed Setup Instructions

### Step 1: Environment Configuration

Create a `.env` file from the example:

```bash
cp config.example.env .env
```

The `.env` file contains all configuration. Key settings:

- **Kafka**: Bootstrap servers, topic name, consumer group
- **Database**: Connection details (host, port, credentials)
- **Producer**: Event generation intervals and user count
- **Consumer**: Batch size and flush intervals
- **Monitoring**: Metrics ports and Prometheus URL

### Step 2: Start Infrastructure with Docker Compose

The `docker-compose.yml` file defines all infrastructure services:

```bash
 
docker-compose up -d

 
docker-compose logs -f

 
docker-compose ps
```

**Services started:**
- 3x Zookeeper nodes (for Kafka coordination)
- 3x Kafka brokers (for message streaming)
- PostgreSQL (for data storage)
- Prometheus (for metrics collection)
- Grafana (for visualization)
- Kafka UI (optional, for Kafka management)

**Wait for services to be healthy:**
- Zookeeper: ~10 seconds
- Kafka: ~20-30 seconds (depends on Zookeeper)
- PostgreSQL: ~5-10 seconds
- Prometheus/Grafana: ~10 seconds

 

### Step 3: Database Schema Setup

The database schema is automatically created when PostgreSQL starts (via the `migrations/001_initial_schema.sql` file mounted in the container).

**Tables created:**
- `raw_events`: Stores all incoming events
- `hourly_aggregations`: Stores aggregated metrics by hour and event type
- `pipeline_metrics`: Stores internal pipeline performance metrics

You can verify the schema:

```bash
docker exec -it postgres psql -U pipeline_user -d data_pipeline -c "\dt"
```

### Step 4: Install Python Dependencies

Install the project and its dependencies:

```bash
 
pip install -e .


```

**Key dependencies:**
- `kafka-python`: Kafka client
- `psycopg2-binary`: PostgreSQL adapter
- `prometheus-client`: Metrics export
- `pydantic`: Data validation
- `python-dotenv`: Environment variable management

### Step 5: Create Kafka Topic

**Important:** While Kafka brokers can auto-create topics when first used (if `auto.create.topics.enable=true`, which is often the default), **it's better to create the topic manually** with proper settings. Auto-created topics typically use default settings (1 partition, replication factor 1), which aren't ideal for production.

**Recommended:** Create the topic manually with proper configuration:

```bash
docker exec -it kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic user-events --partitions 3 --replication-factor 3
```

**Why manual creation is better:**
- You control the number of partitions (affects parallelism and throughput)
- You set the replication factor (affects durability and availability)
- You can configure retention, compression, and other settings upfront

**If you skip this step:**
- The topic will be auto-created when the producer first sends a message (if auto-creation is enabled)
- But it will use default settings which may not be optimal
- You can verify the topic exists: `docker exec -it kafka1 kafka-topics --bootstrap-server localhost:29092 --list`

## Running Components

### Running the Producer

The producer simulates user events and publishes them to Kafka.

```bash
python main_producer.py
```

**What it does:**
- Generates events with configurable intervals c
- Publishes events to the Kafka topic `user-events`
- Exposes Prometheus metrics on port 8001
- Logs event generation progress

**Configuration (via `.env`):**
- `PRODUCER_MIN_INTERVAL`: Minimum time between events (seconds)
- `PRODUCER_MAX_INTERVAL`: Maximum time between events (seconds)
- `PRODUCER_USER_COUNT`: Number of simulated users
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_TOPIC`: Topic name (default: `user-events`)

 

**Stopping the Producer:**
- Press `Ctrl+C` to gracefully shutdown

### Running the Consumer

The consumer processes events from Kafka, transforms them, aggregates them, and stores results.

```bash
python main_consumer.py
```

**What it does:**
- Consumes events from Kafka in batches
- Validates and transforms events using Pydantic
- Aggregates events into hourly buckets
- Flushes completed aggregations to PostgreSQL
- Exposes Prometheus metrics on port 8002
- Monitors consumer lag and processing latency

**Configuration (via `.env`):**
- `CONSUMER_BATCH_SIZE`: Number of events to process per batch (default: 100)
- `FLUSH_INTERVAL_SECONDS`: How often to flush completed aggregations (default: 60)
- `AGGREGATOR_FLUSH_INTERVAL_MINUTES`: Aggregation window (default: 5)
- `SAVE_RAW_EVENTS`: Whether to store raw events in database (default: true)
- `SAVE_PIPELINE_METRICS`: Whether to store pipeline metrics (default: true)

**Processing Flow:**
1. **Consume**: Fetch batch of events from Kafka
2. **Transform**: Validate and enrich events (add processed timestamp, flag high-value purchases)
3. **Aggregate**: Add events to hourly buckets (by event type)
4. **Flush**: Periodically write completed aggregations to database
5. **Monitor**: Track metrics and check for alerts

**Stopping the Consumer:**
- Press `Ctrl+C` to gracefully shutdown
- The consumer will flush all pending aggregations before exiting

### Running Both Components

For a complete pipeline, run both components simultaneously:

**Terminal 1 (Producer):**
```bash
python main_producer.py
```

**Terminal 2 (Consumer):**
```bash
python main_consumer.py
```

 

## Data Source Simulation

The `EventSimulator` class generates realistic user events to simulate a production data source.

### Event Generation Strategy

**Timing Model:**
- Uses exponential distribution for realistic event spacing
- Configurable min/max intervals (default: 0.1-5.0 seconds)
- Simulates irregular, bursty traffic patterns

**Event Distribution:**
- Weighted random selection based on realistic user behavior
- Page views and clicks are most common (70% combined)
- Purchases and auth events are less frequent (15% combined)

**Event Structure:**
Each event contains:
```json
{
  "event_id": "page_view_1705312800000_1234",
  "user_id": "user_42",
  "event_type": "page_view",
  "timestamp": "2024-01-15T10:00:00.000Z",
  "event_data": {
    "session_id": "session_12345",
    "ip_address": "192.168.1.1",
    "user_agent": "Mozilla/5.0...",
    "page_url": "/page/home",
    "referrer": "google.com"
  },
  "duration_seconds": 2.5
}
```

**Event-Specific Data:**
- **Page View**: URL, referrer
- **Click**: Element ID, element type
- **Purchase**: Order ID, amount, currency, item count
- **Search**: Query string, results count
- **Login/Logout**: Authentication method

### Customizing Event Generation

Edit `src/producer/simulator.py` or adjust environment variables:

```bash
# Generate events faster (more load)
PRODUCER_MIN_INTERVAL=0.05
PRODUCER_MAX_INTERVAL=2.0

# Simulate more users
PRODUCER_USER_COUNT=5000

# Generate events slower (less load)
PRODUCER_MIN_INTERVAL=1.0
PRODUCER_MAX_INTERVAL=10.0
```

### Verifying Event Generation

 
## Configuration

### Environment Variables

All configuration is managed through environment variables (loaded from `.env` file).

#### Kafka Configuration
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9091,localhost:9092,localhost:9093
KAFKA_TOPIC=user-events
KAFKA_CONSUMER_GROUP=data-pipeline-consumer
```

#### Database Configuration
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=data_pipeline
DB_USER=pipeline_user
DB_PASSWORD=pipeline_password
DB_MIN_CONNECTIONS=2
DB_MAX_CONNECTIONS=10
```

#### Producer Configuration
```bash
PRODUCER_MIN_INTERVAL=0.1      # Minimum seconds between events
PRODUCER_MAX_INTERVAL=5.0      # Maximum seconds between events
PRODUCER_USER_COUNT=1000       # Number of simulated users
```

#### Consumer Configuration
```bash
CONSUMER_BATCH_SIZE=100                    # Events per batch
AGGREGATOR_FLUSH_INTERVAL_MINUTES=5        # Aggregation window
FLUSH_INTERVAL_SECONDS=60                  # Flush frequency
SAVE_RAW_EVENTS=true                       # Store raw events
SAVE_PIPELINE_METRICS=true                 # Store pipeline metrics
```

#### Monitoring Configuration
```bash
METRICS_PORT_PRODUCER=8001
METRICS_PORT_CONSUMER=8002
PROMETHEUS_URL=http://localhost:9090
```

#### Alert Thresholds
```bash
ALERT_ERROR_RATE_THRESHOLD=0.05            # 5% error rate
ALERT_CONSUMER_LAG_THRESHOLD=1000          # Messages behind
ALERT_PROCESSING_LATENCY_THRESHOLD=5.0     # Seconds
```

### Docker Compose Configuration

The `docker-compose.yml` file can be customized for:
- Resource limits (CPU, memory)
- Port mappings
- Volume mounts
- Network configuration

**Important ports:**
- Kafka brokers: `9091`, `9092`, `9093` (external), `29092` (internal)
- Zookeeper: `2181`, `2182`, `2183`
- PostgreSQL: `5432`
- Prometheus: `9090`
- Grafana: `3000`
- Kafka UI: `8080`

## Monitoring and Observability

### Prometheus Metrics

Both producer and consumer expose Prometheus metrics:

**Producer Metrics (port 8001):**
- `events_produced_total`: Total events published
- `bytes_total`: Bytes transferred
- `event_size_bytes`: Distribution of event sizes
- `kafka_produce_errors_total`: Publishing errors

**Consumer Metrics (port 8002):**
- `events_consumed_total`: Total events consumed
- `events_processed_total`: Successfully processed events
- `events_failed_total`: Failed events (by error type)
- `event_processing_duration_seconds`: Processing latency histogram
- `event_transformation_duration_seconds`: Transformation time
- `aggregation_duration_seconds`: Aggregation time
- `db_inserts_total`: Database insert operations
- `db_insert_duration_seconds`: Database write latency
- `kafka_consumer_lag`: Consumer lag per topic/partition
- `last_event_processed_unixtime`: Timestamp of last processed event

**Accessing Metrics:**
```bash
# Producer metrics
curl http://localhost:8001/metrics

# Consumer metrics
curl http://localhost:8002/metrics

#Alert data
curl http://localhost:9090/api/v1/alerts

```

### Grafana Dashboards

A pre-configured dashboard is available at `grafana/dashboards/pipeline_dashboard.json`.

**Dashboard Panels:**
- Event throughput (produced/consumed/processed)
- Processing latency (p50, p95, p99)
- Error rates and types
- Consumer lag
- Database operations (inserts, latency, batch sizes)
- Aggregation statistics

**Accessing Grafana:**
1. Navigate to http://localhost:3000
2. Login with `admin` / `admin`
3. The dashboard should be auto-provisioned

### Prometheus Alerts

Alert rules are defined in `prometheus/alert_rules.yml`:

- **High Error Rate**: Error rate exceeds threshold
- **High Consumer Lag**: Consumer falling behind
- **High Processing Latency**: Slow event processing
- **Database Connection Failure**: Database unavailable

**Viewing Alerts:**
- Prometheus UI: http://localhost:9090/alerts
- Grafana: Alert panel in dashboard

### Kafka UI

Kafka UI provides a web interface for Kafka management:

- **URL**: http://localhost:8080
- **Features**: Topic browsing, message inspection, consumer group monitoring

## Assumptions and Design Decisions

### Architecture Assumptions

1. **Event Ordering**: Events are processed in approximate order, but strict ordering is not required. The system uses Kafka partitions for parallelism.

2. **Data Freshness**: Hourly aggregations are flushed after the hour completes (1-hour delay). This ensures accuracy but means real-time queries may not include the current hour.

3. **Event Deduplication**: Events are deduplicated by `event_id` at the database level (unique constraint). The system assumes event IDs are globally unique.

4. **Scalability**: The system is designed to scale horizontally:
   - Multiple consumer instances can run (same consumer group)
   - Kafka partitions can be increased for parallelism
   - Database connection pooling supports concurrent operations

5. **Fault Tolerance**:
   - Kafka provides message durability and replay capability
   - Database operations use transactions and connection pooling
   - Failed events are tracked but not automatically retried (dead letter queue pattern)

### Design Decisions

1. **Batch Processing**: Events are processed in batches (default: 100) to improve throughput and reduce database load.

2. **Hourly Aggregations**: Events are aggregated by hour and event type. This balances query performance with data granularity.

3. **Connection Pooling**: PostgreSQL connections are pooled (2-10 connections) to handle concurrent operations efficiently.

4. **Metrics Export**: Prometheus metrics are exposed via HTTP endpoints, allowing external monitoring systems to scrape them.

5. **Configuration Management**: All configuration is environment-based, making the system easy to deploy across environments.

6. **Event Enrichment**: Events are enriched during transformation (e.g., flagging high-value purchases) to support downstream analytics.

7. **Graceful Shutdown**: Both producer and consumer handle SIGINT/SIGTERM signals to flush pending work before exiting.

### Limitations and Trade-offs

1. **Memory Usage**: Aggregations are held in memory until flushed. For very high event volumes, this could be a concern.

2. **Database Load**: Raw event storage can generate significant database load. This can be disabled via `SAVE_RAW_EVENTS=false`.

3. **No Real-time Queries**: Aggregations are not available until the hour completes. Real-time queries require accessing raw events.

4. **Single Topic**: The system processes a single Kafka topic. Multi-topic support would require additional configuration.

5. **No Schema Registry**: Event schemas are validated in code (Pydantic) rather than using a schema registry like Confluent Schema Registry.

 
 

## Utility Scripts

### Database Data Checker

The `check_db_data.py` script provides a quick way to verify data is being stored:

```bash
python check_db_data.py
```

 

 

 
