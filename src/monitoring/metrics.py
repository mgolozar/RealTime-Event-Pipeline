import json
import logging
import os
from prometheus_client import Counter, Histogram, Gauge, start_http_server

logger = logging.getLogger(__name__)


class PipelineMetrics:
    """Prometheus metrics for pipeline observability."""
    
    def __init__(self, metrics_port=8000):
        self.events_produced = Counter('events_produced_total', 'Events produced', ['event_type'])
        self.events_consumed = Counter('events_consumed_total', 'Events consumed', ['event_type'])
        self.events_processed = Counter('events_processed_total', 'Events processed', ['event_type'])
        self.events_failed = Counter('events_failed_total', 'Events failed', ['error_type', 'topic'])
        
        self.processing_latency = Histogram(
            'event_processing_duration_seconds', 'Processing time',
            buckets=[0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0]
        )
        self.transformation_latency = Histogram(
            'event_transformation_duration_seconds', 'Transformation time',
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1]
        )
        self.aggregation_latency = Histogram(
            'aggregation_duration_seconds', 'Aggregation time',
            buckets=[0.01, 0.1, 0.5, 1.0, 5.0, 10.0]
        )
        
        self.db_inserts = Counter('db_inserts_total', 'DB inserts', ['table'])
        self.db_insert_duration = Histogram(
            'db_insert_duration_seconds', 'DB insert time',
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        self.db_insert_errors = Counter('db_insert_errors_total', 'DB insert errors', ['table', 'error_type'])
        self.db_batch_size = Histogram(
            'db_batch_size', 'DB batch insert size',
            buckets=[1, 10, 50, 100, 500, 1000]
        )
        self.db_connections_active = Gauge('db_connections_active', 'Active DB connections')
        self.last_event_processed_unixtime = Gauge('last_event_processed_unixtime', 'Unix timestamp of last successfully processed event')
        
        self.kafka_produce_errors = Counter('kafka_produce_errors_total', 'Kafka produce errors')
        self.kafka_consume_errors = Counter('kafka_consume_errors_total', 'Kafka consume errors')
        self.kafka_consumer_lag = Gauge('kafka_consumer_lag', 'Kafka consumer lag', ['topic', 'partition'])
        
        self.aggregations_created = Counter('aggregations_created_total', 'Aggregations created', ['event_type'])
        self.aggregations_flushed = Counter('aggregations_flushed_total', 'Aggregations flushed')
        self.dead_letter_events = Counter('dead_letter_events_total', 'Events sent to dead letter queue', ['reason'])
        
        self.bytes_total = Counter('bytes_total', 'Bytes transferred', ['direction', 'source', 'topic'])
        self.event_size_bytes = Histogram(
            'event_size_bytes', 'Event size in bytes',
            buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000]
        )
        if metrics_port is None:
            metrics_port = int(os.getenv("METRICS_PORT", "8000"))
        try:
            
            start_http_server(metrics_port, addr="0.0.0.0")
            logger.info(f"Metrics server on port {metrics_port}")
        except Exception as e:
            logger.warning(f"Metrics server failed: {e}")
    
    def record_event_produced(self, event_type):
        self.events_produced.labels(event_type=event_type).inc()
    
    def record_event_consumed(self, event_type):
        self.events_consumed.labels(event_type=event_type).inc()
    
    def record_event_processed(self, event_type):
        self.events_processed.labels(event_type=event_type).inc()
    
    def mark_processed(self, topic: str):
        """Mark event as processed using topic as event_type label."""
        self.events_processed.labels(event_type=topic).inc()
    
    def mark_event_processed_now(self, now_unixtime: float):
        """Mark the timestamp when an event was successfully processed end-to-end."""
        self.last_event_processed_unixtime.set(now_unixtime)
    
    def record_event_failed(self, error_type):
        """Record event failure without topic (backward compatibility)."""
        self.events_failed.labels(error_type=error_type, topic='unknown').inc()
    
    def record_event_failed_with_topic(self, topic: str, error_type: str):
        """Record event failure with topic correlation."""
        self.events_failed.labels(error_type=error_type, topic=topic).inc()
    
    def record_processing_time(self, duration):
        self.processing_latency.observe(duration)
    
    def record_transformation_time(self, duration):
        self.transformation_latency.observe(duration)
    
    def record_aggregation_time(self, duration):
        self.aggregation_latency.observe(duration)
    
    def record_db_insert(self, table, duration=None, batch_size=None, error_type=None):
        """Record database insert with optional duration, batch size, and error tracking."""
        self.db_inserts.labels(table=table).inc()
        if duration is not None:
            self.db_insert_duration.observe(duration)
        if batch_size is not None:
            self.db_batch_size.observe(batch_size)
        if error_type is not None:
            self.db_insert_errors.labels(table=table, error_type=error_type).inc()
    
    def set_db_connections(self, count):
        self.db_connections_active.set(count)
    
    def record_kafka_produce_error(self):
        self.kafka_produce_errors.inc()
    
    def record_kafka_consume_error(self):
        self.kafka_consume_errors.inc()
    
    def set_consumer_lag(self, topic: str, partition: int, lag: int):
        """Set consumer lag gauge for a specific topic and partition."""
        self.kafka_consumer_lag.labels(topic=topic, partition=str(partition)).set(lag)
    
    def record_aggregation_created(self, event_type):
        self.aggregations_created.labels(event_type=event_type).inc()
    
    def record_aggregation_flushed(self, count):
        self.aggregations_flushed.inc(count)
    
    def record_dead_letter(self, reason: str):
        """Record event sent to dead letter queue."""
        self.dead_letter_events.labels(reason=reason).inc()
    
    def record_event_in(self, topic: str, payload_bytes: int, event_type: str = None):
        """Record event consumed with byte tracking. Uses topic as event_type if not provided."""
        event_type_label = topic if event_type is None else event_type
        self.events_consumed.labels(event_type=event_type_label).inc()
        self.bytes_total.labels(direction='in', source='kafka', topic=topic).inc(payload_bytes)
        self.event_size_bytes.observe(payload_bytes)
    
    def record_event_out(self, topic: str, payload_bytes: int, event_type: str = None, dest='kafka'):
        """Record event produced with byte tracking. Uses topic as event_type if not provided."""
        event_type_label = topic if event_type is None else event_type
        self.events_produced.labels(event_type=event_type_label).inc()
        self.bytes_total.labels(direction='out', source=dest, topic=topic).inc(payload_bytes)
