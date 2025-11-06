import json
import logging
from kafka import KafkaProducer
from src.config import Config

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    """Kafka producer with idempotent delivery."""
    
    def __init__(self, bootstrap_servers=None, topic=None):
        self.topic = topic or Config.KAFKA_TOPIC
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers or Config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            retry_backoff_ms=100,
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432,
            enable_idempotence=True,
            max_in_flight_requests_per_connection=1,  # required for idempotence
            request_timeout_ms=30000,
            delivery_timeout_ms=120000,
        )
        logger.info(f"Producer ready for topic {self.topic}")
    
    def publish(self, event, key=None):
        """Send event to Kafka. Uses user_id as partition key if key not provided."""
        partition_key = key or event.get('user_id', 'default')
        future = self.producer.send(self.topic, value=event, key=partition_key)
        future.add_callback(self._on_success)
        future.add_errback(self._on_error)
        return future
    
    def _on_success(self, record_metadata):
        logger.debug(f"Published to {record_metadata.topic}/{record_metadata.partition}@{record_metadata.offset}")
    
    def _on_error(self, exception):
        logger.error(f"Publish failed: {exception}")
    
    def flush(self, timeout=10.0):
        self.producer.flush(timeout=timeout)
    
    def close(self, timeout=10.0):
        self.flush(timeout)
        self.producer.close(timeout=timeout)
        logger.info("Producer closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
