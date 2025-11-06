import json
import logging
import threading
import time
from kafka import KafkaConsumer
from src.config import Config

logger = logging.getLogger(__name__)


class KafkaEventConsumer:
    """Kafka consumer with batch processing support."""
    
    def __init__(self, bootstrap_servers=None, topic=None,
                 group_id=None, batch_size=100, auto_commit=True):
        self.topic = topic or Config.KAFKA_TOPIC
        self.batch_size = batch_size
        self.running = False
        self._timeout_thread = None
        
        servers = (bootstrap_servers or Config.KAFKA_BOOTSTRAP_SERVERS)
        servers = servers.split(",") if isinstance(servers, str) else servers
        self.consumer = KafkaConsumer(
            bootstrap_servers=servers,
            group_id=group_id or Config.KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=auto_commit,
            auto_commit_interval_ms=5000,
            fetch_min_bytes=1,
            fetch_max_wait_ms=500,
            max_poll_records=batch_size,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
        )
        
        self.consumer.subscribe([self.topic])
        self._wait_for_assignment()
        logger.info(f"Consumer ready for {self.topic} (group: {group_id or Config.KAFKA_CONSUMER_GROUP})")
    
    def _wait_for_assignment(self):
        self.consumer.poll(timeout_ms=500)
        while not self.consumer.assignment():
            self.consumer.poll(timeout_ms=500)
    
    def _start_timeout(self, seconds):
        if not seconds:
            return
        
        def timeout():
            time.sleep(seconds)
            if self.running:
                logger.info(f"Timeout reached ({seconds}s)")
                self.stop()
                try:
                    self.consumer.wakeup()
                except Exception:
                    pass
        
        self._timeout_thread = threading.Thread(target=timeout, daemon=True)
        self._timeout_thread.start()
    
    def consume(self, message_handler, timeout=None):
        """Consume messages one at a time."""
        self.running = True
        self._start_timeout(timeout)
        
        try:
            while self.running:
                packs = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                if not self.running or not packs:
                    continue
                
                for tp, records in packs.items():
                    for r in records:
                        message_handler(r.value)
                
                if not self.consumer.config['enable_auto_commit']:
                    self.consumer.commit()
        finally:
            self.close()
    
    def get_consumer_lag(self):
        """Compute consumer lag for all assigned partitions."""
        lag_info = {}
        try:
            assignment = self.consumer.assignment()
            if not assignment:
                return lag_info
            
            # Get end offsets (high watermarks) for all assigned partitions
            end_offsets = self.consumer.end_offsets(assignment)
            
            for tp in assignment:
                try:
                    # Get current consumer position (committed offset)
                    position = self.consumer.position(tp)
                    # Get high watermark (latest available offset)
                    end_offset = end_offsets.get(tp, 0)
                    
                    # Calculate lag: high watermark - current position
                    lag = max(0, end_offset - position) if end_offset is not None and position is not None else 0
                    lag_info[tp] = lag
                except Exception as e:
                    logger.debug(f"Error computing lag for {tp}: {e}")
                    lag_info[tp] = 0
        except Exception as e:
            logger.debug(f"Error getting consumer lag: {e}")
        
        return lag_info
    
    def consume_batch(self, batch_handler, timeout=None):
        """Consume messages in batches for better throughput."""
        self.running = True
        self._start_timeout(timeout)
        
        try:
            while self.running:
                packs = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                if not self.running or not packs:
                    continue
                
                batch = [r.value for _, recs in packs.items() for r in recs]
                if batch:
                    batch_handler(batch)
                    if not self.consumer.config['enable_auto_commit']:
                        self.consumer.commit()
        finally:
            self.close()
    
    def stop(self):
        self.running = False
        logger.info("Stopping consumer")
    
    def close(self):
        try:
            self.consumer.close()
            logger.info("Consumer closed")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
