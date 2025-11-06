#!/usr/bin/env python3

import asyncio
import json
import logging
import signal
import sys
from src.config import Config
from src.producer.simulator import EventSimulator
from src.producer.kafka_producer import KafkaEventProducer
from src.monitoring.metrics import PipelineMetrics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

simulator = None
producer = None
metrics = None


async def publish_event(event):
    global producer, metrics
    
    try:
        payload_bytes = len(json.dumps(event).encode('utf-8'))
        topic = producer.topic
        
        metrics.record_event_out(topic, payload_bytes)
        producer.publish(event)
    except Exception as e:
        logger.error(f"Publish failed: {e}")
        metrics.record_kafka_produce_error()
        metrics.record_event_failed_with_topic(topic, 'publish_error')


def signal_handler(sig, frame):
    logger.info("Shutting down...")
    if simulator:
        simulator.stop()
    if producer:
        producer.close()
    sys.exit(0)


async def main():
    global simulator, producer, metrics
    
    logger.info("Starting producer...")
    
    metrics = PipelineMetrics(metrics_port=Config.METRICS_PORT_PRODUCER)
    producer = KafkaEventProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        topic=Config.KAFKA_TOPIC
    )
    
    simulator = EventSimulator(
        min_interval=Config.PRODUCER_MIN_INTERVAL,
        max_interval=Config.PRODUCER_MAX_INTERVAL,
        user_count=Config.PRODUCER_USER_COUNT
    )
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await simulator.generate_events(publish_event)
    except KeyboardInterrupt:
        pass
    finally:
        if simulator:
            simulator.stop()
        if producer:
            producer.close()
        logger.info("Producer stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
