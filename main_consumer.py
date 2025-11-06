#!/usr/bin/env python3

import json
import logging
import signal
import sys
import time
import threading
from src.config import Config
from src.consumer.kafka_consumer import KafkaEventConsumer
from src.consumer.transformer import EventTransformer
from src.consumer.aggregator import EventAggregator
from src.storage.database import DatabaseManager
from src.monitoring.metrics import PipelineMetrics
from src.monitoring.alerts import AlertManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

consumer = None
transformer = None
aggregator = None
db_manager = None
metrics = None
alert_manager = None
running = True


def signal_handler(sig, frame):
    global running
    logger.info("Shutting down...")
    running = False
    if consumer:
        consumer.stop()
    sys.exit(0)


def process_batch(batch):
    global transformer, aggregator, metrics, alert_manager, db_manager
    
    if not batch:
        return
    
    start_time = time.time()
    processed = 0
    failed = 0
    
    try:
        transformed = []
        for raw_event in batch:
            try:
                payload_bytes = len(json.dumps(raw_event).encode('utf-8'))
                topic = consumer.topic
                
                metrics.record_event_in(topic, payload_bytes)
                
                if Config.SAVE_RAW_EVENTS:
                    success, error_type = db_manager.insert_raw_event(raw_event)
                    if not success and error_type:
                        metrics.record_db_insert('raw_events', error_type=error_type)
                
                transform_start = time.time()
                event = transformer.transform(raw_event)
                metrics.record_transformation_time(time.time() - transform_start)
                
                if event:
                    transformed.append(event)
                    processed += 1
                else:
                    failed += 1
                    metrics.record_event_failed_with_topic(topic, 'transformation_error')
                    metrics.record_dead_letter('transformation_failed')
            except Exception as e:
                logger.error(f"Transform error: {e}")
                failed += 1
                metrics.record_event_failed_with_topic(topic, 'transformation_exception')
                metrics.record_dead_letter('transformation_exception')
        
        if transformed:
            agg_start = time.time()
            aggregator.add_events(transformed)
            metrics.record_aggregation_time(time.time() - agg_start)
            
            stats = aggregator.get_stats()
            logger.debug(f"Added {len(transformed)} events, active aggregations: {stats['active_aggregations']}")
            
            topic = consumer.topic
            for event in transformed:
                metrics.mark_processed(topic)
                metrics.record_aggregation_created(event.event_type)
                metrics.mark_event_processed_now(time.time())
        
        processing_time = time.time() - start_time
        metrics.record_processing_time(processing_time)
        
        if Config.SAVE_PIPELINE_METRICS:
            success, error_type = db_manager.insert_pipeline_metric(
                'processing_latency_seconds', processing_time,
                {'batch_size': len(batch), 'processed': processed, 'failed': failed}
            )
            if not success and error_type:
                metrics.record_db_insert('pipeline_metrics', error_type=error_type)
        
        if processing_time > Config.ALERT_PROCESSING_LATENCY_THRESHOLD:
            alert_manager.check_processing_latency(processing_time)
        
        error_rate = failed / len(batch) if batch else 0
        alert_manager.check_error_rate(failed, len(batch))
        
    except Exception as e:
        logger.error(f"Batch processing error: {e}", exc_info=True)
        topic = consumer.topic
        metrics.record_event_failed_with_topic(topic, 'batch_processing_error')
        metrics.record_dead_letter('batch_processing_error')
        alert_manager.check_pipeline_failure(e)


def flush_aggregations(force_all=False):
    global aggregator, db_manager, metrics, alert_manager
    
    try:
        stats_before = aggregator.get_stats()
        completed = aggregator.flush(force_all=force_all)
        
        if not completed:
            logger.debug(f"No aggregations to flush (active: {stats_before['active_aggregations']})")
            return
        
        flush_type = "all" if force_all else "completed"
        logger.info(f"Flushing {len(completed)} aggregations ({flush_type})")
        
        insert_start = time.time()
        inserted, error_type = db_manager.insert_aggregations_batch(completed)
        insert_duration = time.time() - insert_start
        
        batch_size = len(completed) if completed else None
        metrics.record_db_insert(
            'hourly_aggregations',
            insert_duration,
            batch_size=batch_size,
            error_type=error_type
        )
        
        if inserted > 0:
            metrics.record_aggregation_flushed(inserted)
            logger.info(f"Flushed {inserted} aggregations to database")
        else:
            logger.warning("Aggregation insert failed")
            alert_manager.check_db_connection_failure()
    except Exception as e:
        logger.error(f"Flush error: {e}", exc_info=True)
        alert_manager.check_db_connection_failure()


def main():
    global consumer, transformer, aggregator, db_manager, metrics, alert_manager, running
    
    logger.info("Starting consumer...")
    
    metrics = PipelineMetrics(metrics_port=Config.METRICS_PORT_CONSUMER)
    alert_manager = AlertManager()
    
    db_manager = DatabaseManager(
        host=Config.DB_HOST, port=Config.DB_PORT, database=Config.DB_NAME,
        user=Config.DB_USER, password=Config.DB_PASSWORD,
        min_connections=Config.DB_MIN_CONNECTIONS, max_connections=Config.DB_MAX_CONNECTIONS
    )
    
    if not db_manager.health_check():
        logger.error("Database health check failed")
        alert_manager.check_db_connection_failure()
        sys.exit(1)
    
    transformer = EventTransformer()
    aggregator = EventAggregator(flush_interval_minutes=Config.AGGREGATOR_FLUSH_INTERVAL_MINUTES)
    
    logger.info(f"Consumer batch size: {Config.CONSUMER_BATCH_SIZE}, flush interval: {Config.FLUSH_INTERVAL_SECONDS}s")
    
    consumer = KafkaEventConsumer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        topic=Config.KAFKA_TOPIC,
        group_id=Config.KAFKA_CONSUMER_GROUP,
        batch_size=Config.CONSUMER_BATCH_SIZE
    )
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        def consume_loop():
            consumer.consume_batch(process_batch)
        
        consume_thread = threading.Thread(target=consume_loop, daemon=True)
        consume_thread.start()
        
        last_flush = time.time()
        last_full_flush = time.time()
        last_lag_check = time.time()
        flush_interval = Config.FLUSH_INTERVAL_SECONDS
        full_flush_interval = 300  # 5 minutes
        lag_check_interval = 10  # Check lag every 10 seconds
        
        while running:
            time.sleep(1)
            now = time.time()
            
            # Check and report consumer lag periodically
            if now - last_lag_check >= lag_check_interval:
                try:
                    lag_info = consumer.get_consumer_lag()
                    for tp, lag in lag_info.items():
                        metrics.set_consumer_lag(tp.topic, tp.partition, lag)
                except Exception as e:
                    logger.debug(f"Error reporting consumer lag: {e}")
                last_lag_check = now
            
            # Flush completed hours every interval
            if now - last_flush >= flush_interval:
                flush_aggregations(force_all=False)
                last_flush = now
            
            # Full flush (including current hour) every 5 minutes
            if now - last_full_flush >= full_flush_interval:
                flush_aggregations(force_all=True)
                last_full_flush = now
        
        consume_thread.join(timeout=10)
        
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
        alert_manager.check_pipeline_failure(e)
        raise
    finally:
        logger.info("Final flush...")
        flush_aggregations(force_all=True)
        
        if consumer:
            consumer.close()
        if db_manager:
            db_manager.close()
        logger.info("Consumer stopped")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
