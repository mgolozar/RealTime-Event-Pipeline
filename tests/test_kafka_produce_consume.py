"""Simple test for Kafka produce and consume functionality."""

import time
import pytest
from src.producer.kafka_producer import KafkaEventProducer
from src.consumer.kafka_consumer import KafkaEventConsumer
from src.config import Config


@pytest.fixture
def test_topic():
    """Use a test topic to avoid interfering with production data."""
    return "test-user-events"


@pytest.fixture
def test_group_id():
    """Use a unique test consumer group."""
    return f"test-consumer-{int(time.time() * 1000)}"


@pytest.fixture
def bootstrap_servers():
    """Kafka bootstrap servers from config."""
    return Config.KAFKA_BOOTSTRAP_SERVERS


def test_produce_and_consume_single_message(bootstrap_servers, test_topic, test_group_id):
    """Test producing a single message and consuming it."""
    test_event = {
        "event_id": "test_event_123",
        "user_id": "test_user_456",
        "event_type": "page_view",
        "timestamp": "2024-01-01T12:00:00Z",
        "event_data": {
            "page_url": "/test",
            "session_id": "test_session"
        },
        "duration_seconds": 1.5
    }
    
    received_messages = []
    
    # Start consumer first
    consumer = KafkaEventConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=test_topic,
        group_id=test_group_id,
        batch_size=10,
        auto_commit=False
    )
    
    def message_handler(message):
        received_messages.append(message)
        consumer.stop()
    
    # Start consumer in a thread
    import threading
    consume_thread = threading.Thread(
        target=lambda: consumer.consume(message_handler, timeout=15),
        daemon=True
    )
    consume_thread.start()
    
    # Give consumer time to subscribe and get partition assignment
    time.sleep(3)
    
    # Produce message
    producer = KafkaEventProducer(
        bootstrap_servers=bootstrap_servers,
        topic=test_topic
    )
    
    try:
        future = producer.publish(test_event)
        future.get(timeout=10)  # Wait for delivery confirmation
        producer.flush()
        
        # Wait for consumer to receive message (with timeout)
        consume_thread.join(timeout=15)
        
        # Verify message was received
        assert len(received_messages) > 0, "No messages received"
        received = received_messages[0]
        
        assert received["event_id"] == test_event["event_id"]
        assert received["user_id"] == test_event["user_id"]
        assert received["event_type"] == test_event["event_type"]
        assert received["event_data"]["page_url"] == test_event["event_data"]["page_url"]
        
    finally:
        producer.close()
        if consumer.running:
            consumer.stop()
        consumer.close()


def test_produce_and_consume_batch(bootstrap_servers, test_topic, test_group_id):
    """Test producing multiple messages and consuming them in a batch."""
    test_events = [
        {
            "event_id": f"test_event_{i}",
            "user_id": f"test_user_{i}",
            "event_type": "click",
            "timestamp": "2024-01-01T12:00:00Z",
            "event_data": {"element_id": f"btn_{i}"},
            "duration_seconds": None
        }
        for i in range(5)
    ]
    
    received_batches = []
    expected_count = len(test_events)
    
    # Start consumer
    consumer = KafkaEventConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=test_topic,
        group_id=test_group_id,
        batch_size=10,
        auto_commit=False
    )
    
    def batch_handler(batch):
        received_batches.append(batch)
        # Count total messages received so far
        total_received = sum(len(b) for b in received_batches)
        # Stop only when we've received all expected messages
        if total_received >= expected_count:
            consumer.stop()
    
    import threading
    consume_thread = threading.Thread(
        target=lambda: consumer.consume_batch(batch_handler, timeout=20),
        daemon=True
    )
    consume_thread.start()
    
    # Give consumer time to subscribe and get partition assignment
    time.sleep(3)
    
    # Produce messages
    producer = KafkaEventProducer(
        bootstrap_servers=bootstrap_servers,
        topic=test_topic
    )
    
    try:
        for event in test_events:
            future = producer.publish(event)
            future.get(timeout=10)
        
        producer.flush()
        
        # Wait for consumer to receive all messages
        consume_thread.join(timeout=20)
        
        # Verify messages were received
        assert len(received_batches) > 0, "No batches received"
        
        all_received = []
        for batch in received_batches:
            all_received.extend(batch)
        
        assert len(all_received) >= len(test_events), f"Expected {len(test_events)} messages, got {len(all_received)}"
        
        # Verify event IDs match
        received_ids = {msg["event_id"] for msg in all_received}
        expected_ids = {event["event_id"] for event in test_events}
        assert received_ids.issuperset(expected_ids), "Not all events were received"
        
    finally:
        producer.close()
        if consumer.running:
            consumer.stop()
        consumer.close()

