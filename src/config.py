import os
from dotenv import load_dotenv

load_dotenv()


def _get_env(key, default=None):
    val = os.getenv(key, default)
    if val is None:
        raise ValueError(f"Missing required env var: {key}")
    return val


def _get_env_int(key, default=None):
    val = os.getenv(key)
    return int(val) if val else default


def _get_env_float(key, default=None):
    val = os.getenv(key)
    return float(val) if val else default


class Config:
    """Application configuration from environment variables."""
    
    KAFKA_BOOTSTRAP_SERVERS = _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9091,localhost:9092,localhost:9093")
    KAFKA_TOPIC = _get_env("KAFKA_TOPIC", "user-events")
    KAFKA_CONSUMER_GROUP = _get_env("KAFKA_CONSUMER_GROUP", "data-pipeline-consumer")
    
    DB_HOST = _get_env("DB_HOST", "localhost")
    DB_PORT = _get_env_int("DB_PORT", 5432)
    DB_NAME = _get_env("DB_NAME", "data_pipeline")
    DB_USER = _get_env("DB_USER", "pipeline_user")
    DB_PASSWORD = _get_env("DB_PASSWORD", "pipeline_password")
    DB_MIN_CONNECTIONS = _get_env_int("DB_MIN_CONNECTIONS", 2)
    DB_MAX_CONNECTIONS = _get_env_int("DB_MAX_CONNECTIONS", 10)
    
    METRICS_PORT_PRODUCER = _get_env_int("METRICS_PORT_PRODUCER", 8001)
    METRICS_PORT_CONSUMER = _get_env_int("METRICS_PORT_CONSUMER", 8002)
    PROMETHEUS_URL = _get_env("PROMETHEUS_URL", "http://localhost:9090")
    
    PRODUCER_MIN_INTERVAL = _get_env_float("PRODUCER_MIN_INTERVAL", 0.1)
    PRODUCER_MAX_INTERVAL = _get_env_float("PRODUCER_MAX_INTERVAL", 5.0)
    PRODUCER_USER_COUNT = _get_env_int("PRODUCER_USER_COUNT", 1000)
    
    CONSUMER_BATCH_SIZE = _get_env_int("CONSUMER_BATCH_SIZE", 100)
    AGGREGATOR_FLUSH_INTERVAL_MINUTES = _get_env_int("AGGREGATOR_FLUSH_INTERVAL_MINUTES", 5)
    FLUSH_INTERVAL_SECONDS = _get_env_int("FLUSH_INTERVAL_SECONDS", 60)
    
    SAVE_RAW_EVENTS = os.getenv("SAVE_RAW_EVENTS", "true").lower() == "true"
    SAVE_PIPELINE_METRICS = os.getenv("SAVE_PIPELINE_METRICS", "true").lower() == "true"
    
    ALERT_ERROR_RATE_THRESHOLD = _get_env_float("ALERT_ERROR_RATE_THRESHOLD", 0.05)
    ALERT_CONSUMER_LAG_THRESHOLD = _get_env_int("ALERT_CONSUMER_LAG_THRESHOLD", 1000)
    ALERT_PROCESSING_LATENCY_THRESHOLD = _get_env_float("ALERT_PROCESSING_LATENCY_THRESHOLD", 5.0)
