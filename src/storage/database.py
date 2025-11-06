import logging
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch, Json
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DatabaseManager:
    """PostgreSQL connection pool manager with batch operations."""
    
    def __init__(self, host="localhost", port=5432, database="data_pipeline",
                 user="pipeline_user", password="pipeline_password",
                 min_connections=2, max_connections=10):
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                min_connections, max_connections,
                host=host, port=port, database=database, user=user, password=password
            )
            logger.info("Database pool created")
        except Exception as e:
            logger.error(f"Failed to create pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"DB operation failed: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def insert_aggregations_batch(self, aggregations):
        """Batch insert with upsert logic. Returns (inserted_count, error_type)."""
        if not aggregations:
            return 0, None
        
        query = """
            INSERT INTO hourly_aggregations 
            (aggregation_hour, event_type, total_events, unique_users, 
             total_duration_seconds, avg_duration_seconds, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (aggregation_hour, event_type)
            DO UPDATE SET
                total_events = EXCLUDED.total_events,
                unique_users = EXCLUDED.unique_users,
                total_duration_seconds = EXCLUDED.total_duration_seconds,
                avg_duration_seconds = EXCLUDED.avg_duration_seconds,
                updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    data = [
                        (agg['aggregation_hour'], agg['event_type'], agg['total_events'],
                         agg['unique_users'], agg.get('total_duration_seconds', 0.0),
                         agg.get('avg_duration_seconds', 0.0))
                        for agg in aggregations
                    ]
                    execute_batch(cursor, query, data, page_size=100)
                    conn.commit()
                    logger.info(f"Inserted {len(aggregations)} aggregations")
                    return len(aggregations), None
        except psycopg2.IntegrityError as e:
            logger.error(f"Batch insert integrity error: {e}", exc_info=True)
            return 0, 'integrity_error'
        except psycopg2.OperationalError as e:
            logger.error(f"Batch insert operational error: {e}", exc_info=True)
            return 0, 'connection_error'
        except Exception as e:
            logger.error(f"Batch insert failed: {e}", exc_info=True)
            return 0, 'unknown_error'
    
    def insert_raw_event(self, event):
        """Insert raw event. Returns (success, error_type)."""
        query = """
            INSERT INTO raw_events 
            (event_id, user_id, event_type, event_data, duration_seconds, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (
                        event['event_id'], event['user_id'], event['event_type'],
                        Json(event.get('event_data', {})), event.get('duration_seconds'),
                        event['timestamp']
                    ))
                    conn.commit()
                    return True, None
        except psycopg2.IntegrityError as e:
            logger.error(f"Raw event insert integrity error: {e}")
            return False, 'integrity_error'
        except psycopg2.OperationalError as e:
            logger.error(f"Raw event insert operational error: {e}")
            return False, 'connection_error'
        except Exception as e:
            logger.error(f"Raw event insert failed: {e}")
            return False, 'unknown_error'
    
    def insert_pipeline_metric(self, metric_name, metric_value, labels=None):
        """Insert pipeline metric. Returns (success, error_type)."""
        query = "INSERT INTO pipeline_metrics (metric_name, metric_value, labels) VALUES (%s, %s, %s)"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (metric_name, metric_value, Json(labels or {})))
                    conn.commit()
                    return True, None
        except psycopg2.IntegrityError as e:
            logger.error(f"Metric insert integrity error: {e}")
            return False, 'integrity_error'
        except psycopg2.OperationalError as e:
            logger.error(f"Metric insert operational error: {e}")
            return False, 'connection_error'
        except Exception as e:
            logger.error(f"Metric insert failed: {e}")
            return False, 'unknown_error'
    
    def health_check(self):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def get_table_counts(self):
        counts = {}
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM hourly_aggregations")
                    counts['hourly_aggregations'] = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM raw_events")
                    counts['raw_events'] = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM pipeline_metrics")
                    counts['pipeline_metrics'] = cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get table counts: {e}")
        return counts
    
    def close(self):
        if hasattr(self, 'connection_pool'):
            self.connection_pool.closeall()
            logger.info("Database pool closed")
