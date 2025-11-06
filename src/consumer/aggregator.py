import logging
from datetime import datetime, timedelta
from src.consumer.transformer import TransformedEvent

logger = logging.getLogger(__name__)


class HourlyAggregation:
    """Tracks events for a specific hour and event type."""
    
    def __init__(self, aggregation_hour, event_type):
        self.aggregation_hour = aggregation_hour
        self.event_type = event_type
        self.total_events = 0
        self.unique_users = set()
        self.total_duration = 0.0
        self.duration_count = 0
    
    def add_event(self, event):
        self.total_events += 1
        self.unique_users.add(event.user_id)
        
        if event.duration_seconds is not None:
            self.total_duration += event.duration_seconds
            self.duration_count += 1
    
    def to_dict(self):
        avg_duration = (self.total_duration / self.duration_count
                       if self.duration_count > 0 else 0.0)
        
        return {
            'aggregation_hour': self.aggregation_hour,
            'event_type': self.event_type,
            'total_events': self.total_events,
            'unique_users': len(self.unique_users),
            'total_duration_seconds': round(self.total_duration, 2),
            'avg_duration_seconds': round(avg_duration, 2)
        }


class EventAggregator:
    """Aggregates events into hourly buckets."""
    
    def __init__(self, flush_interval_minutes=5):
        self.flush_interval_minutes = flush_interval_minutes
        self.aggregations = {}
        self.last_flush_time = datetime.utcnow()
    
    def add_event(self, event):
        event_ts = event.timestamp
        if event_ts.tzinfo is None:
            event_ts = event_ts.replace(tzinfo=None)
        
        hour = event_ts.replace(minute=0, second=0, microsecond=0)
        key = (hour, event.event_type)
        
        if key not in self.aggregations:
            self.aggregations[key] = HourlyAggregation(hour, event.event_type)
        
        self.aggregations[key].add_event(event)
    
    def add_events(self, events):
        for event in events:
            self.add_event(event)
    
    def get_completed_aggregations(self):
        """Returns aggregations for hours that are fully complete (older than 1 hour)."""
        now = datetime.utcnow()
        cutoff = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        
        completed = []
        keys_to_remove = []
        
        for key, agg in self.aggregations.items():
            hour = key[0]
            if hour.tzinfo is not None:
                hour = hour.replace(tzinfo=None)
            
            if hour < cutoff:
                completed.append(agg.to_dict())
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.aggregations[key]
        
        return completed
    
    def get_all_aggregations(self):
        return [agg.to_dict() for agg in self.aggregations.values()]
    
    def flush(self, force_all=False):
        """Flush completed aggregations, or all if force_all=True."""
        if force_all:
            completed = self.get_all_aggregations()
            self.aggregations.clear()
        else:
            completed = self.get_completed_aggregations()
        
        self.last_flush_time = datetime.utcnow()
        
        if completed:
            logger.info(f"Flushing {len(completed)} aggregations")
        
        return completed
    
    def get_stats(self):
        return {
            'active_aggregations': len(self.aggregations),
            'last_flush_time': self.last_flush_time.isoformat(),
            'aggregation_keys': list(self.aggregations.keys())
        }
