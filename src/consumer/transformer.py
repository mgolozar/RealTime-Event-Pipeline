import logging
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, validator

logger = logging.getLogger(__name__)


class TransformedEvent(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    timestamp: datetime
    event_data: dict
    duration_seconds: Optional[float] = None
    
    @validator('timestamp', pre=True)
    def parse_timestamp(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v
    
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class EventTransformer:
    """Validates and enriches raw events."""
    
    def __init__(self):
        self.processed_count = 0
        self.error_count = 0
    
    def transform(self, raw_event):
        try:
            transformed = TransformedEvent(**raw_event)
            self.processed_count += 1
            return self._enrich(transformed)
        except Exception as e:
            self.error_count += 1
            logger.error(f"Transform failed: {e}")
            return None
    
    def _enrich(self, event):
        event_dict = event.dict()
        event_dict['processed_at'] = datetime.utcnow().isoformat()
        
        # Flag high-value purchases
        if event.event_type == 'purchase' and event.event_data.get('amount'):
            event_dict['event_data']['is_high_value'] = event.event_data['amount'] > 100.0
        
        return TransformedEvent(**event_dict)
    
    def get_stats(self):
        total = self.processed_count + self.error_count
        return {
            'processed': self.processed_count,
            'errors': self.error_count,
            'success_rate': (self.processed_count / total * 100) if total > 0 else 0
        }
