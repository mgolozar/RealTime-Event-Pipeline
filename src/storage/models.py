from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class HourlyAggregationModel(BaseModel):
    aggregation_hour: datetime
    event_type: str = Field(..., max_length=50)
    total_events: int = Field(..., ge=0)
    unique_users: int = Field(..., ge=0)
    total_duration_seconds: float = Field(default=0.0, ge=0)
    avg_duration_seconds: float = Field(default=0.0, ge=0)
    
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class RawEventModel(BaseModel):
    event_id: str = Field(..., max_length=100)
    user_id: str = Field(..., max_length=100)
    event_type: str = Field(..., max_length=50)
    event_data: dict
    duration_seconds: Optional[float] = None
    timestamp: datetime
    
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
