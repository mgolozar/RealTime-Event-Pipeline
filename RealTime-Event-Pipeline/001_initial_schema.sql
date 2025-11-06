 

CREATE TABLE IF NOT EXISTS hourly_aggregations (
    id BIGSERIAL PRIMARY KEY,
    aggregation_hour TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    total_events INTEGER NOT NULL DEFAULT 0,
    unique_users INTEGER NOT NULL DEFAULT 0,
    total_duration_seconds NUMERIC(10, 2) DEFAULT 0,
    avg_duration_seconds NUMERIC(10, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(aggregation_hour, event_type)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_aggregation_hour ON hourly_aggregations(aggregation_hour DESC);
CREATE INDEX IF NOT EXISTS idx_event_type ON hourly_aggregations(event_type);
CREATE INDEX IF NOT EXISTS idx_aggregation_hour_type ON hourly_aggregations(aggregation_hour, event_type);

 
CREATE TABLE IF NOT EXISTS raw_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) UNIQUE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    duration_seconds NUMERIC(10, 2),
    timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp ON raw_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id ON raw_events(user_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON raw_events(event_type);

 
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(15, 2) NOT NULL,
    metric_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    labels JSONB
);

CREATE INDEX IF NOT EXISTS idx_metric_timestamp ON pipeline_metrics(metric_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_metric_name ON pipeline_metrics(metric_name);

