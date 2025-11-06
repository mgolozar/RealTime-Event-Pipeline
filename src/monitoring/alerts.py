import logging
from datetime import datetime
from prometheus_client import Counter

logger = logging.getLogger(__name__)


class AlertManager:
    """Threshold-based alerting with Prometheus metrics."""
    
    def __init__(self):
        self.alert_callbacks = []
        self.alerts_fired = Counter('alerts_fired_total', 'Alerts fired', ['alert_type', 'severity'])
        
        self.thresholds = {
            'error_rate': 0.05,
            'consumer_lag': 1000,
            'processing_latency': 5.0,
            'db_connection_failures': 3
        }
        
        self.alert_state = {}
    
    def register_callback(self, callback):
        self.alert_callbacks.append(callback)
    
    def _fire_alert(self, alert_type, severity, message, context=None):
        context = context or {}
        context['timestamp'] = datetime.utcnow().isoformat()
        context['alert_message'] = message
        
        alert_key = f"{alert_type}_{severity}"
        self.alert_state[alert_key] = True
        
        self.alerts_fired.labels(alert_type=alert_type, severity=severity).inc()
        
        log_level = logging.CRITICAL if severity == 'critical' else logging.WARNING
        logger.log(log_level, f"ALERT [{severity}] {alert_type}: {message}", extra=context)
        
        for callback in self.alert_callbacks:
            try:
                callback(alert_type, severity, context)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")
    
    def check_error_rate(self, error_count, total_count):
        if total_count == 0:
            return
        
        error_rate = error_count / total_count
        if error_rate > self.thresholds['error_rate']:
            self._fire_alert(
                'high_error_rate', 'critical',
                f"Error rate {error_rate:.2%} exceeds {self.thresholds['error_rate']:.2%}",
                {'error_rate': error_rate, 'error_count': error_count, 'total_count': total_count}
            )
    
    def check_consumer_lag(self, lag):
        if lag > self.thresholds['consumer_lag']:
            self._fire_alert(
                'high_consumer_lag', 'warning',
                f"Consumer lag {lag} exceeds {self.thresholds['consumer_lag']}",
                {'lag': lag}
            )
    
    def check_processing_latency(self, latency):
        if latency > self.thresholds['processing_latency']:
            self._fire_alert(
                'high_processing_latency', 'warning',
                f"Processing latency {latency:.2f}s exceeds {self.thresholds['processing_latency']}s",
                {'latency': latency}
            )
    
    def check_db_connection_failure(self):
        self._fire_alert('db_connection_failure', 'critical', "Database connection failed", {})
    
    def check_pipeline_failure(self, error):
        self._fire_alert(
            'pipeline_failure', 'critical',
            f"Pipeline failure: {str(error)}",
            {'error_type': type(error).__name__}
        )
    
    def clear_alert(self, alert_type, severity):
        alert_key = f"{alert_type}_{severity}"
        self.alert_state[alert_key] = False
