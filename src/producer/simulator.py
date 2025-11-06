import asyncio
import random
import time
from datetime import datetime, timezone
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    CLICK = "click"
    PURCHASE = "purchase"
    LOGIN = "login"
    LOGOUT = "logout"
    SEARCH = "search"


class EventSimulator:
    """Generates realistic user events with irregular timing."""
    
    def __init__(self, min_interval=0.1, max_interval=5.0, user_count=1000):
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.user_count = user_count
        self.running = False
        
        # Realistic event distribution
        self.event_weights = {
            EventType.PAGE_VIEW: 0.4,
            EventType.CLICK: 0.3,
            EventType.SEARCH: 0.15,
            EventType.LOGIN: 0.05,
            EventType.LOGOUT: 0.05,
            EventType.PURCHASE: 0.05,
        }
    
    def _generate_event(self):
        event_type = random.choices(
            list(EventType),
            weights=[self.event_weights.get(et, 0.1) for et in EventType]
        )[0]
        
        user_id = f"user_{random.randint(1, self.user_count)}"
        event_data = self._build_event_data(event_type)
        
        return {
            "event_id": f"{event_type}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            "user_id": user_id,
            "event_type": event_type.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_data": event_data,
            "duration_seconds": round(random.uniform(0.1, 30.0), 2) if event_type in [EventType.PAGE_VIEW, EventType.CLICK] else None
        }
    
    def _build_event_data(self, event_type):
        base = {
            "session_id": f"session_{random.randint(10000, 99999)}",
            "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "user_agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Mozilla/5.0 (X11; Linux x86_64)"
            ])
        }
        
        if event_type == EventType.PAGE_VIEW:
            base.update({
                "page_url": f"/page/{random.choice(['home', 'products', 'about', 'contact'])}",
                "referrer": random.choice(["direct", "google.com", "facebook.com", "twitter.com"])
            })
        elif event_type == EventType.CLICK:
            base.update({
                "element_id": f"btn_{random.choice(['submit', 'cancel', 'next', 'back'])}",
                "element_type": "button"
            })
        elif event_type == EventType.PURCHASE:
            base.update({
                "order_id": f"order_{random.randint(100000, 999999)}",
                "amount": round(random.uniform(10.0, 500.0), 2),
                "currency": "USD",
                "items_count": random.randint(1, 10)
            })
        elif event_type == EventType.SEARCH:
            base.update({
                "query": random.choice(["laptop", "phone", "headphones", "keyboard", "mouse"]),
                "results_count": random.randint(0, 100)
            })
        elif event_type in [EventType.LOGIN, EventType.LOGOUT]:
            base.update({"auth_method": random.choice(["email", "oauth", "sso"])})
        
        return base
    
    def _get_irregular_interval(self):
        """Exponential distribution for more realistic event spacing."""
        mean = (self.min_interval + self.max_interval) / 2
        interval = random.expovariate(1.0 / mean)
        return max(self.min_interval, min(self.max_interval, interval))
    
    async def generate_events(self, callback):
        self.running = True
        count = 0
        logger.info("Event generation started")
        
        while self.running:
            try:
                event = self._generate_event()
                await callback(event)
                count += 1
                
                if count % 100 == 0:
                    logger.info(f"Generated {count} events")
                
                await asyncio.sleep(self._get_irregular_interval())
            except Exception as e:
                logger.error(f"Event generation error: {e}")
                await asyncio.sleep(1)
    
    def stop(self):
        self.running = False
        logger.info("Event generation stopped")
