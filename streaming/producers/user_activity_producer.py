 # streaming/producers/user_activity_producer.py

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UserActivityProducer:
    def __init__(self, bootstrap_servers: str = 'kafka:29092'):
        """Initialize Kafka producer for user activity events"""
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'user_activity_events'
        
        # Try to connect to Kafka with retries
        max_attempts = 10
        retry_interval_sec = 5
        
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Attempting to connect to Kafka (Attempt {attempt}/{max_attempts})...")
                
                # Producer configuration
                self.producer = KafkaProducer(
                    bootstrap_servers=[bootstrap_servers],
                    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                    key_serializer=lambda x: x.encode('utf-8') if x else None,
                    acks='all',
                    retries=5,
                    retry_backoff_ms=1000,
                    request_timeout_ms=30000,
                    batch_size=16384,
                    linger_ms=10,
                    compression_type='gzip'
                )
                
                # Verify connection by sending a test message
                self.producer.send('__test_topic', value={"test": "connection"})
                self.producer.flush(timeout=10)
                
                logger.info(f"Successfully connected to Kafka at {bootstrap_servers}")
                break
                
            except KafkaError as e:
                if attempt == max_attempts:
                    logger.error(f"Failed to connect to Kafka after {max_attempts} attempts: {e}")
                    raise
                else:
                    logger.warning(f"Failed to connect to Kafka (attempt {attempt}): {e}")
                    logger.info(f"Retrying in {retry_interval_sec} seconds...")
                    time.sleep(retry_interval_sec)
        
        # Sample data for generating realistic events
        self.customers = [f"CUST-{i:06d}" for i in range(1, 10001)]  # 10k customers
        self.products = [f"PROD-{i:06d}" for i in range(1, 1001)]   # 1k products
        self.pages = [
            '/home', '/catalog', '/search', '/cart', '/checkout', 
            '/profile', '/orders', '/wishlist', '/recommendations',
            '/product-detail', '/category', '/brand', '/deals'
        ]
        self.devices = ['desktop', 'mobile', 'tablet']
        self.event_types = [
            'page_view', 'product_view', 'add_to_cart', 'remove_from_cart',
            'add_to_wishlist', 'search', 'filter_apply', 'sort_apply',
            'checkout_start', 'purchase_complete', 'user_login', 'user_logout'
        ]
        self.referrers = [
            'google.com', 'facebook.com', 'instagram.com', 'direct',
            'email_campaign', 'affiliate', 'retargeting_ad', 'organic'
        ]
        
        # Event weights for realistic distribution
        self.event_weights = {
            'page_view': 0.35,
            'product_view': 0.25,
            'search': 0.15,
            'add_to_cart': 0.08,
            'filter_apply': 0.05,
            'sort_apply': 0.03,
            'add_to_wishlist': 0.03,
            'remove_from_cart': 0.02,
            'checkout_start': 0.02,
            'purchase_complete': 0.01,
            'user_login': 0.005,
            'user_logout': 0.005
        }

    def generate_session_id(self) -> str:
        """Generate a session ID"""
        return f"sess_{uuid.uuid4().hex[:12]}"

    def generate_user_event(self, session_context: Dict = None) -> Dict:
        """Generate a realistic user activity event"""
        
        # Use session context if provided, otherwise create new session
        if session_context:
            customer_id = session_context['customer_id']
            session_id = session_context['session_id']
            device_type = session_context['device_type']
            last_event_time = session_context.get('last_event_time', datetime.utcnow())
        else:
            customer_id = random.choice(self.customers)
            session_id = self.generate_session_id()
            device_type = random.choice(self.devices)
            last_event_time = datetime.utcnow()

        # Generate event based on weighted probabilities
        event_type = random.choices(
            list(self.event_weights.keys()),
            weights=list(self.event_weights.values()),
            k=1
        )[0]

        # Generate timestamp with some variance from last event
        if session_context:
            time_delta = random.randint(1, 300)  # 1 second to 5 minutes
            event_time = last_event_time + timedelta(seconds=time_delta)
        else:
            event_time = datetime.utcnow()

        # Generate page URL based on event type
        if event_type == 'product_view':
            product_id = random.choice(self.products)
            page_url = f"/product/{product_id}"
        elif event_type in ['add_to_cart', 'remove_from_cart']:
            product_id = random.choice(self.products)
            page_url = f"/product/{product_id}"
        elif event_type == 'search':
            search_terms = ['laptop', 'phone', 'headphones', 'shoes', 'clothing', 'electronics']
            search_term = random.choice(search_terms)
            page_url = f"/search?q={search_term}"
        elif event_type in ['filter_apply', 'sort_apply']:
            page_url = '/catalog'
        else:
            page_url = random.choice(self.pages)

        # Generate metadata based on event type
        metadata = {
            'user_agent': f"{device_type}_browser_v{random.randint(90, 120)}",
            'ip_address': f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            'screen_resolution': self.get_screen_resolution(device_type),
            'referrer': random.choice(self.referrers)
        }

        # Add event-specific metadata
        if event_type == 'product_view':
            metadata.update({
                'product_id': product_id,
                'time_spent_seconds': random.randint(10, 300),
                'scroll_depth': random.randint(20, 100),
                'images_viewed': random.randint(1, 5)
            })
        elif event_type in ['add_to_cart', 'remove_from_cart']:
            metadata.update({
                'product_id': product_id,
                'quantity': random.randint(1, 3),
                'price': round(random.uniform(10.99, 299.99), 2)
            })
        elif event_type == 'search':
            metadata.update({
                'search_term': search_term,
                'results_count': random.randint(0, 1000),
                'filters_applied': random.choice([True, False])
            })
        elif event_type == 'purchase_complete':
            metadata.update({
                'order_id': f"ORD-{uuid.uuid4().hex[:8].upper()}",
                'total_amount': round(random.uniform(25.00, 500.00), 2),
                'payment_method': random.choice(['credit_card', 'paypal', 'apple_pay', 'google_pay'])
            })

        event = {
            'event_id': str(uuid.uuid4()),
            'session_id': session_id,
            'customer_id': customer_id,
            'event_type': event_type,
            'event_time': event_time.isoformat(),
            'page_url': page_url,
            'device_type': device_type,
            'metadata': metadata,
            'ingestion_time': datetime.utcnow().isoformat()
        }

        return event

    def get_screen_resolution(self, device_type: str) -> str:
        """Get realistic screen resolution based on device type"""
        resolutions = {
            'desktop': ['1920x1080', '2560x1440', '1366x768', '1440x900'],
            'mobile': ['375x667', '414x896', '360x640', '375x812'],
            'tablet': ['768x1024', '834x1112', '810x1080', '1024x768']
        }
        return random.choice(resolutions[device_type])

    def generate_user_session(self, duration_minutes: int = 30) -> List[Dict]:
        """Generate a realistic user session with multiple events"""
        session_events = []
        customer_id = random.choice(self.customers)
        session_id = self.generate_session_id()
        device_type = random.choice(self.devices)
        
        session_context = {
            'customer_id': customer_id,
            'session_id': session_id,
            'device_type': device_type,
            'last_event_time': datetime.utcnow()
        }

        # Generate 5-15 events per session
        num_events = random.randint(5, 15)
        
        for i in range(num_events):
            event = self.generate_user_event(session_context)
            session_events.append(event)
            session_context['last_event_time'] = datetime.fromisoformat(event['event_time'])
            
            # Simulate realistic time gaps between events
            time.sleep(random.uniform(0.1, 2.0))

        return session_events

    def send_event(self, event: Dict) -> bool:
        """Send single event to Kafka"""
        try:
            # Use customer_id as partition key for related events
            key = event['customer_id']
            
            future = self.producer.send(
                self.topic,
                key=key,
                value=event
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Event sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send event: {e}")
            return False

    def run_producer(self, events_per_minute: int = 1000, duration_minutes: int = None):
        """Run the producer to generate continuous stream of events"""
        logger.info(f"Starting user activity producer...")
        logger.info(f"Target: {events_per_minute} events per minute")
        logger.info(f"Kafka topic: {self.topic}")
        
        events_sent = 0
        start_time = time.time()
        
        try:
            while True:
                # Calculate sleep time to maintain target rate
                sleep_time = 60.0 / events_per_minute
                
                # Generate event (mix of single events and sessions)
                if random.random() < 0.7:  # 70% single events
                    event = self.generate_user_event()
                    if self.send_event(event):
                        events_sent += 1
                else:  # 30% session-based events
                    session_events = self.generate_user_session()
                    for event in session_events:
                        if self.send_event(event):
                            events_sent += 1
                
                # Log progress every 100 events
                if events_sent % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = events_sent / elapsed * 60 if elapsed > 0 else 0
                    logger.info(f"Sent {events_sent} events, rate: {rate:.1f} events/min")
                
                # Check duration limit
                if duration_minutes and (time.time() - start_time) >= duration_minutes * 60:
                    break
                
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            # Flush any remaining messages
            self.producer.flush()
            self.producer.close()
            
            elapsed = time.time() - start_time
            logger.info(f"Producer finished. Sent {events_sent} events in {elapsed:.1f} seconds")

if __name__ == "__main__":
    import os
    
    # Get configuration from environment
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    events_per_minute = int(os.getenv('EVENTS_PER_MINUTE', '1000'))
    duration = int(os.getenv('DURATION_MINUTES', '0')) if os.getenv('DURATION_MINUTES') else None
    
    # Create and run producer
    producer = UserActivityProducer(bootstrap_servers=kafka_servers)
    producer.run_producer(
        events_per_minute=events_per_minute,
        duration_minutes=duration
    )