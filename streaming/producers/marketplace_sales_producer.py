 # streaming/producers/marketplace_sales_producer.py

import json
import os
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

class MarketplaceSalesProducer:
    def __init__(self, bootstrap_servers: str = 'kafka:29092'):
        """Initialize Kafka producer for marketplace sales with late arrivals"""
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'marketplace_sales'
        
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
        
        # Sample data
        self.marketplaces = ['Amazon', 'eBay', 'Shopify', 'Etsy', 'Walmart']
        self.products = [f"PROD-{i:06d}" for i in range(1, 1001)]
        self.sellers = [f"SELLER-{i:05d}" for i in range(1, 501)]
        self.currencies = ['USD', 'EUR', 'GBP', 'CAD', 'AUD']
        self.payment_methods = ['credit_card', 'paypal', 'bank_transfer', 'digital_wallet']
        self.statuses = ['completed', 'pending', 'cancelled', 'refunded']

    def generate_marketplace_sale(self, delayed_hours: int = 0) -> Dict:
        """Generate a marketplace sales record with optional delay"""
        
        # Generate transaction time (could be in the past for late arrivals)
        base_time = datetime.utcnow() - timedelta(hours=delayed_hours)
        transaction_time = base_time - timedelta(
            hours=random.randint(0, 48),  # Up to 48 hours ago
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # Settlement time is always later than transaction time
        settlement_delay_hours = random.randint(6, 48)  # 6-48 hours delay
        settlement_time = transaction_time + timedelta(hours=settlement_delay_hours)
        
        # Generate sale data
        marketplace = random.choice(self.marketplaces)
        product_id = random.choice(self.products)
        seller_id = random.choice(self.sellers)
        
        # Generate realistic amounts based on marketplace
        marketplace_price_ranges = {
            'Amazon': (10, 200),
            'eBay': (5, 150),
            'Shopify': (15, 300),
            'Etsy': (8, 80),
            'Walmart': (12, 180)
        }
        
        price_range = marketplace_price_ranges.get(marketplace, (10, 100))
        amount = round(random.uniform(price_range[0], price_range[1]), 2)
        
        # Add marketplace-specific fields
        marketplace_metadata = self.generate_marketplace_metadata(marketplace, amount)
        
        sale = {
            'transaction_id': f"TXN-{marketplace[:3].upper()}-{uuid.uuid4().hex[:8].upper()}",
            'marketplace_name': marketplace,
            'seller_id': seller_id,
            'product_id': product_id,
            'amount': amount,
            'currency': random.choice(self.currencies),
            'quantity': random.randint(1, 5),
            'transaction_time': transaction_time.isoformat(),
            'settlement_time': settlement_time.isoformat(),
            'payment_method': random.choice(self.payment_methods),
            'status': random.choice(self.statuses),
            'marketplace_metadata': marketplace_metadata,
            'ingestion_time': datetime.utcnow().isoformat()
        }
        
        return sale

    def generate_marketplace_metadata(self, marketplace: str, amount: float) -> Dict:
        """Generate marketplace-specific metadata"""
        
        base_metadata = {
            'shipping_cost': round(random.uniform(0, 15), 2),
            'tax_amount': round(amount * random.uniform(0.05, 0.12), 2),
            'commission_rate': round(random.uniform(0.08, 0.15), 4),
            'listing_id': f"LIST-{random.randint(1000000, 9999999)}"
        }
        
        # Marketplace-specific metadata
        if marketplace == 'Amazon':
            base_metadata.update({
                'fulfillment_center': random.choice(['FC-001', 'FC-002', 'FC-003']),
                'prime_eligible': random.choice([True, False]),
                'category_rank': random.randint(1, 100000)
            })
        elif marketplace == 'eBay':
            base_metadata.update({
                'auction_type': random.choice(['fixed_price', 'auction', 'best_offer']),
                'listing_duration': random.randint(1, 30),
                'watchers_count': random.randint(0, 50)
            })
        elif marketplace == 'Shopify':
            base_metadata.update({
                'store_name': f"Store-{random.randint(1000, 9999)}",
                'theme_name': random.choice(['Dawn', 'Narrative', 'Brooklyn']),
                'app_integrations': random.randint(1, 15)
            })
        elif marketplace == 'Etsy':
            base_metadata.update({
                'shop_name': f"CreativeShop{random.randint(100, 999)}",
                'handmade': random.choice([True, False]),
                'processing_time_days': random.randint(1, 14)
            })
        elif marketplace == 'Walmart':
            base_metadata.update({
                'walmart_item_id': f"WM-{random.randint(10000000, 99999999)}",
                'pickup_available': random.choice([True, False]),
                'delivery_time_hours': random.randint(2, 48)
            })
        
        return base_metadata

    def generate_late_arrival_batch(self, batch_size: int = 50) -> List[Dict]:
        """Generate a batch of late-arriving sales data"""
        
        batch = []
        for _ in range(batch_size):
            # Random delay between 0-48 hours for late arrivals
            delay_hours = random.choice([
                0,  # 30% immediate
                0,
                0,
                random.randint(1, 6),    # 25% within 6 hours
                random.randint(6, 24),   # 25% within 24 hours  
                random.randint(24, 48)   # 20% within 48 hours
            ])
            
            sale = self.generate_marketplace_sale(delayed_hours=delay_hours)
            batch.append(sale)
        
        return batch

    def send_sale(self, sale: Dict) -> bool:
        """Send single sale to Kafka"""
        try:
            # Use transaction_id as partition key
            key = sale['transaction_id']
            
            future = self.producer.send(
                self.topic,
                key=key,
                value=sale
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Sale sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send sale: {e}")
            return False

    def simulate_marketplace_settlement_cycle(self):
        """Simulate realistic marketplace settlement patterns"""
        
        # Different marketplaces have different settlement patterns
        settlement_patterns = {
            'Amazon': {'freq_minutes': 60, 'batch_size': 20},      # Hourly, larger batches
            'eBay': {'freq_minutes': 30, 'batch_size': 15},        # Every 30 min, medium batches
            'Shopify': {'freq_minutes': 15, 'batch_size': 10},     # Every 15 min, smaller batches
            'Etsy': {'freq_minutes': 120, 'batch_size': 8},        # Every 2 hours, small batches
            'Walmart': {'freq_minutes': 45, 'batch_size': 25}      # Every 45 min, large batches
        }
        
        logger.info("Starting marketplace settlement cycle simulation")
        last_settlement = {marketplace: datetime.utcnow() for marketplace in self.marketplaces}
        
        while True:
            current_time = datetime.utcnow()
            
            for marketplace, pattern in settlement_patterns.items():
                time_since_last = (current_time - last_settlement[marketplace]).total_seconds() / 60
                
                if time_since_last >= pattern['freq_minutes']:
                    logger.info(f"Processing {marketplace} settlement batch")
                    
                    # Generate batch for this marketplace
                    batch_size = random.randint(
                        max(1, pattern['batch_size'] - 5),
                        pattern['batch_size'] + 5
                    )
                    
                    batch = []
                    for _ in range(batch_size):
                        sale = self.generate_marketplace_sale()
                        # Force this sale to be from the current marketplace
                        sale['marketplace_name'] = marketplace
                        batch.append(sale)
                    
                    # Send batch
                    for sale in batch:
                        self.send_sale(sale)
                    
                    last_settlement[marketplace] = current_time
                    logger.info(f"Sent {len(batch)} sales from {marketplace}")
            
            # Sleep for a short interval
            time.sleep(30)  # Check every 30 seconds

    def run_producer(self, sales_per_hour: int = 100, duration_minutes: int = None):
        """Run the producer with configurable rate and duration"""
        logger.info(f"Starting marketplace sales producer...")
        logger.info(f"Target: {sales_per_hour} sales per hour")
        logger.info(f"Kafka topic: {self.topic}")
        
        # Check for delay demo mode
        delay_demo_mode = os.getenv('DELAY_DEMO_MODE', 'false').lower() == 'true'
        late_arrival_batch = os.getenv('LATE_ARRIVAL_BATCH', 'false').lower() == 'true'
        batch_size = int(os.getenv('BATCH_SIZE', '50'))
        
        if late_arrival_batch:
            logger.info(f"🚨 LATE ARRIVAL BATCH MODE - Sending {batch_size} delayed records")
            self.send_late_arrival_batch(batch_size)
            return
        
        sales_sent = 0
        start_time = time.time()
        
        try:
            if delay_demo_mode:
                logger.info("🎯 DELAY DEMO MODE - Enhanced delay patterns for demonstration")
                self.run_delay_demo_mode(sales_per_hour, duration_minutes)
            elif sales_per_hour < 200:  # Use settlement cycle for lower rates
                self.simulate_marketplace_settlement_cycle()
            else:
                # Use constant rate for higher throughput
                sleep_time = 3600.0 / sales_per_hour  # seconds between sales
                
                while True:
                    # Generate sale with random late arrival
                    sale = self.generate_marketplace_sale(
                        delayed_hours=random.choice([0, 0, 0, random.randint(1, 48)])
                    )
                    
                    if self.send_sale(sale):
                        sales_sent += 1
                    
                    # Log progress every 50 sales
                    if sales_sent % 50 == 0:
                        elapsed = time.time() - start_time
                        rate = sales_sent / elapsed * 3600 if elapsed > 0 else 0
                        logger.info(f"Sent {sales_sent} sales, rate: {rate:.1f} sales/hour")
                    
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
            logger.info(f"Producer finished. Sent {sales_sent} sales in {elapsed:.1f} seconds")

    def run_delay_demo_mode(self, sales_per_hour: int, duration_minutes: int = None):
        """Run producer in delay demonstration mode with enhanced patterns"""
        logger.info("🎯 Starting Delay Demo Mode...")
        
        sales_sent = 0
        immediate_sales = 0
        delayed_sales = 0
        start_time = time.time()
        sleep_time = 3600.0 / sales_per_hour
        
        # Define delay patterns for demonstration
        delay_patterns = {
            'immediate': 0,      # 40% immediate
            'short_delay': lambda: random.randint(1, 6),     # 30% 1-6 hours
            'medium_delay': lambda: random.randint(6, 24),   # 20% 6-24 hours  
            'long_delay': lambda: random.randint(24, 48)     # 10% 24-48 hours
        }
        
        pattern_weights = [0.4, 0.3, 0.2, 0.1]
        pattern_names = list(delay_patterns.keys())
        
        while True:
            try:
                # Choose delay pattern based on weights
                pattern = random.choices(pattern_names, weights=pattern_weights)[0]
                
                if pattern == 'immediate':
                    delay_hours = 0
                    immediate_sales += 1
                else:
                    delay_hours = delay_patterns[pattern]()
                    delayed_sales += 1
                
                # Generate and send sale
                sale = self.generate_marketplace_sale(delayed_hours=delay_hours)
                
                # Add demo metadata
                sale['demo_metadata'] = {
                    'delay_pattern': pattern,
                    'demo_mode': True,
                    'delay_hours': delay_hours
                }
                
                if self.send_sale(sale):
                    sales_sent += 1
                
                # Enhanced logging for demo
                if sales_sent % 25 == 0:
                    elapsed = time.time() - start_time
                    rate = sales_sent / elapsed * 3600 if elapsed > 0 else 0
                    logger.info(f"📊 Demo Stats: {sales_sent} total | {immediate_sales} immediate | {delayed_sales} delayed | Rate: {rate:.1f}/hr")
                
                # Check duration limit  
                if duration_minutes and (time.time() - start_time) >= duration_minutes * 60:
                    break
                
                time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error in delay demo mode: {e}")
                time.sleep(1)
        
        logger.info(f"🎯 Delay Demo Completed: {immediate_sales} immediate, {delayed_sales} delayed sales")

    def send_late_arrival_batch(self, batch_size: int = 50):
        """Send a batch of significantly delayed sales for late arrival demonstration"""
        logger.info(f"📦 Generating late arrival batch of {batch_size} records...")
        
        batch_sent = 0
        
        for i in range(batch_size):
            # Generate sales with significant delays (12-48 hours ago)
            delay_hours = random.randint(12, 48)
            sale = self.generate_marketplace_sale(delayed_hours=delay_hours)
            
            # Add late arrival metadata
            delay_severity = "high" if delay_hours > 36 else "medium" if delay_hours > 24 else "low"
            sale['late_arrival_metadata'] = {
                'is_late_arrival': True,
                'delay_hours': delay_hours,
                'delay_severity': delay_severity,
                'batch_id': f"LATE_BATCH_{int(time.time())}",
                'record_number': i + 1
            }
            
            if self.send_sale(sale):
                batch_sent += 1
                
            # Log progress
            if (i + 1) % 10 == 0:
                logger.info(f"📦 Late batch progress: {i + 1}/{batch_size} sent")
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        logger.info(f"✅ Late arrival batch completed: {batch_sent}/{batch_size} records sent")

if __name__ == "__main__":
    import os
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Marketplace Sales Producer')
    parser.add_argument('--late-batch', action='store_true', help='Send late arrival batch')
    args = parser.parse_args()
    
    # Get configuration from environment
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    sales_per_hour = int(os.getenv('SALES_PER_HOUR', '100'))
    duration = int(os.getenv('DURATION_MINUTES', '0')) if os.getenv('DURATION_MINUTES') else None
    
    # Override for late batch mode
    if args.late_batch:
        os.environ['LATE_ARRIVAL_BATCH'] = 'true'
    
    # Create and run producer
    producer = MarketplaceSalesProducer(bootstrap_servers=kafka_servers)
    producer.run_producer(
        sales_per_hour=sales_per_hour,
        duration_minutes=duration
    )