 # streaming/consumers/streaming_consumer.py

import json
import logging
import time
from datetime import datetime
from typing import Dict, List
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import threading
import signal
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamingConsumer:
    def __init__(self, bootstrap_servers: str = 'kafka:29092'):
        """Initialize Kafka consumer for streaming data"""
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}
        self.running = True
        self.consumer_threads = []
        
        # Topic configurations
        self.topics_config = {
            'user_activity_events': {
                'group_id': 'user_activity_consumer_group',
                'auto_offset_reset': 'latest',
                'enable_auto_commit': True,
                'auto_commit_interval_ms': 1000,
                'session_timeout_ms': 30000,
                'max_poll_records': 500
            },
            'marketplace_sales': {
                'group_id': 'marketplace_sales_consumer_group',
                'auto_offset_reset': 'latest',
                'enable_auto_commit': True,
                'auto_commit_interval_ms': 5000,
                'session_timeout_ms': 30000,
                'max_poll_records': 100
            }
        }
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down consumers...")
        self.running = False

    def create_consumer(self, topic: str) -> KafkaConsumer:
        """Create a Kafka consumer for a specific topic"""
        config = self.topics_config.get(topic, {})
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[self.bootstrap_servers],
            group_id=config.get('group_id', f'{topic}_consumer_group'),
            auto_offset_reset=config.get('auto_offset_reset', 'latest'),
            enable_auto_commit=config.get('enable_auto_commit', True),
            auto_commit_interval_ms=config.get('auto_commit_interval_ms', 1000),
            session_timeout_ms=config.get('session_timeout_ms', 30000),
            max_poll_records=config.get('max_poll_records', 500),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            key_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        logger.info(f"Created consumer for topic: {topic}")
        return consumer

    def process_user_activity_event(self, event: Dict) -> bool:
        """Process individual user activity event"""
        try:
            # Validate required fields
            required_fields = ['event_id', 'customer_id', 'event_type', 'event_time']
            for field in required_fields:
                if field not in event or event[field] is None:
                    logger.warning(f"Missing required field '{field}' in event: {event.get('event_id', 'unknown')}")
                    return False
            
            # Validate event type
            valid_event_types = [
                'page_view', 'product_view', 'add_to_cart', 'remove_from_cart',
                'add_to_wishlist', 'search', 'filter_apply', 'sort_apply',
                'checkout_start', 'purchase_complete', 'user_login', 'user_logout'
            ]
            
            if event['event_type'] not in valid_event_types:
                logger.warning(f"Invalid event type: {event['event_type']}")
                return False
            
            # Add processing timestamp
            event['processing_time'] = datetime.utcnow().isoformat()
            
            # Log event processing (sample only for high-volume)
            if hash(event['event_id']) % 1000 == 0:  # Log 0.1% of events
                logger.debug(f"Processed user activity event: {event['event_type']} from {event['customer_id']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing user activity event: {str(e)}")
            return False

    def process_marketplace_sale(self, sale: Dict) -> bool:
        """Process individual marketplace sale event"""
        try:
            # Validate required fields
            required_fields = ['transaction_id', 'marketplace_name', 'product_id', 'amount']
            for field in required_fields:
                if field not in sale or sale[field] is None:
                    logger.warning(f"Missing required field '{field}' in sale: {sale.get('transaction_id', 'unknown')}")
                    return False
            
            # Validate amount
            if not isinstance(sale['amount'], (int, float)) or sale['amount'] <= 0:
                logger.warning(f"Invalid amount in sale: {sale['amount']}")
                return False
            
            # Validate marketplace
            valid_marketplaces = ['Amazon', 'eBay', 'Shopify', 'Etsy', 'Walmart']
            if sale['marketplace_name'] not in valid_marketplaces:
                logger.warning(f"Unknown marketplace: {sale['marketplace_name']}")
            
            # Calculate delay information for demonstration
            delay_info = self.calculate_delay_metrics(sale)
            sale['delay_analysis'] = delay_info
            
            # Add processing timestamp
            sale['processing_time'] = datetime.utcnow().isoformat()
            
            # Enhanced logging for delay demonstration
            if delay_info['is_delayed'] or 'late_arrival_metadata' in sale:
                delay_hours = delay_info['delay_hours']
                delay_severity = delay_info['delay_severity']
                logger.info(f"🕐 DELAYED SALE DETECTED: {sale['transaction_id']} | "
                           f"Delay: {delay_hours:.2f}h | Severity: {delay_severity} | "
                           f"Marketplace: {sale['marketplace_name']}")
            
            # Log demo-specific metadata
            if 'demo_metadata' in sale and sale['demo_metadata'].get('demo_mode'):
                pattern = sale['demo_metadata']['delay_pattern']
                logger.info(f"🎯 DEMO SALE: {sale['transaction_id']} | Pattern: {pattern}")
            
            # Log late arrival batches  
            if 'late_arrival_metadata' in sale:
                batch_info = sale['late_arrival_metadata']
                logger.info(f"📦 LATE BATCH: {sale['transaction_id']} | "
                           f"Batch: {batch_info['batch_id']} | "
                           f"Record: {batch_info['record_number']} | "
                           f"Severity: {batch_info['delay_severity']}")
            
            # Log processing for monitoring (sample high-volume events)
            if hash(sale['transaction_id']) % 100 == 0:  # Log 1% of sales
                logger.debug(f"Processed marketplace sale: {sale['marketplace_name']} - ${sale['amount']:.2f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing marketplace sale: {str(e)}")
            return False

    def calculate_delay_metrics(self, sale: Dict) -> Dict:
        """Calculate delay metrics for a sale event"""
        try:
            # Parse timestamps
            transaction_time = datetime.fromisoformat(sale['transaction_time'].replace('Z', ''))
            ingestion_time = datetime.fromisoformat(sale['ingestion_time'].replace('Z', ''))
            
            # Calculate delay in hours
            delay_timedelta = ingestion_time - transaction_time
            delay_hours = delay_timedelta.total_seconds() / 3600
            
            # Determine delay severity
            if delay_hours < 1:
                delay_severity = "immediate"
            elif delay_hours < 6:
                delay_severity = "short"
            elif delay_hours < 24:
                delay_severity = "medium"
            elif delay_hours < 48:
                delay_severity = "long"
            else:
                delay_severity = "critical"
            
            return {
                'delay_hours': delay_hours,
                'delay_severity': delay_severity,
                'is_delayed': delay_hours > 1.0,
                'transaction_time': transaction_time.isoformat(),
                'ingestion_time': ingestion_time.isoformat()
            }
            
        except Exception as e:
            logger.warning(f"Could not calculate delay metrics: {str(e)}")
            return {
                'delay_hours': 0.0,
                'delay_severity': "unknown",
                'is_delayed': False,
                'error': str(e)
            }
                # Don't return False, as we want to capture unknown marketplaces too
            
            # Add processing timestamp
            sale['processing_time'] = datetime.utcnow().isoformat()
            
            # Check for late arrival
            if 'transaction_time' in sale and 'ingestion_time' in sale:
                try:
                    trans_time = datetime.fromisoformat(sale['transaction_time'].replace('Z', '+00:00'))
                    ingest_time = datetime.fromisoformat(sale['ingestion_time'].replace('Z', '+00:00'))
                    delay_hours = (ingest_time - trans_time).total_seconds() / 3600
                    
                    if delay_hours > 1:  # More than 1 hour delay
                        logger.info(f"Late arrival detected: {sale['transaction_id']} "
                                   f"delayed by {delay_hours:.2f} hours")
                        sale['is_late_arrival'] = True
                        sale['delay_hours'] = delay_hours
                    else:
                        sale['is_late_arrival'] = False
                        
                except Exception as e:
                    logger.warning(f"Could not calculate delay for {sale['transaction_id']}: {str(e)}")
            
            logger.debug(f"Processed marketplace sale: {sale['transaction_id']} "
                        f"from {sale['marketplace_name']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing marketplace sale: {str(e)}")
            return False

    def consume_user_activity_events(self):
        """Consumer thread for user activity events"""
        topic = 'user_activity_events'
        logger.info(f"Starting consumer thread for {topic}")
        
        try:
            consumer = self.create_consumer(topic)
            self.consumers[topic] = consumer
            
            events_processed = 0
            last_log_time = time.time()
            
            while self.running:
                try:
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.running:
                                break
                            
                            try:
                                event = message.value
                                if event and self.process_user_activity_event(event):
                                    events_processed += 1
                                    
                            except Exception as e:
                                logger.error(f"Error processing message from {topic}: {str(e)}")
                                continue
                    
                    # Log progress every minute
                    if time.time() - last_log_time > 60:
                        logger.info(f"User activity consumer processed {events_processed} events")
                        last_log_time = time.time()
                        
                except Exception as e:
                    logger.error(f"Error in user activity consumer loop: {str(e)}")
                    time.sleep(5)  # Wait before retrying
                    
        except Exception as e:
            logger.error(f"Fatal error in user activity consumer: {str(e)}")
        finally:
            if topic in self.consumers:
                self.consumers[topic].close()
                logger.info(f"Closed consumer for {topic}")

    def consume_marketplace_sales(self):
        """Consumer thread for marketplace sales"""
        topic = 'marketplace_sales'
        logger.info(f"Starting consumer thread for {topic}")
        
        try:
            consumer = self.create_consumer(topic)
            self.consumers[topic] = consumer
            
            # Enhanced tracking for delay demonstration
            sales_processed = 0
            delay_stats = {
                'immediate': 0,    # < 1 hour
                'short': 0,        # 1-6 hours
                'medium': 0,       # 6-24 hours
                'long': 0,         # 24-48 hours
                'critical': 0      # > 48 hours
            }
            late_arrivals_count = 0
            demo_sales_count = 0
            batch_sales_count = 0
            last_log_time = time.time()
            
            while self.running:
                try:
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.running:
                                break
                            
                            try:
                                sale = message.value
                                if sale and self.process_marketplace_sale(sale):
                                    sales_processed += 1
                                    
                                    # Track delay statistics
                                    if 'delay_analysis' in sale:
                                        delay_severity = sale['delay_analysis']['delay_severity']
                                        if delay_severity in delay_stats:
                                            delay_stats[delay_severity] += 1
                                        
                                        if sale['delay_analysis']['is_delayed']:
                                            late_arrivals_count += 1
                                    
                                    # Track demo metadata
                                    if 'demo_metadata' in sale:
                                        demo_sales_count += 1
                                    
                                    # Track late arrival batches
                                    if 'late_arrival_metadata' in sale:
                                        batch_sales_count += 1
                                        
                            except Exception as e:
                                logger.error(f"Error processing message from {topic}: {str(e)}")
                                continue
                    
                    # Enhanced logging every minute with delay statistics
                    if time.time() - last_log_time > 60:
                        total_delayed = sum(delay_stats[k] for k in ['short', 'medium', 'long', 'critical'])
                        delay_percentage = (total_delayed / sales_processed * 100) if sales_processed > 0 else 0
                        
                        logger.info(f"📊 Marketplace Sales Stats: {sales_processed} total | "
                                   f"{delay_percentage:.1f}% delayed | Demo: {demo_sales_count} | "
                                   f"Batches: {batch_sales_count}")
                        logger.info(f"🕐 Delay Breakdown: Immediate: {delay_stats['immediate']} | "
                                   f"Short: {delay_stats['short']} | Medium: {delay_stats['medium']} | "
                                   f"Long: {delay_stats['long']} | Critical: {delay_stats['critical']}")
                        last_log_time = time.time()
                        
                except Exception as e:
                    logger.error(f"Error in marketplace sales consumer loop: {str(e)}")
                    time.sleep(5)  # Wait before retrying
                    
        except Exception as e:
            logger.error(f"Fatal error in marketplace sales consumer: {str(e)}")
        finally:
            if topic in self.consumers:
                self.consumers[topic].close()
                logger.info(f"Closed consumer for {topic}")

    def get_consumer_lag(self, topic: str) -> Dict:
        """Get consumer lag information for monitoring"""
        try:
            if topic not in self.consumers:
                return {"error": "Consumer not found"}
            
            consumer = self.consumers[topic]
            
            # Get assigned partitions
            assigned_partitions = consumer.assignment()
            if not assigned_partitions:
                return {"lag": 0, "partitions": 0}
            
            # Get end offsets (latest available)
            end_offsets = consumer.end_offsets(assigned_partitions)
            
            total_lag = 0
            partition_count = 0
            
            for partition in assigned_partitions:
                try:
                    # Get current position
                    current_offset = consumer.position(partition)
                    end_offset = end_offsets.get(partition, current_offset)
                    
                    lag = end_offset - current_offset
                    total_lag += lag
                    partition_count += 1
                    
                except Exception as e:
                    logger.warning(f"Could not get lag for partition {partition}: {str(e)}")
            
            return {
                "total_lag": total_lag,
                "partitions": partition_count,
                "avg_lag": total_lag / partition_count if partition_count > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting consumer lag for {topic}: {str(e)}")
            return {"error": str(e)}

    def health_check(self) -> Dict:
        """Perform health check on consumers"""
        health_status = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": "healthy",
            "consumers": {}
        }
        
        for topic in self.topics_config.keys():
            if topic in self.consumers:
                try:
                    lag_info = self.get_consumer_lag(topic)
                    consumer_health = {
                        "status": "running",
                        "lag_info": lag_info
                    }
                    
                    # Check if lag is too high
                    if isinstance(lag_info.get("total_lag"), int) and lag_info["total_lag"] > 10000:
                        consumer_health["status"] = "lagging"
                        health_status["overall_status"] = "degraded"
                        
                except Exception as e:
                    consumer_health = {
                        "status": "error",
                        "error": str(e)
                    }
                    health_status["overall_status"] = "unhealthy"
            else:
                consumer_health = {
                    "status": "not_running"
                }
                health_status["overall_status"] = "unhealthy"
            
            health_status["consumers"][topic] = consumer_health
        
        return health_status

    def start_consumers(self, topics: List[str] = None):
        """Start consumer threads for specified topics"""
        if topics is None:
            topics = list(self.topics_config.keys())
        
        logger.info(f"Starting consumers for topics: {topics}")
        
        for topic in topics:
            if topic == 'user_activity_events':
                thread = threading.Thread(
                    target=self.consume_user_activity_events,
                    name=f"consumer-{topic}",
                    daemon=True
                )
            elif topic == 'marketplace_sales':
                thread = threading.Thread(
                    target=self.consume_marketplace_sales,
                    name=f"consumer-{topic}",
                    daemon=True
                )
            else:
                logger.warning(f"Unknown topic: {topic}")
                continue
            
            thread.start()
            self.consumer_threads.append(thread)
            logger.info(f"Started consumer thread for {topic}")

    def stop_consumers(self):
        """Stop all consumers gracefully"""
        logger.info("Stopping all consumers...")
        self.running = False
        
        # Wait for threads to finish
        for thread in self.consumer_threads:
            thread.join(timeout=10)
            if thread.is_alive():
                logger.warning(f"Thread {thread.name} did not stop gracefully")
        
        # Close all consumers
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
                logger.info(f"Closed consumer for {topic}")
            except Exception as e:
                logger.error(f"Error closing consumer for {topic}: {str(e)}")
        
        logger.info("All consumers stopped")

    def run_monitoring_loop(self, interval_seconds: int = 60):
        """Run monitoring loop to track consumer health"""
        logger.info(f"Starting monitoring loop (interval: {interval_seconds}s)")
        
        while self.running:
            try:
                health = self.health_check()
                
                if health["overall_status"] != "healthy":
                    logger.warning(f"Consumer health check: {health['overall_status']}")
                    for topic, status in health["consumers"].items():
                        if status["status"] != "running":
                            logger.warning(f"  {topic}: {status['status']}")
                else:
                    logger.info("All consumers healthy")
                
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {str(e)}")
                time.sleep(10)

def main():
    # Get configuration from environment
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    topics = os.getenv('CONSUMER_TOPICS', 'user_activity_events,marketplace_sales').split(',')
    monitoring_interval = int(os.getenv('MONITORING_INTERVAL', '60'))
    
    # Create and start consumers
    consumer_manager = StreamingConsumer(bootstrap_servers=kafka_servers)
    
    try:
        # Start consumers
        consumer_manager.start_consumers(topics)
        
        # Run monitoring loop
        consumer_manager.run_monitoring_loop(monitoring_interval)
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Consumer manager error: {str(e)}")
    finally:
        consumer_manager.stop_consumers()

if __name__ == "__main__":
    main()