#!/usr/bin/env python3
"""
Kafka Delay Data Flow Monitor
Real-time monitoring of delayed data patterns in Kafka topics
"""

import json
import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Dict, List
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import threading
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaDelayMonitor:
    """Monitor Kafka topics for delay patterns and late arrivals"""
    
    def __init__(self, bootstrap_servers: str = 'kafka:29092'):
        self.bootstrap_servers = bootstrap_servers
        self.running = True
        self.consumers = {}
        self.delay_stats = defaultdict(lambda: defaultdict(int))
        self.recent_delays = deque(maxlen=1000)  # Store last 1000 delay measurements
        self.monitoring_threads = []
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Topic configurations for monitoring
        self.topics_config = {
            'marketplace_sales': {
                'group_id': 'delay_monitor_sales',
                'auto_offset_reset': 'latest'
            },
            'user_activity_events': {
                'group_id': 'delay_monitor_activity',
                'auto_offset_reset': 'latest'
            }
        }
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down monitor...")
        self.running = False
    
    def create_consumer(self, topic: str) -> KafkaConsumer:
        """Create a Kafka consumer for monitoring"""
        config = self.topics_config.get(topic, {})
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[self.bootstrap_servers],
            group_id=config.get('group_id', f'delay_monitor_{topic}'),
            auto_offset_reset=config.get('auto_offset_reset', 'latest'),
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            session_timeout_ms=30000,
            max_poll_records=100,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            key_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        logger.info(f"Created delay monitor consumer for topic: {topic}")
        return consumer
    
    def calculate_delay(self, message: Dict) -> Dict:
        """Calculate delay metrics for a message"""
        try:
            # Determine timestamp fields based on message type
            if 'transaction_time' in message:  # Marketplace sales
                event_time = datetime.fromisoformat(message['transaction_time'].replace('Z', ''))
                ingestion_time = datetime.fromisoformat(message['ingestion_time'].replace('Z', ''))
                message_type = 'marketplace_sale'
            elif 'event_time' in message:  # User activity
                event_time = datetime.fromisoformat(message['event_time'].replace('Z', ''))
                ingestion_time = datetime.fromisoformat(message['ingestion_time'].replace('Z', ''))
                message_type = 'user_activity'
            else:
                return {'error': 'No timestamp fields found'}
            
            # Calculate delay
            delay_timedelta = ingestion_time - event_time
            delay_hours = delay_timedelta.total_seconds() / 3600
            
            # Categorize delay
            if delay_hours < 0.1:  # < 6 minutes
                delay_category = 'immediate'
            elif delay_hours < 1:
                delay_category = 'short'
            elif delay_hours < 6:
                delay_category = 'medium'
            elif delay_hours < 24:
                delay_category = 'long'
            elif delay_hours < 48:
                delay_category = 'very_long'
            else:
                delay_category = 'critical'
            
            return {
                'message_type': message_type,
                'delay_hours': delay_hours,
                'delay_category': delay_category,
                'event_time': event_time.isoformat(),
                'ingestion_time': ingestion_time.isoformat(),
                'is_delayed': delay_hours > 1.0
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def monitor_marketplace_sales(self):
        """Monitor marketplace sales topic for delays"""
        topic = 'marketplace_sales'
        logger.info(f"Starting delay monitoring for {topic}")
        
        try:
            consumer = self.create_consumer(topic)
            self.consumers[topic] = consumer
            
            messages_processed = 0
            
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
                                if sale:
                                    delay_info = self.calculate_delay(sale)
                                    
                                    if 'error' not in delay_info:
                                        # Update statistics
                                        self.delay_stats[topic][delay_info['delay_category']] += 1
                                        
                                        # Store recent delay
                                        self.recent_delays.append({
                                            'topic': topic,
                                            'timestamp': datetime.utcnow().isoformat(),
                                            'delay_info': delay_info,
                                            'message_id': sale.get('transaction_id', 'unknown')
                                        })
                                        
                                        # Log significant delays
                                        if delay_info['delay_hours'] > 6:
                                            logger.warning(f"🚨 HIGH DELAY DETECTED: {sale.get('transaction_id')} | "
                                                         f"Delay: {delay_info['delay_hours']:.2f}h | "
                                                         f"Category: {delay_info['delay_category']}")
                                        
                                        # Log demo patterns
                                        if 'demo_metadata' in sale:
                                            logger.info(f"🎯 DEMO PATTERN: {sale['demo_metadata']['delay_pattern']} | "
                                                       f"Delay: {delay_info['delay_hours']:.2f}h")
                                        
                                        # Log late arrival batches
                                        if 'late_arrival_metadata' in sale:
                                            batch_info = sale['late_arrival_metadata']
                                            logger.info(f"📦 LATE BATCH: {batch_info['batch_id']} | "
                                                       f"Record: {batch_info['record_number']} | "
                                                       f"Delay: {delay_info['delay_hours']:.2f}h")
                                    
                                    messages_processed += 1
                                    
                            except Exception as e:
                                logger.error(f"Error processing message: {str(e)}")
                                continue
                        
                except Exception as e:
                    logger.error(f"Error in {topic} monitoring loop: {str(e)}")
                    time.sleep(5)
                    
        except Exception as e:
            logger.error(f"Fatal error in {topic} monitor: {str(e)}")
        finally:
            if topic in self.consumers:
                self.consumers[topic].close()
                logger.info(f"Closed monitor consumer for {topic}")
    
    def monitor_user_activity(self):
        """Monitor user activity topic for delays"""
        topic = 'user_activity_events'
        logger.info(f"Starting delay monitoring for {topic}")
        
        try:
            consumer = self.create_consumer(topic)
            self.consumers[topic] = consumer
            
            messages_processed = 0
            
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
                                if event:
                                    delay_info = self.calculate_delay(event)
                                    
                                    if 'error' not in delay_info:
                                        # Update statistics
                                        self.delay_stats[topic][delay_info['delay_category']] += 1
                                        
                                        # Store recent delay
                                        self.recent_delays.append({
                                            'topic': topic,
                                            'timestamp': datetime.utcnow().isoformat(),
                                            'delay_info': delay_info,
                                            'message_id': event.get('event_id', 'unknown')
                                        })
                                        
                                        # Log significant delays (less common for user activity)
                                        if delay_info['delay_hours'] > 1:
                                            logger.info(f"⏰ User Activity Delay: {event.get('event_id')} | "
                                                       f"Delay: {delay_info['delay_hours']:.2f}h")
                                    
                                    messages_processed += 1
                                    
                            except Exception as e:
                                logger.error(f"Error processing message: {str(e)}")
                                continue
                        
                except Exception as e:
                    logger.error(f"Error in {topic} monitoring loop: {str(e)}")
                    time.sleep(5)
                    
        except Exception as e:
            logger.error(f"Fatal error in {topic} monitor: {str(e)}")
        finally:
            if topic in self.consumers:
                self.consumers[topic].close()
                logger.info(f"Closed monitor consumer for {topic}")
    
    def print_delay_statistics(self):
        """Print delay statistics periodically"""
        while self.running:
            try:
                time.sleep(30)  # Print stats every 30 seconds
                
                if not self.running:
                    break
                
                logger.info("📊 === DELAY MONITORING STATISTICS ===")
                
                for topic, stats in self.delay_stats.items():
                    total_messages = sum(stats.values())
                    if total_messages > 0:
                        logger.info(f"📈 {topic.upper()}: {total_messages} messages processed")
                        
                        for category, count in stats.items():
                            percentage = (count / total_messages) * 100
                            logger.info(f"   {category}: {count} ({percentage:.1f}%)")
                
                # Show recent high delays
                recent_high_delays = [
                    d for d in list(self.recent_delays)[-50:]  # Last 50 delays
                    if d['delay_info']['delay_hours'] > 6
                ]
                
                if recent_high_delays:
                    logger.info(f"🚨 Recent High Delays ({len(recent_high_delays)} in last 50):")
                    for delay in recent_high_delays[-5:]:  # Show last 5
                        info = delay['delay_info']
                        logger.info(f"   {delay['message_id']}: {info['delay_hours']:.2f}h ({info['delay_category']})")
                
                logger.info("=" * 50)
                
            except Exception as e:
                logger.error(f"Error printing statistics: {str(e)}")
                time.sleep(5)
    
    def start_monitoring(self, topics: List[str] = None):
        """Start monitoring specified topics"""
        if topics is None:
            topics = list(self.topics_config.keys())
        
        logger.info(f"🚀 Starting Kafka delay monitoring for topics: {topics}")
        
        # Start monitoring threads
        for topic in topics:
            if topic == 'marketplace_sales':
                thread = threading.Thread(
                    target=self.monitor_marketplace_sales,
                    name=f"monitor-{topic}",
                    daemon=True
                )
            elif topic == 'user_activity_events':
                thread = threading.Thread(
                    target=self.monitor_user_activity,
                    name=f"monitor-{topic}",
                    daemon=True
                )
            else:
                logger.warning(f"Unknown topic for monitoring: {topic}")
                continue
            
            thread.start()
            self.monitoring_threads.append(thread)
            logger.info(f"Started monitoring thread for {topic}")
        
        # Start statistics thread
        stats_thread = threading.Thread(
            target=self.print_delay_statistics,
            name="delay-stats",
            daemon=True
        )
        stats_thread.start()
        self.monitoring_threads.append(stats_thread)
        
        logger.info("🎯 Delay monitoring started successfully!")
    
    def stop_monitoring(self):
        """Stop all monitoring threads"""
        logger.info("🛑 Stopping delay monitoring...")
        self.running = False
        
        # Wait for threads to finish
        for thread in self.monitoring_threads:
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
        
        logger.info("Delay monitoring stopped")


def main():
    """Main function to run the delay monitor"""
    import os
    
    # Get configuration from environment
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    topics = os.getenv('MONITOR_TOPICS', 'marketplace_sales,user_activity_events').split(',')
    
    # Create and start monitor
    monitor = KafkaDelayMonitor(bootstrap_servers=kafka_servers)
    
    try:
        monitor.start_monitoring(topics)
        
        # Keep running until interrupted
        while monitor.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Monitor interrupted by user")
    except Exception as e:
        logger.error(f"Monitor error: {str(e)}")
    finally:
        monitor.stop_monitoring()


if __name__ == "__main__":
    main()
