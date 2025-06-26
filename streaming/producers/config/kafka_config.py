 # streaming/producers/config/kafka_config.py

import os
from typing import Dict, Any
import logging
import json

logger = logging.getLogger(__name__)

class KafkaConfig:
    """Centralized Kafka configuration management"""
    
    def __init__(self, environment: str = "local"):
        self.environment = environment
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    def get_producer_config(self, producer_type: str = "default") -> Dict[str, Any]:
        """Get producer configuration"""
        base_config = {
            'bootstrap_servers': [self.bootstrap_servers],
            'acks': 'all',
            'retries': 3,
            'batch_size': 16384,
            'linger_ms': 10,
            'compression_type': 'gzip',
            'max_in_flight_requests_per_connection': 5,
            'enable_idempotence': True,
            'value_serializer': lambda x: json.dumps(x, default=str).encode('utf-8'),
            'key_serializer': lambda x: x.encode('utf-8') if x else None
        }
        
        if producer_type == "high_throughput":
            base_config.update({
                'batch_size': 32768,
                'linger_ms': 20,
                'compression_type': 'lz4',
                'buffer_memory': 67108864  # 64MB
            })
        elif producer_type == "low_latency":
            base_config.update({
                'batch_size': 0,
                'linger_ms': 0,
                'compression_type': 'none'
            })
        
        return base_config
    
    def get_consumer_config(self, group_id: str, consumer_type: str = "default") -> Dict[str, Any]:
        """Get consumer configuration"""
        base_config = {
            'bootstrap_servers': [self.bootstrap_servers],
            'group_id': group_id,
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'auto_commit_interval_ms': 1000,
            'session_timeout_ms': 30000,
            'heartbeat_interval_ms': 10000,
            'max_poll_records': 500,
            'max_poll_interval_ms': 300000
        }
        
        if consumer_type == "high_throughput":
            base_config.update({
                'max_poll_records': 1000,
                'fetch_min_bytes': 1048576,  # 1MB
                'fetch_max_wait_ms': 500
            })
        elif consumer_type == "low_latency":
            base_config.update({
                'max_poll_records': 100,
                'fetch_min_bytes': 1,
                'fetch_max_wait_ms': 50
            })
        
        return base_config
    
    def get_topic_config(self, topic_name: str) -> Dict[str, Any]:
        """Get topic-specific configuration"""
        topic_configs = {
            'user_activity_events': {
                'partitions': 6,
                'replication_factor': 1,
                'configs': {
                    'retention.ms': '604800000',  # 7 days
                    'segment.ms': '86400000',     # 1 day
                    'cleanup.policy': 'delete',
                    'compression.type': 'snappy'
                }
            },
            'marketplace_sales': {
                'partitions': 3,
                'replication_factor': 1,
                'configs': {
                    'retention.ms': '2592000000',  # 30 days
                    'segment.ms': '86400000',      # 1 day
                    'cleanup.policy': 'delete',
                    'compression.type': 'snappy'
                }
            }
        }
        
        return topic_configs.get(topic_name, {
            'partitions': 1,
            'replication_factor': 1,
            'configs': {}
        })
    
    def get_schema_registry_config(self) -> Dict[str, Any]:
        """Get Schema Registry configuration (if using Confluent)"""
        return {
            'url': os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081'),
            'basic_auth_user_info': os.getenv('SCHEMA_REGISTRY_AUTH', ''),
            'auto_register_schemas': True
        }
    
    def validate_config(self) -> bool:
        """Validate Kafka configuration"""
        try:
            from kafka import KafkaProducer
            
            # Test connection
            producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                api_version=(0, 10, 1)
            )
            producer.close()
            
            logger.info("Kafka configuration validation successful")
            return True
            
        except Exception as e:
            logger.error(f"Kafka configuration validation failed: {str(e)}")
            return False


# Topic schemas and configurations
TOPIC_SCHEMAS = {
    'user_activity_events': {
        'key_schema': {
            'type': 'string',
            'description': 'Customer ID as partition key'
        },
        'value_schema': {
            'type': 'record',
            'name': 'UserActivityEvent',
            'fields': [
                {'name': 'event_id', 'type': 'string'},
                {'name': 'session_id', 'type': 'string'},
                {'name': 'customer_id', 'type': 'string'},
                {'name': 'event_type', 'type': 'string'},
                {'name': 'event_time', 'type': 'string'},
                {'name': 'page_url', 'type': ['null', 'string']},
                {'name': 'device_type', 'type': 'string'},
                {'name': 'metadata', 'type': 'map', 'values': 'string'},
                {'name': 'ingestion_time', 'type': 'string'}
            ]
        }
    },
    'marketplace_sales': {
        'key_schema': {
            'type': 'string',
            'description': 'Transaction ID as partition key'
        },
        'value_schema': {
            'type': 'record',
            'name': 'MarketplaceSale',
            'fields': [
                {'name': 'transaction_id', 'type': 'string'},
                {'name': 'marketplace_name', 'type': 'string'},
                {'name': 'seller_id', 'type': 'string'},
                {'name': 'product_id', 'type': 'string'},
                {'name': 'amount', 'type': 'double'},
                {'name': 'currency', 'type': 'string'},
                {'name': 'quantity', 'type': 'int'},
                {'name': 'transaction_time', 'type': 'string'},
                {'name': 'settlement_time', 'type': 'string'},
                {'name': 'payment_method', 'type': 'string'},
                {'name': 'status', 'type': 'string'},
                {'name': 'marketplace_metadata', 'type': 'map', 'values': 'string'},
                {'name': 'ingestion_time', 'type': 'string'}
            ]
        }
    }
}

# Convenience functions
def get_kafka_config(environment: str = "local") -> KafkaConfig:
    """Get Kafka configuration instance"""
    return KafkaConfig(environment)

def get_producer_config(producer_type: str = "default") -> Dict[str, Any]:
    """Get producer configuration"""
    config = KafkaConfig()
    return config.get_producer_config(producer_type)

def get_consumer_config(group_id: str, consumer_type: str = "default") -> Dict[str, Any]:
    """Get consumer configuration"""
    config = KafkaConfig()
    return config.get_consumer_config(group_id, consumer_type)