# processing/config/spark_config.py

import os
from pyspark.sql import SparkSession
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class SparkConfigManager:
    """Centralized Spark configuration management for the data platform"""
    
    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """Load environment-specific Spark configurations"""
        
        base_config = {
            # Core Spark Settings
            "spark.app.name": "ECommerce-Data-Platform",
            "spark.master": self._get_spark_master(),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            
            # Memory and Resource Management
            "spark.executor.memory": self._get_executor_memory(),
            "spark.executor.cores": self._get_executor_cores(),
            "spark.executor.instances": self._get_executor_instances(),
            "spark.driver.memory": self._get_driver_memory(),
            "spark.driver.cores": "2",
            "spark.driver.maxResultSize": "2g",
            
            # Iceberg Configuration
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.type": "hadoop",
            "spark.sql.catalog.spark_catalog.warehouse": self._get_warehouse_path(),
            "spark.sql.catalog.spark_catalog.s3.endpoint": self._get_s3_endpoint(),
            
            # S3/MinIO Configuration
            "spark.hadoop.fs.s3a.access.key": self._get_s3_access_key(),
            "spark.hadoop.fs.s3a.secret.key": self._get_s3_secret_key(),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.multipart.size": "104857600",  # 100MB
            "spark.hadoop.fs.s3a.connection.maximum": "100",
            
            # Streaming Configuration
            "spark.sql.streaming.checkpointLocation": f"{self._get_warehouse_path()}/checkpoints/",
            "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
            
            # Performance Tuning
            "spark.sql.files.maxPartitionBytes": "268435456",  # 256MB
            "spark.sql.shuffle.partitions": self._get_shuffle_partitions(),
            "spark.default.parallelism": self._get_default_parallelism(),
            
            # Compression and Storage
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.parquet.enableVectorizedReader": "true",
            "spark.sql.parquet.columnarReaderBatchSize": "4096",
            
            # Dynamic Allocation (for production)
            "spark.dynamicAllocation.enabled": self._get_dynamic_allocation_enabled(),
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": self._get_max_executors(),
            "spark.dynamicAllocation.initialExecutors": "2",
            
            # Logging and Monitoring
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.serializer.objectStreamReset": "100",
            "spark.cleaner.periodicGC.interval": "5min",
        }
        
        # Environment-specific overrides
        env_overrides = self._get_environment_overrides()
        base_config.update(env_overrides)
        
        return base_config
    
    def _get_environment_overrides(self) -> Dict:
        """Get environment-specific configuration overrides"""
        if self.environment == "production":
            return {
                "spark.executor.memory": "4g",
                "spark.executor.instances": "4",
                "spark.sql.shuffle.partitions": "200",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": "10",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB"
            }
        elif self.environment == "testing":
            return {
                "spark.executor.memory": "1g",
                "spark.executor.instances": "1",
                "spark.sql.shuffle.partitions": "10",
                "spark.dynamicAllocation.enabled": "false"
            }
        else:  # development
            return {
                "spark.executor.memory": "2g",
                "spark.executor.instances": "2",
                "spark.sql.shuffle.partitions": "50",
                "spark.dynamicAllocation.enabled": "false"
            }
    
    def _get_spark_master(self) -> str:
        return os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    
    def _get_warehouse_path(self) -> str:
        return os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/")
    
    def _get_s3_endpoint(self) -> str:
        return os.getenv("S3_ENDPOINT", "http://minio:9000")
    
    def _get_s3_access_key(self) -> str:
        return os.getenv("AWS_ACCESS_KEY_ID", "minio")
    
    def _get_s3_secret_key(self) -> str:
        return os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
    
    def _get_executor_memory(self) -> str:
        return os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    
    def _get_executor_cores(self) -> str:
        return os.getenv("SPARK_EXECUTOR_CORES", "2")
    
    def _get_executor_instances(self) -> str:
        return os.getenv("SPARK_EXECUTOR_INSTANCES", "2")
    
    def _get_driver_memory(self) -> str:
        return os.getenv("SPARK_DRIVER_MEMORY", "1g")
    
    def _get_shuffle_partitions(self) -> str:
        base_partitions = {
            "development": "50",
            "testing": "10", 
            "production": "200"
        }
        return os.getenv("SPARK_SHUFFLE_PARTITIONS", 
                        base_partitions.get(self.environment, "50"))
    
    def _get_default_parallelism(self) -> str:
        base_parallelism = {
            "development": "4",
            "testing": "2",
            "production": "8"
        }
        return os.getenv("SPARK_DEFAULT_PARALLELISM",
                        base_parallelism.get(self.environment, "4"))
    
    def _get_dynamic_allocation_enabled(self) -> str:
        return "true" if self.environment == "production" else "false"
    
    def _get_max_executors(self) -> str:
        max_executors = {
            "development": "4",
            "testing": "2",
            "production": "10"
        }
        return max_executors.get(self.environment, "4")
    
    def create_spark_session(self, app_name: Optional[str] = None) -> SparkSession:
        """Create optimized Spark session with all configurations"""
        
        if app_name:
            self.config["spark.app.name"] = app_name
        
        builder = SparkSession.builder
        
        # Apply all configurations
        for key, value in self.config.items():
            builder = builder.config(key, value)
        
        # Enable Hive support if needed
        builder = builder.enableHiveSupport()
        
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        # Log configuration summary
        logger.info(f"Created Spark session for {self.environment} environment")
        logger.info(f"Master: {self.config['spark.master']}")
        logger.info(f"Executor memory: {self.config['spark.executor.memory']}")
        logger.info(f"Executor instances: {self.config['spark.executor.instances']}")
        
        return spark
    
    def get_streaming_config(self) -> Dict:
        """Get configuration specific to Spark Streaming"""
        return {
            "spark.sql.streaming.checkpointLocation": f"{self._get_warehouse_path()}/checkpoints/",
            "spark.sql.streaming.stateStore.maintenanceInterval": "600s",
            "spark.sql.streaming.stopGracefullyOnShutdown": "true",
            "spark.sql.streaming.metricsEnabled": "true"
        }
    
    def get_kafka_config(self) -> Dict:
        """Get Kafka-specific configuration for Spark Streaming"""
        return {
            "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": "10000",
            "startingOffsets": "latest",
            "kafka.session.timeout.ms": "30000",
            "kafka.request.timeout.ms": "40000",
            "kafka.max.poll.records": "1000"
        }
    
    def optimize_for_workload(self, workload_type: str) -> Dict:
        """Get optimized configuration for specific workload types"""
        
        optimizations = {}
        
        if workload_type == "batch_etl":
            optimizations.update({
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
                "spark.sql.shuffle.partitions": "200"
            })
        
        elif workload_type == "streaming":
            optimizations.update({
                "spark.sql.streaming.continuous.executorQueueSize": "1024",
                "spark.sql.streaming.continuous.executorPollIntervalMs": "10",
                "spark.streaming.backpressure.enabled": "true",
                "spark.streaming.kafka.maxRatePerPartition": "1000"
            })
        
        elif workload_type == "ml_feature_engineering":
            optimizations.update({
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            })
        
        elif workload_type == "data_quality":
            optimizations.update({
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.statistics.histogram.enabled": "true",
                "spark.sql.cbo.enabled": "true",
                "spark.sql.cbo.joinReorder.enabled": "true"
            })
        
        return optimizations
    
    def create_optimized_session(self, app_name: str, workload_type: str) -> SparkSession:
        """Create Spark session optimized for specific workload"""
        
        # Start with base config
        config = self.config.copy()
        
        # Apply workload optimizations
        workload_config = self.optimize_for_workload(workload_type)
        config.update(workload_config)
        
        # Create session
        builder = SparkSession.builder.appName(app_name)
        
        for key, value in config.items():
            builder = builder.config(key, value)
        
        spark = builder.enableHiveSupport().getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Created optimized Spark session for {workload_type} workload")
        
        return spark
    
    def validate_configuration(self) -> bool:
        """Validate Spark configuration"""
        try:
            # Test creating a minimal Spark session
            test_session = SparkSession.builder \
                .appName("ConfigValidation") \
                .config("spark.master", "local[1]") \
                .getOrCreate()
            
            # Test basic operations
            test_df = test_session.range(10)
            count = test_df.count()
            
            test_session.stop()
            
            logger.info("Spark configuration validation passed")
            return count == 10
            
        except Exception as e:
            logger.error(f"Spark configuration validation failed: {str(e)}")
            return False


# Convenience functions for easy usage
def get_spark_session(app_name: str = "DataPlatform", 
                     environment: str = "development") -> SparkSession:
    """Convenience function to get configured Spark session"""
    config_manager = SparkConfigManager(environment)
    return config_manager.create_spark_session(app_name)

def get_streaming_spark_session(app_name: str = "StreamingApp",
                              environment: str = "development") -> SparkSession:
    """Convenience function to get Spark session optimized for streaming"""
    config_manager = SparkConfigManager(environment)
    return config_manager.create_optimized_session(app_name, "streaming")

def get_batch_spark_session(app_name: str = "BatchETL",
                           environment: str = "development") -> SparkSession:
    """Convenience function to get Spark session optimized for batch ETL"""
    config_manager = SparkConfigManager(environment)
    return config_manager.create_optimized_session(app_name, "batch_etl")