 # processing/utils/iceberg_utils.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

class IcebergTableManager:
    """Utility class for managing Iceberg tables"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.catalog_name = "spark_catalog"
    
    def create_database(self, database_name: str) -> bool:
        """Create database if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            logger.info(f"Created/verified database: {database_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating database {database_name}: {str(e)}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        try:
            self.spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except:
            return False
    
    def create_table_from_dataframe(self, 
                                   df: DataFrame, 
                                   table_name: str,
                                   partition_cols: List[str] = None,
                                   table_properties: Dict[str, str] = None) -> bool:
        """Create Iceberg table from DataFrame"""
        try:
            # Default table properties
            default_properties = {
                'write.format.default': 'parquet',
                'write.parquet.compression-codec': 'snappy',
                'write.target-file-size-bytes': '268435456',  # 256MB
                'commit.retry.num-retries': '3'
            }
            
            if table_properties:
                default_properties.update(table_properties)
            
            # Build CREATE TABLE SQL
            writer = df.writeTo(table_name).using("iceberg")
            
            # Add partitioning
            if partition_cols:
                # Transform partition columns for date/time columns
                partition_transforms = []
                for col in partition_cols:
                    if any(keyword in col.lower() for keyword in ['date', 'time']):
                        if 'date' in col.lower():
                            partition_transforms.append(f"days({col})")
                        else:
                            partition_transforms.append(f"hours({col})")
                    else:
                        partition_transforms.append(col)
                
                writer = writer.partitionedBy(*partition_transforms)
            
            # Add table properties
            for key, value in default_properties.items():
                writer = writer.tableProperty(key, value)
            
            # Create table
            writer.create()
            logger.info(f"Created Iceberg table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            return False
    
    def write_to_table(self, 
                      df: DataFrame, 
                      table_name: str,
                      mode: str = "append",
                      replace_where: str = None) -> bool:
        """Write DataFrame to Iceberg table"""
        try:
            writer = df.writeTo(table_name)
            
            if mode == "overwrite":
                if replace_where:
                    writer = writer.overwritePartitions()
                else:
                    writer = writer.overwrite()
            elif mode == "append":
                writer = writer.append()
            else:
                raise ValueError(f"Unsupported mode: {mode}")
            
            writer.commit()
            logger.info(f"Successfully wrote to table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing to table {table_name}: {str(e)}")
            return False
    
    def read_table(self, table_name: str, 
                   as_of_timestamp: str = None,
                   snapshot_id: str = None) -> Optional[DataFrame]:
        """Read from Iceberg table with optional time travel"""
        try:
            if as_of_timestamp:
                return self.spark.read.option("as-of-timestamp", as_of_timestamp).table(table_name)
            elif snapshot_id:
                return self.spark.read.option("snapshot-id", snapshot_id).table(table_name)
            else:
                return self.spark.table(table_name)
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {str(e)}")
            return None
    
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Get table metadata including snapshots, schema, etc."""
        try:
            metadata = {}
            
            # Basic table info
            describe_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
            metadata['description'] = describe_df.collect()
            
            # Schema information
            schema_df = self.spark.sql(f"DESCRIBE TABLE {table_name}")
            metadata['schema'] = [row.asDict() for row in schema_df.collect()]
            
            # Snapshots
            try:
                snapshots_df = self.spark.sql(f"SELECT * FROM {table_name}.snapshots")
                metadata['snapshots'] = [row.asDict() for row in snapshots_df.collect()]
            except:
                metadata['snapshots'] = []
            
            # Table history
            try:
                history_df = self.spark.sql(f"SELECT * FROM {table_name}.history")
                metadata['history'] = [row.asDict() for row in history_df.collect()]
            except:
                metadata['history'] = []
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error getting metadata for {table_name}: {str(e)}")
            return {}
    
    def compact_table(self, table_name: str, 
                     target_file_size: int = 268435456) -> bool:
        """Compact table files"""
        try:
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.rewrite_data_files(
                    table => '{table_name}',
                    strategy => 'binpack',
                    options => map('target-file-size-bytes', '{target_file_size}')
                )
            """)
            logger.info(f"Successfully compacted table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error compacting table {table_name}: {str(e)}")
            return False
    
    def expire_snapshots(self, table_name: str, 
                        retain_last_days: int = 7) -> bool:
        """Remove old snapshots"""
        try:
            older_than = datetime.now() - timedelta(days=retain_last_days)
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.expire_snapshots(
                    table => '{table_name}',
                    older_than => TIMESTAMP '{older_than.strftime('%Y-%m-%d %H:%M:%S')}'
                )
            """)
            logger.info(f"Expired snapshots for table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error expiring snapshots for {table_name}: {str(e)}")
            return False
    
    def remove_orphan_files(self, table_name: str,
                           older_than_days: int = 3) -> bool:
        """Remove orphaned files"""
        try:
            older_than = datetime.now() - timedelta(days=older_than_days)
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.remove_orphan_files(
                    table => '{table_name}',
                    older_than => TIMESTAMP '{older_than.strftime('%Y-%m-%d %H:%M:%S')}'
                )
            """)
            logger.info(f"Removed orphan files for table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error removing orphan files for {table_name}: {str(e)}")
            return False
    
    def rollback_to_snapshot(self, table_name: str, snapshot_id: str) -> bool:
        """Rollback table to specific snapshot"""
        try:
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.rollback_to_snapshot(
                    table => '{table_name}',
                    snapshot_id => {snapshot_id}
                )
            """)
            logger.info(f"Rolled back table {table_name} to snapshot {snapshot_id}")
            return True
        except Exception as e:
            logger.error(f"Error rolling back table {table_name}: {str(e)}")
            return False
    
    def get_table_size(self, table_name: str) -> Dict[str, Any]:
        """Get table size information"""
        try:
            # Get file-level statistics
            files_df = self.spark.sql(f"SELECT * FROM {table_name}.files")
            files_data = files_df.collect()
            
            if files_data:
                total_size = sum(row['file_size_in_bytes'] for row in files_data)
                file_count = len(files_data)
                avg_file_size = total_size / file_count if file_count > 0 else 0
                
                return {
                    'total_size_bytes': total_size,
                    'total_size_mb': round(total_size / (1024 * 1024), 2),
                    'file_count': file_count,
                    'avg_file_size_mb': round(avg_file_size / (1024 * 1024), 2)
                }
            else:
                return {'total_size_bytes': 0, 'file_count': 0}
                
        except Exception as e:
            logger.error(f"Error getting table size for {table_name}: {str(e)}")
            return {}


class IcebergMaintenanceScheduler:
    """Scheduler for automated Iceberg table maintenance"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_manager = IcebergTableManager(spark)
    
    def daily_maintenance(self, table_name: str) -> Dict[str, bool]:
        """Run daily maintenance tasks"""
        results = {}
        
        logger.info(f"Running daily maintenance for {table_name}")
        
        # Expire old snapshots
        results['expire_snapshots'] = self.table_manager.expire_snapshots(table_name)
        
        # Rewrite manifest files
        try:
            self.spark.sql(f"""
                CALL spark_catalog.system.rewrite_manifests('{table_name}')
            """)
            results['rewrite_manifests'] = True
            logger.info(f"Rewrote manifests for {table_name}")
        except Exception as e:
            logger.error(f"Error rewriting manifests for {table_name}: {str(e)}")
            results['rewrite_manifests'] = False
        
        return results
    
    def weekly_maintenance(self, table_name: str) -> Dict[str, bool]:
        """Run weekly maintenance tasks"""
        results = {}
        
        logger.info(f"Running weekly maintenance for {table_name}")
        
        # Compact data files
        results['compact_table'] = self.table_manager.compact_table(table_name)
        
        # Remove orphan files
        results['remove_orphans'] = self.table_manager.remove_orphan_files(table_name)
        
        return results


# Convenience functions
def create_iceberg_session(app_name: str = "IcebergApp",
                          warehouse_path: str = "s3a://warehouse/") -> SparkSession:
    """Create Spark session configured for Iceberg"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", warehouse_path) \
        .config("spark.sql.catalog.spark_catalog.s3.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def optimize_iceberg_table(spark: SparkSession, table_name: str) -> bool:
    """Quick table optimization"""
    manager = IcebergTableManager(spark)
    return manager.compact_table(table_name)

def get_table_health_report(spark: SparkSession, table_name: str) -> Dict[str, Any]:
    """Get comprehensive table health report"""
    manager = IcebergTableManager(spark)
    
    health_report = {
        'table_name': table_name,
        'timestamp': datetime.now().isoformat(),
        'metadata': manager.get_table_metadata(table_name),
        'size_info': manager.get_table_size(table_name),
        'recommendations': []
    }
    
    # Analyze and provide recommendations
    size_info = health_report['size_info']
    if size_info.get('file_count', 0) > 100:
        health_report['recommendations'].append("Consider compacting - high file count")
    
    if size_info.get('avg_file_size_mb', 0) < 64:
        health_report['recommendations'].append("Consider compacting - small average file size")
    
    snapshots = health_report['metadata'].get('snapshots', [])
    if len(snapshots) > 50:
        health_report['recommendations'].append("Consider expiring old snapshots")
    
    return health_report