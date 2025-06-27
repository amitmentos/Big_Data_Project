 # processing/config/iceberg_config.py

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging

logger = logging.getLogger(__name__)

class IcebergConfigManager:
    """Configuration and utilities for Apache Iceberg tables"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.warehouse_path = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/")
        self.catalog_name = "spark_catalog"
    
    def get_table_properties(self, layer: str = "bronze") -> Dict[str, str]:
        """Get optimized table properties based on data layer"""
        
        base_properties = {
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'snappy',
            'write.metadata.compression-codec': 'gzip',
            'commit.retry.num-retries': '3',
            'commit.retry.min-wait-ms': '100',
            'commit.retry.max-wait-ms': '60000'
        }
        
        if layer == "bronze":
            # Bronze layer: optimized for high-volume ingestion
            base_properties.update({
                'write.target-file-size-bytes': '134217728',  # 128MB
                'write.parquet.row-group-size-bytes': '134217728',
                'write.parquet.page-size-bytes': '1048576',  # 1MB
                'history.expire.min-snapshots-to-keep': '10',
                'history.expire.max-snapshot-age-ms': str(7 * 24 * 60 * 60 * 1000)  # 7 days
            })
        
        elif layer == "silver":
            # Silver layer: balanced for processing and queries
            base_properties.update({
                'write.target-file-size-bytes': '268435456',  # 256MB
                'write.parquet.row-group-size-bytes': '134217728',
                'write.parquet.page-size-bytes': '1048576',
                'history.expire.min-snapshots-to-keep': '15',
                'history.expire.max-snapshot-age-ms': str(14 * 24 * 60 * 60 * 1000)  # 14 days
            })
        
        elif layer == "gold":
            # Gold layer: optimized for analytical queries
            base_properties.update({
                'write.target-file-size-bytes': '536870912',  # 512MB
                'write.parquet.row-group-size-bytes': '268435456',
                'write.parquet.page-size-bytes': '2097152',  # 2MB
                'history.expire.min-snapshots-to-keep': '30',
                'history.expire.max-snapshot-age-ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
                'read.parquet.vectorization.enabled': 'true'
            })
        
        return base_properties
    
    def create_database(self, database_name: str) -> bool:
        """Create database if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            logger.info(f"Created/verified database: {database_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating database {database_name}: {str(e)}")
            return False
    
    def create_table(self, 
                     table_name: str,
                     schema: StructType,
                     partition_cols: Optional[List[str]] = None,
                     layer: str = "bronze",
                     table_comment: Optional[str] = None) -> bool:
        """Create Iceberg table with optimized properties"""
        
        try:
            # Generate DDL
            ddl_parts = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]
            
            # Add columns
            column_definitions = []
            for field in schema.fields:
                col_def = f"{field.name} {field.dataType.simpleString()}"
                if not field.nullable:
                    col_def += " NOT NULL"
                if field.metadata.get('comment'):
                    col_def += f" COMMENT '{field.metadata['comment']}'"
                column_definitions.append(col_def)
            
            ddl_parts.append(",\n  ".join(column_definitions))
            ddl_parts.append(") USING ICEBERG")
            
            # Add partitioning
            if partition_cols:
                partition_specs = []
                for col in partition_cols:
                    # Use appropriate partitioning strategy based on column type
                    if any(word in col.lower() for word in ['date', 'time']):
                        if 'date' in col.lower():
                            partition_specs.append(f"days({col})")
                        else:
                            partition_specs.append(f"hours({col})")
                    else:
                        partition_specs.append(col)
                
                ddl_parts.append(f"PARTITIONED BY ({', '.join(partition_specs)})")
            
            # Add table properties
            properties = self.get_table_properties(layer)
            if table_comment:
                properties['comment'] = table_comment
            
            if properties:
                prop_strings = [f"'{k}' = '{v}'" for k, v in properties.items()]
                ddl_parts.append(f"TBLPROPERTIES ({', '.join(prop_strings)})")
            
            ddl = "\n".join(ddl_parts)
            
            # Execute DDL
            self.spark.sql(ddl)
            logger.info(f"Created Iceberg table: {table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            return False
    
    def optimize_table(self, table_name: str, 
                      strategy: str = "rewrite_data_files") -> bool:
        """Optimize Iceberg table using various strategies"""
        
        try:
            if strategy == "rewrite_data_files":
                # Compact small files
                self.spark.sql(f"""
                    CALL {self.catalog_name}.system.rewrite_data_files(
                        table => '{table_name}',
                        strategy => 'binpack',
                        options => map('min-input-files', '10')
                    )
                """)
                
            elif strategy == "rewrite_manifests":
                # Optimize manifest files
                self.spark.sql(f"""
                    CALL {self.catalog_name}.system.rewrite_manifests('{table_name}')
                """)
                
            elif strategy == "expire_snapshots":
                # Clean up old snapshots
                retention_days = 7
                older_than = datetime.now() - timedelta(days=retention_days)
                self.spark.sql(f"""
                    CALL {self.catalog_name}.system.expire_snapshots(
                        table => '{table_name}',
                        older_than => TIMESTAMP '{older_than.strftime('%Y-%m-%d %H:%M:%S')}'
                    )
                """)
                
            elif strategy == "remove_orphan_files":
                # Remove orphaned files
                self.spark.sql(f"""
                    CALL {self.catalog_name}.system.remove_orphan_files(
                        table => '{table_name}',
                        older_than => TIMESTAMP '{(datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')}'
                    )
                """)
            
            logger.info(f"Optimized table {table_name} using strategy: {strategy}")
            return True
            
        except Exception as e:
            logger.error(f"Error optimizing table {table_name} with {strategy}: {str(e)}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive table information"""
        
        try:
            info = {}
            
            # Basic table info
            describe_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
            table_details = {row['col_name']: row['data_type'] 
                           for row in describe_df.collect() 
                           if row['col_name'] and row['data_type']}
            info['table_details'] = table_details
            
            # Table size and row count
            try:
                size_info = self.spark.sql(f"SHOW TABLE EXTENDED LIKE '{table_name}'").collect()[0]
                info['size_info'] = size_info.asDict()
            except:
                info['size_info'] = {}
            
            # Snapshot information
            try:
                snapshots = self.spark.sql(f"SELECT * FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 5")
                info['recent_snapshots'] = [row.asDict() for row in snapshots.collect()]
            except:
                info['recent_snapshots'] = []
            
            # Partition information
            try:
                partitions = self.spark.sql(f"SHOW PARTITIONS {table_name}")
                info['partition_count'] = partitions.count()
            except:
                info['partition_count'] = 0
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return {}
    
    def analyze_table_health(self, table_name: str) -> Dict[str, Any]:
        """Analyze table health and provide optimization recommendations"""
        
        try:
            health_report = {
                'table_name': table_name,
                'timestamp': datetime.now().isoformat(),
                'health_score': 100,
                'issues': [],
                'recommendations': []
            }
            
            # Get table information
            table_info = self.get_table_info(table_name)
            
            # Check for small files
            try:
                files_df = self.spark.sql(f"SELECT * FROM {table_name}.files")
                file_sizes = [row['file_size_in_bytes'] for row in files_df.collect()]
                
                if file_sizes:
                    avg_file_size = sum(file_sizes) / len(file_sizes)
                    small_files = [size for size in file_sizes if size < 64 * 1024 * 1024]  # < 64MB
                    
                    if len(small_files) > len(file_sizes) * 0.3:  # More than 30% small files
                        health_report['health_score'] -= 20
                        health_report['issues'].append(f"High number of small files: {len(small_files)}/{len(file_sizes)}")
                        health_report['recommendations'].append("Run file compaction using rewrite_data_files")
                    
                    health_report['file_stats'] = {
                        'total_files': len(file_sizes),
                        'avg_file_size_mb': avg_file_size / (1024 * 1024),
                        'small_files_count': len(small_files)
                    }
            except Exception as e:
                logger.warning(f"Could not analyze files for {table_name}: {str(e)}")
            
            # Check snapshot count
            snapshots = table_info.get('recent_snapshots', [])
            if len(snapshots) > 50:
                health_report['health_score'] -= 10
                health_report['issues'].append(f"High number of snapshots: {len(snapshots)}")
                health_report['recommendations'].append("Clean up old snapshots using expire_snapshots")
            
            # Check partition health
            partition_count = table_info.get('partition_count', 0)
            if partition_count > 1000:
                health_report['health_score'] -= 15
                health_report['issues'].append(f"High number of partitions: {partition_count}")
                health_report['recommendations'].append("Review partitioning strategy")
            
            # Overall health assessment
            if health_report['health_score'] >= 90:
                health_report['status'] = 'EXCELLENT'
            elif health_report['health_score'] >= 70:
                health_report['status'] = 'GOOD'
            elif health_report['health_score'] >= 50:
                health_report['status'] = 'FAIR'
            else:
                health_report['status'] = 'POOR'
            
            return health_report
            
        except Exception as e:
            logger.error(f"Error analyzing table health for {table_name}: {str(e)}")
            return {'error': str(e)}
    
    def backup_table_metadata(self, table_name: str, backup_location: str) -> bool:
        """Backup table metadata for disaster recovery"""
        
        try:
            # Export table metadata
            metadata_path = f"{backup_location}/metadata/{table_name.replace('.', '_')}"
            
            # Get table schema
            schema_df = self.spark.sql(f"DESCRIBE {table_name}")
            schema_df.coalesce(1).write.mode("overwrite").json(f"{metadata_path}/schema")
            
            # Get table properties
            extended_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
            extended_df.coalesce(1).write.mode("overwrite").json(f"{metadata_path}/properties")
            
            # Get recent snapshots
            try:
                snapshots_df = self.spark.sql(f"SELECT * FROM {table_name}.snapshots")
                snapshots_df.coalesce(1).write.mode("overwrite").json(f"{metadata_path}/snapshots")
            except:
                logger.warning(f"Could not backup snapshots for {table_name}")
            
            logger.info(f"Backed up metadata for {table_name} to {metadata_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error backing up metadata for {table_name}: {str(e)}")
            return False
    
    def time_travel_query(self, table_name: str, 
                         timestamp: Optional[str] = None,
                         snapshot_id: Optional[str] = None) -> DataFrame:
        """Perform time travel query on Iceberg table"""
        
        try:
            if timestamp:
                return self.spark.sql(f"""
                    SELECT * FROM {table_name} 
                    FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}'
                """)
            elif snapshot_id:
                return self.spark.sql(f"""
                    SELECT * FROM {table_name} 
                    FOR SYSTEM_VERSION AS OF {snapshot_id}
                """)
            else:
                raise ValueError("Either timestamp or snapshot_id must be provided")
                
        except Exception as e:
            logger.error(f"Error performing time travel query on {table_name}: {str(e)}")
            raise
    
    def rollback_table(self, table_name: str, 
                      timestamp: Optional[str] = None,
                      snapshot_id: Optional[str] = None) -> bool:
        """Rollback table to a previous state"""
        
        try:
            if timestamp:
                self.spark.sql(f"""
                    CALL {self.catalog_name}.system.rollback_to_timestamp(
                        table => '{table_name}',
                        timestamp => TIMESTAMP '{timestamp}'
                    )
                """)
            elif snapshot_id:
                self.spark.sql(f"""
                    CALL {self.catalog_name}.system.rollback_to_snapshot(
                        table => '{table_name}',
                        snapshot_id => {snapshot_id}
                    )
                """)
            else:
                raise ValueError("Either timestamp or snapshot_id must be provided")
            
            logger.info(f"Rolled back table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error rolling back table {table_name}: {str(e)}")
            return False


class IcebergMaintenanceScheduler:
    """Automated maintenance scheduler for Iceberg tables"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.iceberg_config = IcebergConfigManager(spark)
    
    def run_daily_maintenance(self, tables: List[str]) -> Dict[str, bool]:
        """Run daily maintenance tasks on specified tables"""
        
        results = {}
        
        for table in tables:
            logger.info(f"Running daily maintenance for {table}")
            
            try:
                # Expire old snapshots (keep last 7 days)
                success1 = self.iceberg_config.optimize_table(table, "expire_snapshots")
                
                # Rewrite manifest files
                success2 = self.iceberg_config.optimize_table(table, "rewrite_manifests")
                
                results[table] = success1 and success2
                
            except Exception as e:
                logger.error(f"Daily maintenance failed for {table}: {str(e)}")
                results[table] = False
        
        return results
    
    def run_weekly_maintenance(self, tables: List[str]) -> Dict[str, bool]:
        """Run weekly maintenance tasks on specified tables"""
        
        results = {}
        
        for table in tables:
            logger.info(f"Running weekly maintenance for {table}")
            
            try:
                # File compaction
                success1 = self.iceberg_config.optimize_table(table, "rewrite_data_files")
                
                # Remove orphan files
                success2 = self.iceberg_config.optimize_table(table, "remove_orphan_files")
                
                # Health check
                health = self.iceberg_config.analyze_table_health(table)
                logger.info(f"Table {table} health score: {health.get('health_score', 'N/A')}")
                
                results[table] = success1 and success2
                
            except Exception as e:
                logger.error(f"Weekly maintenance failed for {table}: {str(e)}")
                results[table] = False
        
        return results


# Convenience functions
def get_iceberg_manager(spark: SparkSession) -> IcebergConfigManager:
    """Get configured Iceberg manager"""
    return IcebergConfigManager(spark)

def create_bronze_table(spark: SparkSession, table_name: str, 
                       schema: StructType, partition_cols: List[str] = None) -> bool:
    """Convenience function to create Bronze layer table"""
    manager = IcebergConfigManager(spark)
    return manager.create_table(table_name, schema, partition_cols, "bronze")

def create_silver_table(spark: SparkSession, table_name: str,
                       schema: StructType, partition_cols: List[str] = None) -> bool:
    """Convenience function to create Silver layer table"""
    manager = IcebergConfigManager(spark)
    return manager.create_table(table_name, schema, partition_cols, "silver")

def create_gold_table(spark: SparkSession, table_name: str,
                     schema: StructType, partition_cols: List[str] = None) -> bool:
    """Convenience function to create Gold layer table"""
    manager = IcebergConfigManager(spark)
    return manager.create_table(table_name, schema, partition_cols, "gold")