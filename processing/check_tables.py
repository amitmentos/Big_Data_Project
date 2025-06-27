#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder \
        .appName('Check Gold Tables') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
        .config('spark.sql.catalog.spark_catalog.type', 'hive') \
        .config('spark.sql.catalog.spark_catalog.uri', 'thrift://hive-metastore:9083') \
        .config('spark.sql.catalog.spark_catalog.warehouse', 's3a://warehouse/') \
        .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000') \
        .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
        .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \
        .getOrCreate()

    tables_to_check = [
        'gold.fact_sales',
        'gold.fact_user_activity', 
        'silver.dim_customer_scd',
        'silver.dim_product'
    ]

    for table in tables_to_check:
        try:
            count = spark.sql(f'SELECT COUNT(*) as count FROM {table}').collect()[0]['count']
            print(f'{table}: {count} records')
            
            # Show a sample of the data
            if count > 0:
                sample = spark.sql(f'SELECT * FROM {table} LIMIT 3')
                print(f'Sample from {table}:')
                sample.show(truncate=False)
                print()
        except Exception as e:
            print(f'{table}: ERROR - {e}')

    spark.stop()

if __name__ == "__main__":
    main()
