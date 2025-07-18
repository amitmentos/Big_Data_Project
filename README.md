# E-commerce Data Platform - Complete Implementation

## Overview

This project implements a complete end-to-end data engineering solution for an e-commerce retail platform. It demonstrates modern big data technologies including Apache Spark, Kafka, MinIO (S3-compatible storage), and Airflow in a production-ready architecture following the Medallion pattern (Bronze → Silver → Gold).

### Business Context
- **Domain**: Multi-channel e-commerce platform (website, mobile app, marketplace partnerships)
- **Challenge**: Optimize marketing spend across channels based on customer segments
- **ML Use Case**: Product recommendation engine for personalized customer experiences
- **Data Quality**: Hybrid validation with custom checks and Great Expectations framework

## Architecture

### Technology Stack
| Component | Technology | Purpose |
|-----------|------------|---------|
| **Storage** | MinIO | S3-compatible object storage |
| **Processing** | Apache Spark | Batch and streaming data processing |
| **Streaming** | Apache Kafka | Real-time data ingestion and delay monitoring |
| **Orchestration** | Apache Airflow | Workflow scheduling and monitoring |
| **Containerization** | Docker Compose | Service orchestration |
| **Data Quality** | Great Expectations | Data validation and quality assurance |

### Data Architecture

```
Data Sources → Kafka → Bronze Layer → Silver Layer → Gold Layer → Analytics/ML
     ↓            ↓         ↓           ↓            ↓
  Streaming    Raw Data   Cleaned    Enriched    Business
   Events      Landing    & Valid   & Historic   Ready
```

## Project Structure

```
ecommerce-data-platform/
├── README.md                          # This file
├── PROJECT_SUMMARY.md                 # Project overview and architecture
├── KAFKA_DELAY_IMPLEMENTATION.md      # Kafka delay monitoring details
├── GREAT_EXPECTATIONS_INTEGRATION.md  # Data quality framework details
├── docker-compose.yml                 # Main orchestration
├── start_platform_demo.sh             # Main startup script
├── run_complete_demo.py               # Complete demo orchestrator
├── create_sample_data.py              # Sample data generation
├── visualize_tables.py                # Data visualization
├── orchestration/                     # Airflow components
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── dags/
│   │   ├── bronze_to_silver_dag.py
│   │   ├── silver_to_gold_dag.py
│   │   ├── ml_feature_generation_dag.py
│   │   ├── data_quality_dag.py
│   │   └── great_expectations_dag.py
├── streaming/                         # Kafka and producers
│   ├── docker-compose.yml
│   ├── kafka_delay_monitor.py         # Kafka delay monitoring
│   ├── producers/
│   ├── consumers/
│   └── kafka/
├── processing/                        # Spark applications
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── spark-apps/
│   ├── great_expectations/            # Great Expectations configuration
│   └── utils/
├── sample_data/                       # Sample datasets
│   ├── campaigns.json
│   ├── customers.json
│   ├── marketplace_sales.json
│   ├── products.json
│   └── user_events.json
├── storage/                           # Data storage
│   ├── data/
│   └── minio/
└── visualizations/                    # Generated visualizations
    ├── campaign_effectiveness_analysis.png
    ├── customer_segmentation_analysis.png
    ├── product_performance_analysis.png
    ├── sales_performance_dashboard.png
    ├── user_activity_analysis.png
    └── interactive_dashboard.html
```

## Prerequisites

### System Requirements
- **Docker**: Version 20.10 or higher
- **Docker Compose**: Version 2.0 or higher
- **Memory**: Minimum 16GB
- **Disk Space**: Minimum 16GB free space
- **Operating System**: Linux, macOS
- **CPU**: M2 or higher

### Installation
1. **Install Docker Desktop**
   - Download from [docker.com](https://www.docker.com/products/docker-desktop)
   - Ensure Docker Compose is included

2. **Verify Installation**
   ```bash
   docker --version
   docker-compose --version
   ```

## Quick Start

### 1. Setup and Start Platform
```bash
# Clone the repository
git clone <repository-url>
cd ecommerce-data-platform

# Run setup script
./setup.sh

# Start the platform
./start_platform_demo.sh
```

The `start_platform_demo.sh` script performs the following steps:
1. Starts all Docker services
2. Waits for services to be ready
3. Generates sample data
4. Sets up the Kafka delay monitoring
5. Runs the platform demo

### 2. Run the Complete Demo
```bash
# Run the complete demo to see all platform capabilities
python run_complete_demo.py
```

This demonstrates:
1. Data ingestion into Bronze layer
2. Transformation to Silver layer
3. Aggregation to Gold layer
4. Data quality validation
5. ML feature generation
6. Visualization examples

## Service Access Points

Once all services are running, access them via:

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | admin/admin |
| **Spark Master UI** | http://localhost:8081 | - |
| **MinIO Console** | http://localhost:9001 | minio/minio123 |

## Data Architecture

### Data Flow Overview

```
Data Sources → Kafka → Bronze Layer → Silver Layer → Gold Layer → Analytics/ML
     ↓            ↓         ↓           ↓            ↓
  Streaming    Raw Data   Cleaned    Enriched    Business
   Events      Landing    & Valid   & Historic   Ready
```

### Medallion Architecture Details

#### 1. Bronze Layer (Raw Data Landing)
- **Purpose**: Store raw data exactly as received
- **Implementation**: 
  - Minimal transformations (parsing, formatting)
  - Preserves original data with added metadata
  - Combines streaming and batch data sources
- **Tables**:
  - `raw_user_events` - Streaming user activity from Kafka
  - `raw_marketplace_sales` - Batch sales data (late arrivals up to 48h)
  - `raw_product_catalog` - Product information
  - `raw_customer_data` - Customer profiles
  - `raw_marketing_campaigns` - Campaign definitions

#### 2. Silver Layer (Standardized & Enriched)
- **Purpose**: Clean, validate, and standardize data
- **Implementation**:
  - Implements data quality checks
  - Standardizes schema and data types
  - Applies basic transformations and enrichments
- **Tables**:
  - `standardized_user_events` - Cleaned user activity
  - `standardized_sales` - Validated sales transactions
  - `dim_customer` - Customer dimension
  - `dim_product` - Product dimension
  - `dim_marketing_campaign` - Marketing campaigns
  - `dim_date` - date format and info

#### 3. Gold Layer (Business Ready)
- **Purpose**: Create business-ready aggregations and features
- **Implementation**:
  - Builds domain-specific views and metrics
  - Creates aggregations for business intelligence
  - Generates features for machine learning
- **Tables**:
  - `fact_sales` - Sales fact table
  - `fact_user_activity` - User activity fact table
  - `sales_performance_metrics` - Channel performance metrics
  - `customer_segmentation` - Customer segments
  - `campaign_effectiveness` - Marketing ROI analysis
  - `customer_product_interactions` - Machine learning feature tables

## Data Processing Components

### 1. Streaming Pipeline
- **Implementation**: Kafka + Spark Streaming
- **Features**:
  - Real-time data ingestion from various sources
  - Event-time processing with watermarking
  - Kafka delay monitoring for SLA tracking
  - Late data handling (up to 48 hours)
  - Continuous processing to Bronze and Silver layers

### 2. Batch Processing
- **Implementation**: Airflow + Spark
- **Features**:
  - Scheduled ETL workflows
  - Incremental and full processing modes
  - Complex transformations for Silver and Gold layers
  - Dependency management between processing stages
  - Data quality validation integration

### 3. ML Feature Engineering
- **Implementation**: Airflow DAG + Spark
- **Features**:
  - Automated feature extraction from Silver and Gold layers
  - Customer behavior feature generation
  - Product performance metrics
  - Time-based features for recommendation
  - Feature versioning and lineage tracking

## Data Quality Framework

The platform implements a hybrid data quality approach:

### 1. Custom Quality Checks
- Integrated into processing pipelines
- Schema validation and enforcement
- Business rule validation
- Completeness and consistency checks
- Automated monitoring and alerting

### 2. Great Expectations Integration
- Declarative data quality definitions
- Expectations defined for each data asset
- Scheduled validation with Airflow
- Detailed quality documentation
- Validation results and reporting
- See [GREAT_EXPECTATIONS_INTEGRATION.md](./GREAT_EXPECTATIONS_INTEGRATION.md) for details

## Key Features

### 1. Kafka Delay Monitoring
- Real-time tracking of message delivery delays
- SLA monitoring for streaming data
- Configurable alerting thresholds
- Custom implementation for production monitoring
- See [KAFKA_DELAY_IMPLEMENTATION.md](./KAFKA_DELAY_IMPLEMENTATION.md) for details

### 2. Business Analytics
- Marketing channel optimization
- Customer segmentation analysis
- Campaign effectiveness tracking
- Sales performance metrics
- User activity patterns

### 3. ML Capabilities
- Customer-product interaction features
- Behavioral features (recency, frequency, monetary)
- Category affinity calculations
- Feature preparation for recommendation engine
- ML feature versioning

## Component Design Details

### Apache Kafka
- Message broker for real-time data streams
- Topics organized by data domain
- Custom delay monitoring implementation
- Producer and consumer implementations

### Apache Spark
- Distributed data processing engine
- Supports both batch and streaming workloads
- Executes transformations between data layers
- Preconfigured for optimal performance

### Apache Airflow
- Workflow orchestration platform
- DAGs for each major processing pipeline:
  - Bronze to Silver ETL
  - Silver to Gold ETL
  - ML Feature Generation
  - Data Quality Monitoring
  - Great Expectations Validation

### MinIO
- S3-compatible object storage
- Stores data in all processing stages
- Organized by data layer (Bronze/Silver/Gold)
- Accessible via S3 API

### Docker & Docker Compose
- Container orchestration for all components
- Isolated environments for each service
- Simplified deployment and configuration
- Organized by functional area

## Troubleshooting
### Common Issues

#### 1. Services Won't Start
```bash
# Check Docker resource allocation (memory, CPU)
docker stats

# Check logs for specific services
docker-compose logs -f <service_name>

# Restart specific service
docker-compose restart <service_name>
```

#### 2. Airflow DAGs Not Running
```bash
# Check Airflow logs
ls -la orchestration/logs/dag_id=*/

# Restart Airflow services
docker-compose -f orchestration/docker-compose.yml restart
```

#### 3. Kafka Connection Issues
```bash
# Check Kafka logs
ls -la logs/kafka/

# Verify Kafka service is running
docker-compose -f streaming/docker-compose.yml ps
```

#### 4. Spark Jobs Failing
```bash
# Check Spark logs
ls -la logs/spark/

# Access Spark UI for job details
open http://localhost:8081
```

#### 5. Data Not Appearing in Layers
```bash
# Run the complete demo again to verify data flow
python run_complete_demo.py

# Check specific layer processing
python silver_transformations.py
python gold_aggregations.py
```

## Running Specific Components

### Data Generation
```bash
# Generate sample data
python create_sample_data.py

# Generate current data
python generate_current_data.py
```

### Data Processing
```bash
# Run Silver layer transformations
python silver_transformations.py

# Run Gold layer aggregations
python gold_aggregations.py
```

### Data Visualization
```bash
# Generate visualizations
python visualize_tables.py
```

### Great Expectations Demo
```bash
# Run data quality validation
python run_great_expectations_demo.py
```

## Platform Startup Flow

The `start_platform_demo.sh` script orchestrates the platform startup:

1. **Service Initialization**
   - Starts all Docker services using multiple docker-compose files
   - Sets up networking between components
   - Initializes storage volumes

2. **Service Readiness Check**
   - Waits for Kafka to be ready
   - Waits for Spark to be ready
   - Waits for Airflow to be ready
   - Waits for MinIO to be ready

3. **Data Preparation**
   - Generates sample data using `create_sample_data.py`
   - Sets up Kafka topics and configurations

4. **Demo Preparation**
   - Sets up Kafka delay monitoring
   - Prepares demo environment
   - Initializes logging

## Complete Demo Flow

The `run_complete_demo.py` script demonstrates the full platform capabilities:

1. **Bronze Layer Processing**
   - Ingests raw data from sample sources
   - Streams events through Kafka
   - Performs minimal transformations
   - Stores data in Bronze tables

2. **Silver Layer Processing**
   - Reads data from Bronze layer
   - Applies cleaning and standardization
   - Validates data quality
   - Stores results in Silver tables

3. **Gold Layer Processing**
   - Reads data from Silver layer
   - Creates business aggregations
   - Generates ML features
   - Stores results in Gold tables

4. **Visualization Generation**
   - Creates analytical visualizations
   - Generates performance dashboards
   - Outputs results to visualizations directory

## Production Considerations

### Scaling the Platform
- Increase Spark executors for larger data volumes
- Add Kafka partitions for higher throughput
- Implement distributed MinIO for storage scaling
- Configure Airflow for parallel task execution

### Security Best Practices
- Enable authentication for all services
- Implement encryption for data in transit and at rest
- Use proper network segmentation
- Implement role-based access control

### High Availability
- Deploy multiple instances of each service
- Configure proper replication factors
- Implement automated failover
- Set up comprehensive monitoring and alerting

## Resources and Documentation

For more detailed information about specific components, refer to:

### Core Documentation
- [PROJECT_SUMMARY.md](./PROJECT_SUMMARY.md): Complete project overview and architecture
- [KAFKA_DELAY_IMPLEMENTATION.md](./KAFKA_DELAY_IMPLEMENTATION.md): Kafka delay monitoring details
- [GREAT_EXPECTATIONS_INTEGRATION.md](./GREAT_EXPECTATIONS_INTEGRATION.md): Data quality framework details

### Component Documentation
- [docs/data-model.md](./docs/data-model.md): Detailed data model documentation
- [docs/setup-guide.md](./docs/setup-guide.md): Comprehensive setup instructions
- [docs/user-guide.md](./docs/user-guide.md): Platform usage documentation

## Cleanup and Reset

### Stop Services
```bash
# Stop all services
docker-compose down

# Stop specific component services
docker-compose -f streaming/docker-compose.yml down
docker-compose -f processing/docker-compose.yml down
docker-compose -f orchestration/docker-compose.yml down
```

### Full Reset
```bash
# Complete reset (WARNING: deletes all data)
docker-compose down -v
docker system prune -f
rm -rf logs/* storage/data/* spark-warehouse/*
```

## Next Steps and Future Enhancements

### Planned Improvements
1. **Advanced Analytics**: Enhanced dashboards and reporting
2. **Machine Learning**: Production deployment of recommendation models
3. **Real-time Alerting**: Enhanced monitoring and notification system
4. **Data Lineage**: Implementation of data lineage tracking
5. **Extended Data Quality**: Additional validation rules and checks

---

This implementation provides a solid foundation for a production-ready data engineering platform that can scale with business needs while maintaining data quality and operational excellence.