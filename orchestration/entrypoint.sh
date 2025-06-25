#!/bin/bash
# orchestration/entrypoint.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Airflow entrypoint script...${NC}"

# Function to wait for database
wait_for_db() {
    echo -e "${YELLOW}Waiting for database to be ready...${NC}"
    
    while ! pg_isready -h postgres -p 5432 -U airflow; do
        echo "Database is not ready yet. Waiting..."
        sleep 2
    done
    
    echo -e "${GREEN}Database is ready!${NC}"
}

# Function to wait for Redis
wait_for_redis() {
    echo -e "${YELLOW}Waiting for Redis to be ready...${NC}"
    
    while ! redis-cli -h redis ping > /dev/null 2>&1; do
        echo "Redis is not ready yet. Waiting..."
        sleep 2
    done
    
    echo -e "${GREEN}Redis is ready!${NC}"
}

# Function to initialize Airflow database
init_airflow_db() {
    echo -e "${YELLOW}Initializing Airflow database...${NC}"
    
    # Force database initialization regardless of check result
    echo "Initializing Airflow database..."
    airflow db init
    echo -e "${GREEN}Airflow database initialized successfully${NC}"
}

# Function to create admin user
create_admin_user() {
    echo -e "${YELLOW}Creating Airflow admin user...${NC}"
    
    # Check if admin user already exists
    if airflow users list 2>/dev/null | grep -q "admin"; then
        echo -e "${GREEN}Admin user already exists${NC}"
    else
        echo "Creating admin user..."
        airflow users create \
            --username admin \
            --firstname Admin \
            --lastname User \
            --role Admin \
            --email admin@example.com \
            --password admin
        echo -e "${GREEN}Admin user created successfully${NC}"
        echo -e "${YELLOW}Login credentials: admin/admin${NC}"
    fi
}

# Function to create additional users
create_data_engineer_user() {
    echo -e "${YELLOW}Creating data engineer user...${NC}"
    
    if airflow users list | grep -q "data_engineer"; then
        echo -e "${GREEN}Data engineer user already exists${NC}"
    else
        echo "Creating data engineer user..."
        airflow users create \
            --username data_engineer \
            --firstname Data \
            --lastname Engineer \
            --role Admin \
            --email engineer@example.com \
            --password engineer123
        echo -e "${GREEN}Data engineer user created successfully${NC}"
    fi
}

# Function to install additional Python packages
install_additional_packages() {
    echo -e "${YELLOW}Installing additional Python packages...${NC}"
    
    # List of additional packages needed for the project
    PACKAGES=(
        "great-expectations==0.17.12"
        "s3fs==2023.6.0"
    )
    
    for package in "${PACKAGES[@]}"; do
        echo "Installing $package..."
        pip install --no-cache-dir "$package" || echo -e "${RED}Failed to install $package${NC}"
    done
    
    echo -e "${GREEN}Additional packages installation completed${NC}"
}

# Function to setup Spark configuration
setup_spark_config() {
    echo -e "${YELLOW}Setting up Spark configuration...${NC}"
    
    # Create Spark configuration directory
    mkdir -p /opt/airflow/spark-conf
    
    # Create spark-defaults.conf
    cat > /opt/airflow/spark-conf/spark-defaults.conf << EOF
# Spark Configuration for Data Engineering Platform
spark.master                     spark://spark-master:7077
spark.executor.memory            2g
spark.executor.cores             2
spark.executor.instances         2
spark.driver.memory              1g
spark.driver.cores               1

# Iceberg Configuration
spark.sql.extensions             org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog  org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type  hadoop
spark.sql.catalog.spark_catalog.warehouse  s3a://warehouse/
spark.sql.catalog.spark_catalog.s3.endpoint  http://minio:9000

# S3 Configuration for MinIO
spark.hadoop.fs.s3a.access.key        minio
spark.hadoop.fs.s3a.secret.key        minio123
spark.hadoop.fs.s3a.impl              org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.connection.ssl.enabled false

# Serialization
spark.serializer                 org.apache.spark.serializer.KryoSerializer

# Adaptive Query Execution
spark.sql.adaptive.enabled       true
spark.sql.adaptive.coalescePartitions.enabled  true

# Streaming Configuration
spark.sql.streaming.checkpointLocation  s3a://warehouse/checkpoints/
EOF

    echo -e "${GREEN}Spark configuration completed${NC}"
}

# Function to setup connections in Airflow
setup_airflow_connections() {
    echo -e "${YELLOW}Setting up Airflow connections...${NC}"
    
    # Spark connection - Fix host to use spark://spark-master:7077 instead of yarn
    airflow connections delete spark_default 2>/dev/null || echo "No existing spark_default connection to delete"
    
    airflow connections add 'spark_default' \
        --conn-type 'spark' \
        --conn-host 'spark://spark-master:7077' || echo "Spark connection creation failed"
    
    echo -e "${GREEN}Spark connection set up correctly${NC}"
    
    # MinIO S3 connection
    airflow connections add 'minio_s3' \
        --conn-type 'generic' \
        --conn-host 'http://minio:9000' \
        --conn-login 'minio' \
        --conn-password 'minio123' \
        --conn-extra '{"endpoint_url": "http://minio:9000", "aws_access_key_id": "minio", "aws_secret_access_key": "minio123"}' || echo "MinIO connection already exists"
    
    # Kafka connection
    airflow connections add 'kafka_default' \
        --conn-type 'generic' \
        --conn-host 'kafka:29092' \
        --conn-extra '{"bootstrap_servers": "kafka:29092"}' || echo "Kafka connection already exists"
    
    # PostgreSQL connection (for metadata)
    airflow connections add 'postgres_default' \
        --conn-type 'postgres' \
        --conn-host 'postgres' \
        --conn-login 'airflow' \
        --conn-password 'airflow' \
        --conn-schema 'airflow' \
        --conn-port 5432 || echo "PostgreSQL connection already exists"
    
    echo -e "${GREEN}Airflow connections setup completed${NC}"
}

# Function to create variables
setup_airflow_variables() {
    echo -e "${YELLOW}Setting up Airflow variables...${NC}"
    
    # Function to set variable if it doesn't exist
    set_variable_if_not_exists() {
        local var_name=$1
        local var_value=$2
        
        if ! airflow variables get "$var_name" &> /dev/null; then
            echo "Setting variable: $var_name"
            airflow variables set "$var_name" "$var_value"
            echo -e "${GREEN}Variable $var_name set successfully${NC}"
        else
            echo -e "${GREEN}Variable $var_name already exists${NC}"
        fi
    }
    
    # Set platform variables
    set_variable_if_not_exists "SPARK_MASTER_URL" "spark://spark-master:7077"
    set_variable_if_not_exists "DATA_LAKE_BUCKET" "warehouse"
    set_variable_if_not_exists "MINIO_ENDPOINT" "http://minio:9000"
    set_variable_if_not_exists "KAFKA_BOOTSTRAP_SERVERS" "kafka:29092"
    
    echo -e "${GREEN}Airflow variables setup completed${NC}"
}

# Function to check if DAGs folder exists
check_dags_folder() {
    echo -e "${YELLOW}Checking DAGs folder...${NC}"
    
    if [ ! -d "/opt/airflow/dags" ]; then
        echo -e "${RED}Warning: DAGs folder does not exist!${NC}"
        mkdir -p /opt/airflow/dags
        echo -e "${GREEN}Created DAGs folder${NC}"
    else
        echo -e "${GREEN}DAGs folder exists${NC}"
    fi
}

# Function to wait for Spark to be ready
wait_for_spark() {
    echo -e "${YELLOW}Waiting for Spark Master...${NC}"
    
    for i in {1..10}; do
        if curl -s "http://spark-master:8080" > /dev/null; then
            echo -e "${GREEN}Spark Master is ready!${NC}"
            return 0
        fi
        echo "Waiting for Spark Master... Attempt $i"
        sleep 5
    done
    
    echo -e "${YELLOW}Spark Master did not respond in time, but continuing...${NC}"
}

# Function to perform health checks
perform_health_checks() {
    echo -e "${YELLOW}Performing health checks...${NC}"
    
    # Check Airflow scheduler
    if pgrep -f "airflow scheduler" > /dev/null; then
        echo -e "${GREEN}✓ Airflow scheduler is running${NC}"
    else
        echo -e "${YELLOW}- Airflow scheduler not running yet${NC}"
    fi
    
    # Check Airflow webserver
    if pgrep -f "airflow webserver" > /dev/null; then
        echo -e "${GREEN}✓ Airflow webserver is running${NC}"
    else
        echo -e "${YELLOW}- Airflow webserver not running yet${NC}"
    fi
    
    # Check database connectivity
    if airflow db check > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Database connectivity OK${NC}"
    else
        echo -e "${RED}✗ Database connectivity issue${NC}"
    fi
}

# Function to unpause all DAGs
unpause_all_dags() {
    echo -e "${YELLOW}Unpausing all DAGs...${NC}"
    
    # Wait a bit for all DAGs to be recognized by Airflow
    sleep 10
    
    # Get all DAGs in a more robust way
    DAG_IDS=$(airflow dags list --output plain | awk '{print $1}')
    
    # Unpause each DAG
    for dag_id in $DAG_IDS; do
        if [[ ! -z "$dag_id" && "$dag_id" != "dag_id" ]]; then
            echo "Unpausing DAG: $dag_id"
            airflow dags unpause "$dag_id"
        fi
    done
    
    # Verify which DAGs were unpaused
    echo -e "${YELLOW}Verifying unpaused DAGs:${NC}"
    airflow dags list
    
    echo -e "${GREEN}All DAGs have been unpaused${NC}"
}

# Main execution sequence
main() {
    echo -e "${YELLOW}=== Airflow Initialization Started ===${NC}"
    
    # Wait for database to be ready
    wait_for_db
    
    # Wait for Redis to be ready
    wait_for_redis
    
    # Initialize Airflow database
    init_airflow_db
    
    # Create admin user
    create_admin_user
    
    # Create data engineer user
    create_data_engineer_user
    
    # Install additional packages if needed
    # install_additional_packages
    
    # Setup Spark configuration
    setup_spark_config
    
    # Setup Airflow connections
    setup_airflow_connections
    
    # Setup Airflow variables
    setup_airflow_variables
    
    # Check environment
    check_dags_folder
    
    # Wait for Spark (optional, for better integration)
    if [[ "$1" == "scheduler" ]]; then
        wait_for_spark
    fi
    
    # Unpause all DAGs
    unpause_all_dags
    
    echo -e "${GREEN}=== Airflow Initialization Completed ===${NC}"
    
    # Start the requested Airflow component
    case "$1" in
        webserver)
            echo -e "${GREEN}Starting Airflow Webserver...${NC}"
            exec airflow webserver
            ;;
        scheduler)
            echo -e "${GREEN}Starting Airflow Scheduler...${NC}"
            exec airflow scheduler
            ;;
        worker)
            echo -e "${GREEN}Starting Airflow Worker...${NC}"
            # Remove any previous worker PID file to avoid startup issues
            rm -f /opt/airflow/airflow-worker.pid 2>/dev/null
            # Create health file for container health check
            touch /opt/airflow/worker_healthy
            # Start worker with additional flags for better stability
            exec airflow celery worker --concurrency=2 --without-mingle --without-gossip
            ;;
        flower)
            echo -e "${GREEN}Starting Airflow Flower...${NC}"
            exec airflow celery flower
            ;;
        *)
            echo -e "${RED}Unknown command: $1${NC}"
            echo "Available commands: webserver, scheduler, worker, flower"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"