#!/bin/bash
# setup.sh - Complete setup script for E-commerce Data Platform

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="ecommerce-data-platform"
COMPOSE_PROJECT_NAME="ecommerce"

echo -e "${BLUE}=== E-commerce Data Platform Setup ===${NC}"
echo -e "${BLUE}This script will set up the complete data platform locally${NC}"
echo

# Function to print status messages
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose (modern version)
    if ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not available. Please install Docker Desktop or Docker Compose plugin."
        exit 1
    fi
    
    # Check available memory (Mac specific)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        available_memory_mb=$(( $(sysctl -n hw.memsize) / 1024 / 1024 ))
        available_memory_gb=$(( available_memory_mb / 1024 ))
        if [ "$available_memory_gb" -lt 8 ]; then
            print_warning "Less than 8GB of total memory detected. The platform may run slowly."
        fi
    fi
    
    # Check disk space
    available_space=$(df -h . | awk 'NR==2{print $4}' | sed 's/G.*//')
    if [ "$available_space" -lt 10 ]; then
        print_warning "Less than 10GB of available disk space. You may need more space for data."
    fi
    
    print_success "Prerequisites check completed"
}

# Function to create directory structure
create_directories() {
    print_status "Creating directory structure..."
    
    # Create data directories
    mkdir -p storage/data/{bronze,silver,gold}
    mkdir -p logs/{airflow,spark,kafka}
    mkdir -p orchestration/logs
    mkdir -p processing/jars
    
    # Set permissions
    chmod -R 755 storage/data
    chmod -R 755 logs
    
    print_success "Directory structure created"
}

# Function to download required JARs
download_jars() {
    print_status "Downloading required JAR files..."
    
    JAR_DIR="processing/jars"
    
    # Iceberg Spark Runtime
    if [ ! -f "$JAR_DIR/iceberg-spark-runtime-3.4_2.12-1.3.1.jar" ]; then
        print_status "Downloading Iceberg Spark Runtime..."
        curl -L -o "$JAR_DIR/iceberg-spark-runtime-3.4_2.12-1.3.1.jar" \
            "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.3.1/iceberg-spark-runtime-3.4_2.12-1.3.1.jar" || {
            print_warning "Failed to download Iceberg JAR. You may need to download it manually."
        }
    fi
    
    # Hadoop AWS
    if [ ! -f "$JAR_DIR/hadoop-aws-3.3.4.jar" ]; then
        print_status "Downloading Hadoop AWS..."
        curl -L -o "$JAR_DIR/hadoop-aws-3.3.4.jar" \
            "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" || {
            print_warning "Failed to download Hadoop AWS JAR. You may need to download it manually."
        }
    fi
    
    # AWS SDK
    if [ ! -f "$JAR_DIR/aws-java-sdk-bundle-1.12.470.jar" ]; then
        print_status "Downloading AWS SDK..."
        curl -L -o "$JAR_DIR/aws-java-sdk-bundle-1.12.470.jar" \
            "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.470/aws-java-sdk-bundle-1.12.470.jar" || {
            print_warning "Failed to download AWS SDK JAR. You may need to download it manually."
        }
    fi
    
    print_success "JAR files download completed (check warnings above)"
}

# Function to create environment file if it doesn't exist
create_env_file() {
    if [ ! -f ".env" ]; then
        print_status "Creating .env file..."
        cat > .env << 'EOF'
# MinIO Configuration
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
MINIO_ENDPOINT=http://minio:9000

# PostgreSQL Configuration (for Airflow)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
AIRFLOW__CORE__FERNET_KEY=b3BlbnNzY2gga2V5IGZvciBhaXJmbG93
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_CORES=2

# Data Processing Configuration
EVENTS_PER_MINUTE=1000
DURATION_MINUTES=0
PROCESS_DATE=$(date +%Y-%m-%d)

# S3 Configuration for Iceberg
AWS_ACCESS_KEY_ID=minio
AWS_SECRET_ACCESS_KEY=minio123
AWS_ENDPOINT_URL=http://minio:9000
AWS_REGION=us-east-1

# Logging Configuration
LOG_LEVEL=INFO

# Timezone
TZ=UTC
EOF
        print_success "Environment file created"
    else
        print_status "Environment file already exists"
    fi
}

# Function to clean up existing containers
cleanup_existing() {
    print_status "Cleaning up existing containers..."
    
    # Stop and remove containers
    docker compose -p $COMPOSE_PROJECT_NAME down -v 2>/dev/null || true
    
    # Remove orphaned containers
    docker container prune -f
    
    # Remove unused networks
    docker network prune -f
    
    print_success "Cleanup completed"
}

# Function to start core infrastructure
start_infrastructure() {
    print_status "Starting core infrastructure..."
    
    # Start core services first
    docker compose up -d minio zookeeper kafka postgres redis
    
    # Wait for services to be ready
    print_status "Waiting for core services to be ready..."
    sleep 30
    
    # Check service health
    check_service_health "minio" "http://localhost:9000/minio/health/live"
    check_service_health "kafka" "localhost:9092"
    check_service_health "postgres" "localhost:5432"
    
    print_success "Core infrastructure started"
}

# Function to check service health
check_service_health() {
    local service_name=$1
    local health_url=$2
    local max_attempts=30
    local attempt=1
    
    print_status "Checking $service_name health..."
    
    while [ $attempt -le $max_attempts ]; do
        if [ "$service_name" = "minio" ]; then
            if curl -f "$health_url" >/dev/null 2>&1; then
                print_success "$service_name is healthy"
                return 0
            fi
        elif [ "$service_name" = "kafka" ]; then
            if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
                print_success "$service_name is healthy"
                return 0
            fi
        elif [ "$service_name" = "postgres" ]; then
            if docker compose exec -T postgres pg_isready -U airflow >/dev/null 2>&1; then
                print_success "$service_name is healthy"
                return 0
            fi
        fi
        
        print_status "Attempt $attempt/$max_attempts - $service_name not ready yet..."
        sleep 10
        ((attempt++))
    done
    
    print_error "$service_name failed to become healthy"
    return 1
}

install_python_dependencies() {
    print_status "Installing required Python dependencies..."
    
    # Install core dependencies needed for the platform
    pip install -r requirements.txt || {
        print_warning "Failed to install dependencies from requirements.txt. Trying individual packages..."
    }
    
    # Install specific versions needed for MinIO and S3 compatibility
    pip install boto3==1.24.59 botocore==1.27.59 s3fs==2022.11.0 fsspec==2022.11.0 \
        aiobotocore==2.4.2 minio==7.1.15 'urllib3<2.0.0' pyiceberg==0.4.0 || {
        print_warning "Failed to install some Python dependencies. Platform may not function correctly."
    }
    
    # Verify installed versions
    print_status "Installed Python package versions:"
    pip list | grep -E 'boto3|botocore|s3fs|fsspec|aiobotocore|minio|urllib3|pyiceberg'
    
    print_success "Python dependencies installed"
}


# Function to initialize Airflow
initialize_airflow() {
    print_status "Initializing Airflow..."
    
    # Initialize Airflow database
    docker compose exec -T airflow-webserver airflow db init
    
    # Create admin user
    docker compose exec -T airflow-webserver airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    
    print_success "Airflow initialized"
}

# Function to start processing services
start_processing_services() {
    print_status "Starting processing services..."
    
    # Start Spark services
    docker compose up -d spark-master spark-worker
    
    # Start Airflow services
    docker compose up -d airflow-webserver airflow-scheduler airflow-worker
    
    # Wait for services
    sleep 30
    
    print_success "Processing services started"
}

# Function to start streaming services
start_streaming_services() {
    print_status "Starting streaming services..."
    
    # Start Kafka producers
    docker compose up -d kafka-producers
    
    print_success "Streaming services started"
}

# Function to verify setup
verify_setup() {
    print_status "Verifying setup..."
    
    # Check all services are running
    local services_status
    services_status=$(docker compose ps --format "table {{.Service}}\t{{.Status}}")
    
    echo "$services_status"
    
    # Check access points
    print_status "Checking service access points..."
    
    # MinIO
    if curl -f http://localhost:9000/minio/health/live >/dev/null 2>&1; then
        print_success "MinIO accessible at http://localhost:9000"
    else
        print_warning "MinIO not accessible"
    fi
    
    # Spark Master
    if curl -f http://localhost:8080 >/dev/null 2>&1; then
        print_success "Spark Master UI accessible at http://localhost:8080"
    else
        print_warning "Spark Master UI not accessible"
    fi
    
    # Airflow
    if curl -f http://localhost:8081/health >/dev/null 2>&1; then
        print_success "Airflow UI accessible at http://localhost:8081"
    else
        print_warning "Airflow UI not accessible"
    fi
    
    print_success "Setup verification completed"
}

# Function to display summary
display_summary() {
    echo
    echo -e "${GREEN}=== Setup Complete ===${NC}"
    echo -e "${BLUE}The E-commerce Data Platform is now running!${NC}"
    echo
    echo -e "${YELLOW}Service Access Points:${NC}"
    echo -e "• MinIO Console: ${BLUE}http://localhost:9001${NC} (minio/minio123)"
    echo -e "• Spark Master UI: ${BLUE}http://localhost:8080${NC}"
    echo -e "• Airflow UI: ${BLUE}http://localhost:8081${NC} (admin/admin)"
    echo
    echo -e "${YELLOW}Next Steps:${NC}"
    echo -e "1. Access Airflow UI and enable the DAGs"
    echo -e "2. Monitor data ingestion in MinIO buckets"
    echo -e "3. Run tests with: ${BLUE}./test_platform.sh${NC}"
    echo
    echo -e "${YELLOW}To stop the platform:${NC}"
    echo -e "• ${BLUE}docker compose down${NC}"
    echo
    echo -e "${YELLOW}To stop and remove all data:${NC}"
    echo -e "• ${BLUE}docker compose down -v${NC}"
}

# Main execution
main() {
    print_status "Starting setup process..."
    
    check_prerequisites
    create_directories
    download_jars
    create_env_file
    cleanup_existing
    start_infrastructure
    initialize_airflow
    start_processing_services
    start_streaming_services
    verify_setup
    display_summary
    
    print_success "Setup completed successfully!"
}

# Execute main function
main "$@"