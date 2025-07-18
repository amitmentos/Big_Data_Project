#!/bin/bash

# E-commerce Data Platform - Complete Demo Startup Script with Great Expectations
# This script starts all services, runs the complete platform demo, then validates with Great Expectations

set -e

echo "E-COMMERCE BIG DATA PLATFORM - COMPLETE DEMO STARTUP"
echo "=========================================================="
echo "Running complete platform flow + Great Expectations validation"

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "Docker is not running. Please start Docker first."
        exit 1
    fi
    echo "Docker is running"
}

# Function to check if docker-compose is available
check_docker_compose() {
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        echo "Docker Compose is not available. Please install Docker Compose."
        exit 1
    fi
    echo "Docker Compose is available"
}

# Function to start services
start_services() {
    echo ""
    echo "Starting all platform services..."
    echo "This may take a few minutes for first-time setup..."
    
    # Use docker compose (newer syntax) or docker-compose (older syntax)
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
    
    # Build and start all services
    $DOCKER_COMPOSE up -d --build
    
    echo "All services starting in background..."
}

# Function to show service status
show_services() {
    echo ""
    echo "Service Status:"
    if docker compose version &> /dev/null; then
        docker compose ps
    else
        docker-compose ps
    fi
}

# Function to ensure Kafka is properly started
ensure_kafka_running() {
    echo ""
    echo "Ensuring Kafka is properly started..."
    
    # Determine docker compose command
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
    
    # Check if Kafka container is running
    if ! docker ps | grep -q "ecommerce-kafka"; then
        echo "Kafka container not running properly. Attempting to fix..."
        
        # Make sure Zookeeper is running
        if ! docker ps | grep -q "ecommerce-zookeeper"; then
            echo "   Restarting Zookeeper first..."
            $DOCKER_COMPOSE up -d zookeeper
            echo "   Waiting for Zookeeper to initialize..."
            sleep 15
        fi
        
        # Start Kafka container
        echo "   Starting Kafka container..."
        $DOCKER_COMPOSE up -d kafka
        
        # Wait for Kafka to be ready
        echo "   Waiting for Kafka to initialize (30 seconds)..."
        sleep 30
        
        # Verify Kafka is running
        if docker ps | grep -q "ecommerce-kafka"; then
            echo "Kafka container started successfully!"
        else
            echo "Failed to start Kafka container."
            echo "   Attempting one more restart with increased timeout..."
            $DOCKER_COMPOSE up -d kafka
            sleep 45
            
            if docker ps | grep -q "ecommerce-kafka"; then
                echo "Kafka container started successfully on second attempt!"
            else
                echo "Failed to start Kafka container after multiple attempts."
                echo "   You may need to manually troubleshoot Kafka startup issues."
                echo "   Continuing with the demo, but streaming features may not work properly."
            fi
        fi
        
        # Restart the Kafka init container
        echo "   Restarting Kafka initialization..."
        $DOCKER_COMPOSE up -d kafka-init
        sleep 10
    else
        echo "Kafka container is already running"
    fi
}

# Function to generate fresh sample data with current timestamps
generate_fresh_data() {
    echo ""
    echo "Generating Fresh Sample Data with Current Timestamps..."
    echo "Creating up-to-date data for all bronze layer tables..."
    
    # Create a fresh data generation script that uses current timestamps
    cat > generate_current_data.py << 'EOF'
#!/usr/bin/env python3
"""
Generate fresh sample data with current timestamps for bronze layer tables
"""
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker()

def generate_current_timestamp_data():
    """Generate data with current timestamps"""
    now = datetime.now()
    current_date = now.strftime('%Y-%m-%d')
    
    print(f"Generating data for date: {current_date}")
    
    # Generate fresh customers with current timestamp
    customers = []
    for i in range(100):
        customer = {
            'customer_id': f"CUST-{i:06d}",
            'customer_name': fake.name(),
            'email': fake.email(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'country': fake.country(),
            'membership_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'created_at': (now - timedelta(days=random.randint(30, 365))).isoformat(),
            'last_updated': now.isoformat(),
            'ingestion_time': now.isoformat()
        }
        customers.append(customer)
    
    # Generate fresh products with current timestamp
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
    brands = ['TechCorp', 'StyleMax', 'HomeComfort', 'SportsPro', 'BookWorld']
    products = []
    for i in range(50):
        category = random.choice(categories)
        product = {
            'product_id': f"PROD-{i:06d}",
            'product_name': f"{fake.catch_phrase()} {category}",
            'category': category,
            'subcategory': f"{category} - {fake.word().title()}",
            'brand': random.choice(brands),
            'base_price': round(random.uniform(10.0, 1000.0), 2),
            'description': fake.text(max_nb_chars=200),
            'is_active': random.choice([True, True, True, False]),
            'last_updated': now.isoformat(),
            'ingestion_time': now.isoformat()
        }
        products.append(product)
    
    # Generate fresh user events with current and recent timestamps
    user_events = []
    for i in range(1000):
        # Mix of events from last 24 hours for realistic stream
        event_time = now - timedelta(hours=random.randint(0, 24))
        event = {
            'event_id': str(uuid.uuid4()),
            'session_id': str(uuid.uuid4()),
            'customer_id': random.choice(customers)['customer_id'],
            'event_type': random.choice(['page_view', 'product_view', 'add_to_cart', 'purchase', 'search']),
            'event_time': event_time.isoformat(),
            'page_url': f"https://ecommerce.com/{fake.uri_path()}",
            'device_type': random.choice(['desktop', 'mobile', 'tablet']),
            'metadata': {
                'product_id': random.choice(products)['product_id'] if random.random() > 0.3 else None,
                'user_agent': fake.user_agent(),
                'ip_address': fake.ipv4()
            },
            'ingestion_time': now.isoformat()
        }
        user_events.append(event)
    
    # Generate fresh marketplace sales with CURRENT DATE and varied delays for demo
    marketplaces = ['Amazon', 'eBay', 'Etsy', 'Walmart', 'Target']
    marketplace_sales = []
    for i in range(500):
        # IMPORTANT: Use current date (June 17, 2025) with varied delays
        # 60% recent (last 3 days), 30% medium delay (3-7 days), 10% old delay (7-14 days)
        delay_choice = random.random()
        if delay_choice < 0.6:  # Recent transactions
            transaction_time = now - timedelta(days=random.uniform(0, 3))
        elif delay_choice < 0.9:  # Medium delay
            transaction_time = now - timedelta(days=random.uniform(3, 7))
        else:  # Older transactions for late arrival demo
            transaction_time = now - timedelta(days=random.uniform(7, 14))
        
        product = random.choice(products)
        sale = {
            'transaction_id': str(uuid.uuid4()),
            'marketplace_name': random.choice(marketplaces),
            'seller_id': str(uuid.uuid4()),
            'product_id': product['product_id'],
            'amount': round(product['base_price'] * random.uniform(0.8, 1.2), 2),
            'currency': 'USD',
            'quantity': random.randint(1, 5),
            'transaction_time': transaction_time.isoformat(),
            'settlement_time': (transaction_time + timedelta(days=random.randint(1, 7))).isoformat(),
            'payment_method': random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Digital Wallet']),
            'status': random.choice(['completed', 'completed', 'completed', 'pending', 'cancelled']),
            'marketplace_metadata': {
                'commission_rate': round(random.uniform(0.05, 0.15), 3),
                'shipping_cost': round(random.uniform(5.0, 25.0), 2),
                'tax_amount': round(random.uniform(0.0, 50.0), 2)
            },
            'ingestion_time': now.isoformat(),
            'data_freshness': 'current_date_' + current_date,
            'demo_timestamp': now.isoformat()
        }
        marketplace_sales.append(sale)
    
    # Generate fresh marketing campaigns
    campaigns = []
    for i in range(10):
        start_date = fake.date_between(start_date='-1m', end_date='+1m')
        campaign = {
            'campaign_id': f'CAMP_{i:03d}',
            'campaign_name': f"{fake.catch_phrase()} Campaign",
            'campaign_type': random.choice(['Email', 'Social Media', 'Display', 'Search', 'Influencer']),
            'start_date': start_date.isoformat(),
            'end_date': (start_date + timedelta(days=random.randint(7, 90))).isoformat(),
            'budget': round(random.uniform(1000.0, 100000.0), 2),
            'target_audience': random.choice(['All', 'New Customers', 'VIP', 'Category Specific']),
            'ingestion_time': now.isoformat()
        }
        campaigns.append(campaign)
    
    # Save to files
    os.makedirs('sample_data', exist_ok=True)
    
    data_files = {
        'customers': customers,
        'products': products,
        'user_events': user_events,
        'marketplace_sales': marketplace_sales,
        'campaigns': campaigns
    }
    
    for name, data in data_files.items():
        filepath = f"sample_data/{name}.json"
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"Generated {len(data)} current {name} records -> {filepath}")
    
    print(f"Fresh data generation completed with timestamp: {now.isoformat()}")
    return current_date

if __name__ == "__main__":
    generate_current_timestamp_data()
EOF
    
    # Make the script executable and run it
    chmod +x generate_current_data.py
    CURRENT_DATE=$(python3 generate_current_data.py | grep "Generating data for date:" | awk '{print $NF}')
    
    echo "Current date for data generation: $CURRENT_DATE"
    return 0
}

# Function to start Kafka delay data flow demonstration
start_kafka_delay_demo() {
    echo ""
    echo "Starting Kafka Delay Data Flow Demonstration..."
    echo "This will showcase real-time streaming with delayed data arrivals..."
    
    # Determine docker compose command
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
    
    echo "Step 1: Starting immediate data producers..."
    
    # Start user activity producer with high rate for immediate demo
    echo "   Starting user activity producer (immediate events)..."
    $DOCKER_COMPOSE exec -d kafka-producers python user_activity_producer.py &
    ACTIVITY_PID=$!
    
    # Start marketplace sales producer with mixed delay patterns
    echo "   Starting marketplace sales producer (with delay patterns)..."
    $DOCKER_COMPOSE exec -d kafka-producers bash -c "
        export SALES_PER_HOUR=200
        export DELAY_DEMO_MODE=true
        python marketplace_sales_producer.py
    " &
    SALES_PID=$!
    
    echo "   Immediate data producers started"
    
    echo "Step 2: Starting streaming consumers and monitoring..."
    
    # Start streaming consumer to process data
    echo "   Starting streaming data consumer..."
    $DOCKER_COMPOSE exec -d spark-master python /opt/streaming/consumers/streaming_consumer.py &
    CONSUMER_PID=$!
    
    # Start Kafka delay monitor
    echo "   Starting Kafka delay monitor..."
    $DOCKER_COMPOSE exec -d spark-master python /opt/streaming/kafka_delay_monitor.py &
    MONITOR_PID=$!
    
    echo "   Streaming consumer and delay monitor started"
    
    echo "Step 3: Demonstrating delay data flow patterns..."
    
    # Show real-time Kafka activity
    echo "   Monitoring Kafka topics for 60 seconds..."
    for i in {1..12}; do
        echo "   Monitoring cycle $i/12 - Checking topics..."
        
        # Check user activity events
        USER_COUNT=$($DOCKER_COMPOSE exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 --topic user_activity_events --time -1 2>/dev/null | \
            awk -F: '{sum+=$3} END {print sum}' || echo "0")
        
        # Check marketplace sales
        SALES_COUNT=$($DOCKER_COMPOSE exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 --topic marketplace_sales --time -1 2>/dev/null | \
            awk -F: '{sum+=$3} END {print sum}' || echo "0")
        
        echo "      User Activity Events: $USER_COUNT total messages"
        echo "      Marketplace Sales: $SALES_COUNT total messages"
        
        if [ $i -eq 6 ]; then
            echo ""
            echo "   Triggering LATE ARRIVAL BATCH at midpoint..."
            $DOCKER_COMPOSE exec -d kafka-producers bash -c "
                export LATE_ARRIVAL_BATCH=true
                export BATCH_SIZE=50
                python marketplace_sales_producer.py --late-batch
            " &
            echo "   Late arrival batch (50 delayed records) triggered!"
            echo ""
        fi
        
        sleep 5
    done
    
    echo "Step 4: Demonstrating late arrival handling..."
    
    # Run late arrival handler
    echo "   Processing late arrivals with 48-hour lookback..."
    $DOCKER_COMPOSE exec spark-master python /opt/streaming/consumers/late_arrival_handler.py \
        --lookback-hours 48 --process-date $(date +%Y-%m-%d) &
    LATE_HANDLER_PID=$!
    
    echo "   Late arrival handler started"
    
    echo ""
    echo "Kafka Delay Data Flow Demonstration Summary:"
    echo "   Real-time user activity streaming active"
    echo "   Marketplace sales with delay patterns active" 
    echo "   Late arrival batch processing demonstrated"
    echo "   Late arrival handler processing enabled"
    echo ""
    echo "The system is now demonstrating:"
    echo "   • Immediate data ingestion (user activities)"
    echo "   • Delayed data arrival patterns (marketplace sales)"
    echo "   • Late arrival detection and reprocessing"
    echo "   • Real-time stream processing with watermarking"
    echo ""
    
    return 0
}

# Function to run the complete demo with current date
run_complete_demo() {
    echo ""
    echo "Starting Complete Platform Flow Demonstration..."
    echo "Please wait while services initialize..."
    
    # Generate fresh data first
    generate_fresh_data
    
    # Get current date for processing
    CURRENT_DATE=$(date +%Y-%m-%d)
    echo "Processing date: $CURRENT_DATE"
    
    # Start Kafka delay data flow demonstration
    start_kafka_delay_demo
    
    echo ""
    echo "Allowing streaming data to accumulate (60 seconds)..."
    sleep 60
    
    # Make sure the Python script is executable
    chmod +x run_complete_demo.py
    
    # Run the complete demo with current date and no-continuous flag
    PROCESSING_DATE=$CURRENT_DATE python3 run_complete_demo.py --no-continuous
}

# Function to run Great Expectations validation after complete demo
run_great_expectations_after_demo() {
    echo ""
    echo "RUNNING GREAT EXPECTATIONS DATA QUALITY VALIDATION"
    echo "====================================================="
    echo "Validating all processed data with Great Expectations..."
    
    # Make sure the Python script is executable
    chmod +x run_great_expectations_demo.py
    
    # Run the Great Expectations validation
    python3 run_great_expectations_demo.py
    
    echo ""
    echo "Great Expectations validation completed!"
}

# Main execution flow
main() {
    echo "Pre-flight checks..."
    check_docker
    check_docker_compose
    
    echo ""
    echo "Starting platform services..."
    start_services
    
    echo ""
    echo "Giving services time to initialize (30 seconds)..."
    echo "    (Initial time for services to start...)"
    sleep 30
    
    # Ensure Kafka is properly started
    ensure_kafka_running
    
    # Show all services status
    show_services
    
    echo ""
    echo "Platform ready! Running complete demo flow..."
    
    # Run complete platform demo first
    run_complete_demo
    
    # Then run Great Expectations validation
    run_great_expectations_after_demo
    
    echo ""
    echo "COMPLETE DEMO FLOW FINISHED!"
    echo "================================"
    echo "Platform demo completed successfully"
    echo "Great Expectations validation completed"
    echo ""
    echo "Access points:"
    echo "   • Airflow UI: http://localhost:8081 (admin/admin)"
    echo "   • Spark UI: http://localhost:8080"
    echo "   • MinIO Console: http://localhost:9001 (minio/minio123)"
    echo ""
    echo "To run demos again manually:"
    echo "   • Complete Demo: python3 run_complete_demo.py"
    echo "   • Great Expectations: python3 run_great_expectations_demo.py"
    echo ""
    echo "Platform is now running continuously..."
    echo "   Press Ctrl+C to stop all services"
    
    # Keep the platform running
    while true; do
        sleep 30
        echo "$(date '+%H:%M:%S') - Platform operational, all services running..."
    done
}

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down services..."
    if docker compose version &> /dev/null; then
        docker compose down
    else
        docker-compose down
    fi
    echo "Services stopped"
}

# Set up signal handlers
trap cleanup EXIT

# Run main function
main

echo ""
echo "Demo completed! Services are still running."
echo "Access points:"
echo "   • Airflow UI: http://localhost:8081 (admin/admin)"
echo "   • Spark UI: http://localhost:8080"
echo "   • MinIO Console: http://localhost:9001 (minio/minio123)"
echo ""
echo "To stop all services, run: docker compose down"
echo "To run individual demos:"
echo "   • Complete Demo: python3 run_complete_demo.py"
echo "   • Great Expectations: python3 run_great_expectations_demo.py"