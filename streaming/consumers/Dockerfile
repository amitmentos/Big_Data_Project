# streaming/consumers/Dockerfile

FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN groupadd -r kafka && useradd -r -g kafka kafka
RUN chown -R kafka:kafka /app
USER kafka

# Set environment variables
ENV PYTHONPATH=/app
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import kafka; kafka.KafkaConsumer(bootstrap_servers='$KAFKA_BOOTSTRAP_SERVERS', api_version=(0, 10, 1))" || exit 1

# Default command
CMD ["python", "streaming_consumer.py"]