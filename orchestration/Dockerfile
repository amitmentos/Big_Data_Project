# orchestration/Dockerfile

FROM apache/airflow:2.7.1-python3.10

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    openjdk-11-jdk \
    curl \
    redis-tools \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME dynamically based on architecture
RUN if [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then \
    export JAVA_HOME_DIR=/usr/lib/jvm/java-11-openjdk-arm64; \
    elif [ -d "/usr/lib/jvm/java-11-openjdk-amd64" ]; then \
    export JAVA_HOME_DIR=/usr/lib/jvm/java-11-openjdk-amd64; \
    else \
    export JAVA_HOME_DIR=$(find /usr/lib/jvm/ -name "java-11-openjdk*" -type d | head -n 1); \
    fi && \
    echo "export JAVA_HOME=$JAVA_HOME_DIR" >> /etc/profile.d/java_home.sh

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --user -r /requirements.txt

# Download Spark and Iceberg JARs
RUN mkdir -p /home/airflow/.local/lib/python3.10/site-packages/pyspark/jars

# Download required JARs with updated versions
RUN curl -o /home/airflow/.local/lib/python3.10/site-packages/pyspark/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.3.1/iceberg-spark-runtime-3.4_2.12-1.3.1.jar

RUN curl -o /home/airflow/.local/lib/python3.10/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -o /home/airflow/.local/lib/python3.10/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.470.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.470/aws-java-sdk-bundle-1.12.470.jar

# Add additional required jars for MinIO compatibility
RUN curl -o /home/airflow/.local/lib/python3.10/site-packages/pyspark/jars/minio-java-client-8.5.6.jar \
    https://repo1.maven.org/maven2/io/minio/minio/8.5.6/minio-8.5.6.jar

RUN curl -o /home/airflow/.local/lib/python3.10/site-packages/pyspark/jars/commons-io-2.11.0.jar \
    https://repo1.maven.org/maven2/commons-io/commons-io/2.11.0/commons-io-2.11.0.jar

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
ENV AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=UTC
ENV PYTHONPATH="/home/airflow/.local/lib/python3.10/site-packages:${PYTHONPATH}"

# Initialize Airflow database on first run
COPY entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
USER airflow

ENTRYPOINT ["/entrypoint.sh"]