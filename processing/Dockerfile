# processing/Dockerfile
# Custom Spark image with Great Expectations support

FROM bitnami/spark:3.4.1

# Switch to root to install packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y curl && apt-get clean

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy Great Expectations configuration
COPY great_expectations/ /opt/processing/great_expectations/
COPY spark-apps/ /opt/processing/spark-apps/
COPY config/ /opt/processing/config/
COPY utils/ /opt/processing/utils/

# Create directory for additional JAR files and copy them
RUN mkdir -p /opt/spark/extra-jars

# Download additional JAR files for MinIO/S3 compatibility
RUN curl -o /opt/spark/extra-jars/minio-java-client-8.5.6.jar \
    https://repo1.maven.org/maven2/io/minio/minio/8.5.6/minio-8.5.6.jar

RUN curl -o /opt/spark/extra-jars/commons-io-2.11.0.jar \
    https://repo1.maven.org/maven2/commons-io/commons-io/2.11.0/commons-io-2.11.0.jar

# Make sure all JAR files are added to the classpath
ENV SPARK_CLASSPATH="/opt/spark/extra-jars/*:${SPARK_CLASSPATH}"

# Set proper permissions
RUN chown -R 1001:1001 /opt/processing /opt/spark/extra-jars

# Switch back to spark user
USER 1001