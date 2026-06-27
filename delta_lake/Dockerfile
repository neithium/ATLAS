# ATLAS Delta Lake Module - Dockerfile
# Spark environment for Delta Lake batch deduplication

FROM apache/spark:3.5.0-python3

# Switch to root for installations
USER root

# Install Python dependencies with retry and timeout settings
# - delta-spark: Delta Lake support
# - pyarrow: Columnar data processing
RUN pip install --timeout 120 --retries 5 \
    delta-spark==3.1.0 \
    pyarrow>=14.0.0 \
    pytz

# Create app directory structure
RUN mkdir -p /app/data/raw /app/data/refined

# Set working directory
WORKDIR /app

# Copy application scripts
COPY generate_data.py /app/
COPY delta_core.py /app/
COPY run_benchmark.py /app/
COPY run_livewire.py /app/

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=python3

# Environment variables for Spark configuration
ENV SPARK_DRIVER_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY=2g

# Default compression codec (zstd for better compression ratio)
ENV COMPRESSION_CODEC=zstd

# Default command - display help
CMD ["echo", "Use: docker-compose run spark to run batch deduplication pipeline"]
