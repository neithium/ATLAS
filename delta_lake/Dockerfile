# ATLAS Delta Lake Module - Dockerfile
# Spark environment for Delta Lake deduplication

FROM apache/spark:3.5.0-python3

# Switch to root for installations
USER root

# Install Python dependencies with retry and timeout settings
RUN pip install  --timeout 120 --retries 5 \
    delta-spark==3.1.0 \
    pyarrow>=14.0.0

# Create app directory structure
RUN mkdir -p /app/data/raw /app/data/refined

# Set working directory
WORKDIR /app

# Copy application scripts
COPY generate_data.py /app/
COPY delta_merge_pipeline.py /app/

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=python3

# Default command - display help
CMD ["echo", "Use: docker-compose run spark python generate_data.py && docker-compose run spark python delta_merge_pipeline.py"]
