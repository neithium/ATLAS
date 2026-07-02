FROM apache/spark:3.5.0-python3

USER root

WORKDIR /app

# -------------------------------------------------------
# Copy Project Files
# -------------------------------------------------------

COPY jobs/requirements.txt /tmp/requirements.txt

COPY jobs /app/jobs
COPY dlq /app/dlq
COPY start.sh /app/start.sh

RUN chmod +x /app/start.sh

# -------------------------------------------------------
# Install Python Dependencies
# -------------------------------------------------------

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# -------------------------------------------------------
# Install System Packages
# -------------------------------------------------------

RUN apt-get update && \
    apt-get install -y \
        curl \
        netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------
# Pre-install Spark JARs
# -------------------------------------------------------

RUN curl -L \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# -------------------------------------------------------
# Create Runtime Directories
# -------------------------------------------------------

RUN mkdir -p \
    /app/data/raw \
    /app/data/archive \
    /app/data/processed \
    /app/checkpoints \
    /app/logs \
    /home/spark/.ivy2/cache && \
    chown -R spark:spark /app /home/spark/.ivy2

# -------------------------------------------------------
# Switch to Spark User
# -------------------------------------------------------

USER spark

# -------------------------------------------------------
# Start Processing Engine
# -------------------------------------------------------

CMD ["/app/start.sh"]