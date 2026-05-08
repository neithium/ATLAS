FROM apache/spark:3.5.0-python3

USER root

WORKDIR /app

# Install Python dependencies
COPY jobs/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-install S3A Jars into Spark Jars directory for instant-start
RUN apt-get update && apt-get install -y curl && \
    curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Fix Spark ivy cache permissions
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2

USER spark