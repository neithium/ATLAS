FROM apache/spark:3.5.0-python3

USER root

WORKDIR /app

# Install Python dependencies
COPY jobs/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Fix Spark ivy cache permissions
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2

USER spark