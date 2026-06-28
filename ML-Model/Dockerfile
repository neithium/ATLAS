FROM python:3.11-slim

WORKDIR /app

# Ensure Python output is instantly flushed to the Docker logs
ENV PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python scripts
COPY . .

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Start the entrypoint which prints the banner and tails the central log file
CMD ["./entrypoint.sh"]
