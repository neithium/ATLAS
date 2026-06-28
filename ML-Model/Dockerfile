FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python scripts
COPY . .

# Keep the container running for manual execution, 
# or you could set an entrypoint to run the scripts continuously.
CMD ["tail", "-f", "/dev/null"]
