# TVMJNS Deployment

This directory contains Docker configuration for deploying the TVMJNS broker.

## Quick Start

### Build and run with Docker Compose

```bash
cd deploy
docker-compose up --build
```

The broker will be available on `localhost:9090`.

### Build Docker image manually

```bash
docker build -f deploy/Dockerfile -t tvmjns-broker:latest .
```

### Run Docker container

```bash
docker run -p 9090:9090 tvmjns-broker:latest
```

## Configuration

### Environment Variables

- `LOG_LEVEL`: Logging level (default: INFO)

### Ports

- `9090`: Default broker port (TCP)

### Custom Port and Thread Count

```bash
docker run -p 8080:8080 tvmjns-broker:latest 8080 8
```

Arguments:
1. Port number (default: 9090)
2. Thread pool size (default: 4)

## Health Check

The container includes a health check that verifies the broker is accepting connections:

```bash
docker ps  # Check HEALTH status
```

## Production Deployment

For production deployment:

1. Use a reverse proxy (nginx, HAProxy) for load balancing
2. Configure persistent logging with volume mounts
3. Set appropriate resource limits
4. Enable monitoring and metrics
5. Use secrets management for sensitive configuration

## Scaling

To run multiple broker instances:

```bash
docker-compose up --scale broker=3
```

Note: Use a load balancer to distribute connections across instances.
