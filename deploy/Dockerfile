# Stage 1: Build the C++ broker
FROM ubuntu:22.04 AS builder

# Install dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy source code
COPY broker/ ./broker/
COPY common/ ./common/

# Build broker
WORKDIR /build/broker
RUN mkdir -p build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    cmake --build . && \
    strip tvmjns-broker

# Stage 2: Runtime image
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 tvmjns

# Copy binary from builder
COPY --from=builder /build/broker/build/tvmjns-broker /usr/local/bin/

# Set permissions
RUN chmod +x /usr/local/bin/tvmjns-broker

# Switch to non-root user
USER tvmjns

# Expose port
EXPOSE 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD timeout 2 bash -c 'echo > /dev/tcp/localhost/9090' || exit 1

# Run broker
ENTRYPOINT ["/usr/local/bin/tvmjns-broker"]
CMD ["9090", "4"]
