# Multi-stage Dockerfile for Zeal Network Data Pipeline
# Supports both producer and consumer components

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -m -u 1000 appuser

# Copy dependency files first (for better caching)
COPY pyproject.toml ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir kafka-python psycopg2-binary python-dotenv pyyaml pydantic prometheus-client python-dateutil

# Copy application code
COPY --chown=appuser:appuser . .

# Copy and set up entrypoint script (set permissions before switching user)
COPY --chown=appuser:appuser docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Switch to non-root user
USER appuser

# Expose metrics ports
EXPOSE 8001 8002

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command (can be overridden)
# Use: docker run <image> producer  or  docker run <image> consumer
ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["producer"]

