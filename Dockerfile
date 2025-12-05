# Multi-stage Dockerfile for EigenWatch Pipeline
FROM python:3.12-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/dagster/app

# Copy dependency files and source code
COPY pyproject.toml ./
COPY src/ ./src/
COPY alembic/ ./alembic/
COPY dagster.yaml ./

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -e . && \
    pip install dagster-webserver==1.11.10 dagster-dg-cli pytest>=7.0.0 black>=23.0.0

# Create directories for logs and storage
RUN mkdir -p /opt/dagster/home/logs/compute && \
    mkdir -p /opt/dagster/home/storage

# Set Dagster home and Python path
ENV DAGSTER_HOME=/opt/dagster/home
ENV PYTHONPATH=/opt/dagster/app/src:$PYTHONPATH

# Expose Dagster webserver port
EXPOSE 3001

# Default command (can be overridden in docker-compose)
CMD ["python", "-m", "dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3001"]
