FROM python:3.12-slim as base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

COPY pyproject.toml ./
COPY src/ ./src/
COPY alembic/ ./alembic/
COPY dagster.yaml ./

RUN pip install --upgrade pip && \
    pip install -e . && \
    pip install dagster-webserver==1.11.10 dagster-dg-cli pytest>=7.0.0 black>=23.0.0

RUN mkdir -p /opt/dagster/home/logs/compute && \
    mkdir -p /opt/dagster/home/storage

ENV DAGSTER_HOME=/opt/dagster/home
ENV PYTHONPATH=/opt/dagster/app/src:$PYTHONPATH

EXPOSE 3001

CMD ["python", "-m", "dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3001"]
